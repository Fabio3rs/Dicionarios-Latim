let enginePromise;

function fetchChecked(url) {
  return fetch(url).then((response) => {
    if (!response.ok) {
      throw new Error(`Falha ao carregar ${url}: HTTP ${response.status}`);
    }
    return response;
  });
}

async function createEngine(assetBase) {
  const resolvedAssetBase = new URL(assetBase, self.location.origin);
  const manifestUrl = new URL("manifest.json", resolvedAssetBase);
  const manifest = await fetchChecked(manifestUrl).then((response) => response.json());
  const engineUrl = new URL(manifest.entrypoint, resolvedAssetBase);
  const moduleUrl = new URL("words_wasm.mjs", resolvedAssetBase);
  const databaseUrl = new URL(manifest.databases.search.file, resolvedAssetBase);

  const [{createWordsAnalysisEngine}, moduleExports, databaseResponse] = await Promise.all([
    import(engineUrl.href),
    import(moduleUrl.href),
    fetchChecked(databaseUrl),
  ]);

  const modulePromise = moduleExports.default({
    locateFile: (path) => new URL(path, moduleUrl).href,
  });
  const databasePromise = databaseResponse.arrayBuffer();
  const [module, databaseBytes] = await Promise.all([modulePromise, databasePromise]);

  return createWordsAnalysisEngine({
    datasetId: manifest.datasetId,
    databaseBytes,
    moduleUrl,
    moduleFactory: async () => module,
  });
}

function initialize(assetBase) {
  if (!enginePromise) {
    enginePromise = createEngine(assetBase)
      .then((engine) => {
        self.postMessage({
          type: "ready",
          databaseBytes: engine.databaseBytes,
          datasetId: engine.datasetId,
        });
        return engine;
      })
      .catch((error) => {
        self.postMessage({type: "unavailable", message: error?.message || String(error)});
        throw error;
      });
  }
  return enginePromise;
}

self.addEventListener("message", async (event) => {
  const message = event.data || {};
  if (message.type === "init") {
    initialize(message.assetBase).catch(() => {});
    return;
  }
  if (message.type !== "analyze") return;

  try {
    const engine = await initialize(message.assetBase);
    const document = engine.search(message.term, {twoWords: true});
    self.postMessage({
      type: "analysis",
      requestId: message.requestId,
      term: message.term,
      document,
    });
  } catch (error) {
    self.postMessage({
      type: "analysis-error",
      requestId: message.requestId,
      term: message.term,
      message: error?.message || String(error),
    });
  }
});
