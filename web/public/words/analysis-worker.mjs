import {buildAnalysisView} from "./analysis-view.mjs";

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

  const {createWordsAnalysisEngine} = await import(engineUrl.href);
  return createWordsAnalysisEngine({
    datasetId: manifest.datasetId,
    databaseUrl,
    moduleUrl,
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
    if (document.schema !== "whitakers-words.browser-search" ||
        document.schemaVersion !== 3) {
      throw new Error("Contrato de análise morfológica incompatível");
    }
    self.postMessage({
      type: "analysis",
      requestId: message.requestId,
      term: message.term,
      document,
      view: buildAnalysisView(document),
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
