const cache = new Map<string, Promise<any>>();

export function fetchCached<T = any>(url: string): Promise<T> {
  if (!cache.has(url)) {
    cache.set(
      url,
      fetch(url).then((r) => {
        if (!r.ok) throw new Error(`Erro ao buscar ${url}: ${r.status}`);
        return r.json() as Promise<T>;
      })
    );
  }
  return cache.get(url) as Promise<T>;
}
