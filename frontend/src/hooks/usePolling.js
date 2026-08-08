import { useEffect, useRef, useState } from "react";

/**
 * Polls `fetchFn` every `intervalMs` and returns the latest result.
 * Keeps the previous value on screen while a new request is in flight,
 * and silently keeps the last good value if a poll fails (network blip) -
 * a live feed shouldn't blank itself out over one dropped request.
 */
export function usePolling(fetchFn, intervalMs, deps = []) {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const savedFetch = useRef(fetchFn);
  savedFetch.current = fetchFn;

  useEffect(() => {
    let cancelled = false;

    async function tick() {
      try {
        const result = await savedFetch.current();
        if (!cancelled) {
          setData(result);
          setError(null);
        }
      } catch (err) {
        if (!cancelled) setError(err);
      }
    }

    tick();
    const id = setInterval(tick, intervalMs);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);

  return { data, error };
}
