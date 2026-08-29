const ASSURANCE_PROGRESS_PATH = /^\/api\/v1\/assurance\/documents\/[^/]+\/progress$/;
const TRANSIENT_STATUS = new Set([408, 425, 429, 500, 502, 503, 504]);
const BASE_DELAY_MS = 750;
const MAX_DELAY_MS = 5000;

function sleep(milliseconds) {
  return new Promise((resolve) => {
    window.setTimeout(resolve, milliseconds);
  });
}

function requestMethod(input, init) {
  return String(
    init?.method
      || (input instanceof Request ? input.method : "GET"),
  ).toUpperCase();
}

function requestUrl(input) {
  const raw = input instanceof Request ? input.url : String(input);
  return new URL(raw, window.location.href);
}

function isAssuranceProgressRequest(input, init) {
  if (requestMethod(input, init) !== "GET") {
    return false;
  }

  const url = requestUrl(input);
  return (
    url.origin === window.location.origin
    && ASSURANCE_PROGRESS_PATH.test(url.pathname)
  );
}

function retryDelay(attempt) {
  if (attempt <= 1) {
    return BASE_DELAY_MS;
  }
  if (attempt === 2) {
    return 1500;
  }
  if (attempt === 3) {
    return 3000;
  }
  return MAX_DELAY_MS;
}

function requestSignal(input, init) {
  return init?.signal || (input instanceof Request ? input.signal : null);
}

function abortError() {
  if (typeof DOMException === "function") {
    return new DOMException("The operation was aborted.", "AbortError");
  }
  const error = new Error("The operation was aborted.");
  error.name = "AbortError";
  return error;
}

function installAssuranceProgressRetry() {
  if (
    typeof window.fetch !== "function"
    || window.fetch.__litoralTraceAssuranceRetry
  ) {
    return;
  }

  const priorFetch = window.fetch.bind(window);

  const resilientFetch = async (input, init = {}) => {
    if (!isAssuranceProgressRequest(input, init)) {
      return priorFetch(input, init);
    }

    const signal = requestSignal(input, init);
    let attempt = 1;

    while (true) {
      if (signal?.aborted) {
        throw abortError();
      }

      try {
        const response = await priorFetch(input, init);
        if (!TRANSIENT_STATUS.has(response.status)) {
          return response;
        }
      } catch (error) {
        if (signal?.aborted || error?.name === "AbortError") {
          throw error;
        }
      }

      await sleep(retryDelay(attempt));
      attempt += 1;
    }
  };

  Object.defineProperty(
    resilientFetch,
    "__litoralTraceAssuranceRetry",
    {
      value: true,
      enumerable: false,
    },
  );

  if (window.fetch.__litoralTraceCsrf) {
    Object.defineProperty(
      resilientFetch,
      "__litoralTraceCsrf",
      {
        value: true,
        enumerable: false,
      },
    );
  }

  window.fetch = resilientFetch;
}

if (document.readyState === "loading") {
  document.addEventListener(
    "DOMContentLoaded",
    installAssuranceProgressRetry,
    { once: true },
  );
} else {
  installAssuranceProgressRetry();
}

export {
  ASSURANCE_PROGRESS_PATH,
  TRANSIENT_STATUS,
  installAssuranceProgressRetry,
};
