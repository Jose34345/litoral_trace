const ASSURANCE_PROGRESS_PATH = /^\/api\/v1\/assurance\/documents\/[^/]+\/progress$/;
const TRANSIENT_STATUS = new Set([408, 425, 429, 500, 502, 503, 504]);
const MAX_ATTEMPTS = 20;
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
  return Math.min(
    MAX_DELAY_MS,
    BASE_DELAY_MS * (2 ** Math.max(0, attempt - 1)),
  );
}

function signalWasAborted(input, init) {
  return Boolean(
    init?.signal?.aborted
      || (input instanceof Request && input.signal?.aborted),
  );
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

    let lastResponse = null;
    let lastError = null;

    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt += 1) {
      if (signalWasAborted(input, init)) {
        if (lastError) {
          throw lastError;
        }
        return priorFetch(input, init);
      }

      try {
        const response = await priorFetch(input, init);
        lastResponse = response;
        lastError = null;

        if (!TRANSIENT_STATUS.has(response.status)) {
          return response;
        }
      } catch (error) {
        lastError = error;
      }

      if (attempt < MAX_ATTEMPTS) {
        await sleep(retryDelay(attempt));
      }
    }

    if (lastResponse) {
      return lastResponse;
    }
    if (lastError) {
      throw lastError;
    }
    return priorFetch(input, init);
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
