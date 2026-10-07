(() => {
  "use strict";

  const endpoint = "/sandbox/engagement/human-visit";
  let sent = false;
  let dwellTimer = null;

  const browserLooksHuman = () =>
    document.visibilityState === "visible" &&
    document.prerendering !== true &&
    navigator.webdriver !== true;

  const send = () => {
    if (sent || !browserLooksHuman()) {
      return;
    }
    sent = true;
    if (dwellTimer !== null) {
      window.clearTimeout(dwellTimer);
      dwellTimer = null;
    }

    if (navigator.sendBeacon) {
      const queued = navigator.sendBeacon(endpoint, new Blob([], { type: "text/plain" }));
      if (queued) {
        return;
      }
    }

    void fetch(endpoint, {
      method: "POST",
      credentials: "same-origin",
      cache: "no-store",
      keepalive: true,
      headers: { "X-Litoral-Human-Signal": "browser-visible" },
    }).catch(() => undefined);
  };

  const armDwellSignal = () => {
    if (!browserLooksHuman() || sent || dwellTimer !== null) {
      return;
    }
    dwellTimer = window.setTimeout(send, 4000);
  };

  const interactionEvents = ["pointerdown", "keydown", "touchstart", "wheel"];
  for (const eventName of interactionEvents) {
    window.addEventListener(eventName, send, { once: true, passive: true });
  }

  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") {
      armDwellSignal();
    } else if (dwellTimer !== null) {
      window.clearTimeout(dwellTimer);
      dwellTimer = null;
    }
  });

  armDwellSignal();
})();
