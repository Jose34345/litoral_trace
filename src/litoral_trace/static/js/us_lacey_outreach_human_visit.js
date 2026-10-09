(() => {
  "use strict";

  const endpoint = "/sandbox/engagement/human-visit";
  // Only first-party persona IDs are sent, never customer documents or values.
  const samplePersona = document.body.dataset.outreachPersona;
  const sampleStep = document.body.dataset.outreachStep;
  const payload = new URLSearchParams();
  if ((samplePersona === "importer" || samplePersona === "broker") &&
      (sampleStep === "1" || sampleStep === "2")) {
    payload.set("sample_persona", samplePersona);
    payload.set("sample_step", sampleStep);
  }
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
      const queued = navigator.sendBeacon(endpoint, payload);
      if (queued) {
        return;
      }
    }

    void fetch(endpoint, {
      method: "POST",
      credentials: "same-origin",
      cache: "no-store",
      keepalive: true,
      body: payload,
      headers: { "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8" },
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
