(() => {
  "use strict";

  const root = document.querySelector("[data-paddle-checkout]");
  if (!root || !window.Paddle) return;

  const environment = root.dataset.paddleEnvironment;
  const token = root.dataset.paddleToken;
  const priceId = root.dataset.paddlePriceId;
  const organizationId = root.dataset.organizationId;
  const paymentPublicId = root.dataset.paymentPublicId;
  const customerEmail = root.dataset.customerEmail;
  const button = root.querySelector("[data-paddle-open]");
  const status = root.querySelector("[data-paddle-status]");

  if (!token || !priceId || !organizationId || !paymentPublicId || !button) {
    if (status) status.textContent = "Secure checkout is temporarily unavailable.";
    return;
  }

  if (environment === "SANDBOX") {
    Paddle.Environment.set("sandbox");
  }

  Paddle.Initialize({
    token,
    eventCallback(event) {
      if (event && event.name === "checkout.completed") {
        button.disabled = true;
        if (status) {
          status.textContent =
            "Payment received by Paddle. Litoral Trace is confirming it server-side…";
        }
        window.setTimeout(() => window.location.reload(), 2500);
      }
    },
  });

  button.addEventListener("click", () => {
    button.disabled = true;
    if (status) status.textContent = "Opening secure checkout…";
    try {
      Paddle.Checkout.open({
        items: [{ priceId, quantity: 1 }],
        customer: customerEmail ? { email: customerEmail } : undefined,
        customData: {
          organization_id: Number(organizationId),
          payment_public_id: paymentPublicId,
        },
        settings: {
          displayMode: "overlay",
          theme: "light",
          locale: "en",
          allowLogout: false,
          showAddDiscounts: false,
          showAddTaxId: true,
        },
      });
      window.setTimeout(() => {
        if (button.disabled) button.disabled = false;
      }, 1500);
    } catch (_error) {
      button.disabled = false;
      if (status) {
        status.textContent =
          "Secure checkout could not be opened. Please try again or contact support.";
      }
    }
  });
})();
