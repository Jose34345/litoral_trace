"""Small server-rendered HTML helpers for the U.S. Lacey portal."""
from __future__ import annotations

from html import escape


def safe(value: object) -> str:
    return escape(str(value if value is not None else ""), quote=True)


def money(cents: int, currency: str = "USD") -> str:
    return f"{currency} {cents / 100:,.2f}"


def shell(title: str, body: str, *, authenticated: bool = False) -> str:
    nav = (
        '<a href="/billing">Billing</a> <form method="post" action="/logout" style="display:inline"><button type="submit">Sign out</button></form>'
        if authenticated
        else '<a href="/login">Sign in</a> <a href="/signup">Create account</a>'
    )
    css = """
    body{margin:0;background:#f5f7f6;color:#12231c;font-family:system-ui,-apple-system,sans-serif;line-height:1.5}
    a{color:#174d36;font-weight:650}.wrap{width:min(880px,calc(100% - 32px));margin:auto}
    nav{display:flex;justify-content:space-between;align-items:center;padding:22px 0}main{padding:36px 0 70px}
    .card{background:#fff;border:1px solid #dce4df;border-radius:16px;padding:28px;margin-top:22px}
    h1{font-size:clamp(34px,5vw,52px);line-height:1.05;letter-spacing:-.04em}p{color:#617068}
    label{display:block;font-weight:700;margin:14px 0 6px}input,select{width:100%;box-sizing:border-box;padding:12px;border:1px solid #cbd7d0;border-radius:9px;font:inherit}
    button,.button{display:inline-block;background:#174d36;color:#fff;border:0;border-radius:9px;padding:12px 17px;font:inherit;font-weight:750;text-decoration:none;cursor:pointer}
    .error{background:#fff0f0;color:#8f2727;padding:12px;border-radius:9px}.ok{background:#edf8f1;color:#17603b;padding:12px;border-radius:9px}.warn{background:#fff8e8;color:#7d5814;padding:12px;border-radius:9px}
    .row{display:grid;grid-template-columns:1fr 1fr;gap:16px}.meta{display:grid;grid-template-columns:1fr 1fr;gap:12px}.metric{border:1px solid #dce4df;border-radius:10px;padding:15px}.reference{font-family:monospace;font-size:18px;font-weight:800;word-break:break-all}.instructions{white-space:pre-wrap;border:1px solid #dce4df;padding:14px;border-radius:9px;background:#fafcfa}
    .check{display:flex;gap:9px;align-items:flex-start;margin:12px 0;color:#617068}.check input{width:auto;margin-top:5px}
    @media(max-width:700px){.row,.meta{grid-template-columns:1fr}}
    """
    return f'<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="robots" content="noindex,nofollow"><title>{safe(title)} · Litoral Trace</title><style>{css}</style></head><body><div class="wrap"><nav><strong>Litoral Trace · U.S. Lacey</strong><div>{nav}</div></nav><main>{body}</main></div></body></html>'


def render_login(*, error: str | None = None, verified: bool = False) -> str:
    notice = ""
    if verified:
        notice = '<div class="ok">Email verified. Sign in to continue to billing.</div>'
    elif error:
        notice = f'<div class="error">{safe(error)}</div>'
    body = f"""
    <h1>Sign in to your U.S. Lacey workspace.</h1>
    <p>Verified accounts can access their company billing status.</p>
    <section class="card">{notice}
      <form method="post" action="/login">
        <label for="email">Business email</label>
        <input id="email" name="email" type="email" required maxlength="255" autocomplete="username">
        <label for="password">Password</label>
        <input id="password" name="password" type="password" required autocomplete="current-password">
        <p><button type="submit">Sign in</button> &nbsp; <a href="/signup">Create an account</a></p>
      </form>
    </section>
    """
    return shell("Sign in", body)


def render_signup(*, commercial, portal, error: str | None = None) -> str:
    notice = f'<div class="error">{safe(error)}</div>' if error else ""
    body = f"""
    <h1>Create your private U.S. Lacey account.</h1>
    <p>Private beta: {safe(money(commercial.price_cents))} for up to {safe(commercial.monthly_operation_limit)} operations under the configured billing period.</p>
    <section class="card">{notice}
      <form method="post" action="/signup">
        <label for="legal_name">Company legal name</label>
        <input id="legal_name" name="legal_name" required maxlength="255" autocomplete="organization">
        <div class="row"><div>
          <label for="business_type">Business type</label>
          <select id="business_type" name="business_type" required><option value="IMPORTER">Importer</option><option value="CUSTOMS_BROKER">Customs broker</option><option value="OTHER">Other</option></select>
        </div><div>
          <label for="admin_name">Administrator name</label>
          <input id="admin_name" name="admin_name" required maxlength="255" autocomplete="name">
        </div></div>
        <label for="admin_email">Business email</label>
        <input id="admin_email" name="admin_email" type="email" required maxlength="255" autocomplete="email">
        <label for="password">Password</label>
        <input id="password" name="password" type="password" required minlength="12" autocomplete="new-password">
        <label class="check"><input type="checkbox" name="accept_terms" value="yes" required><span>I accept the <a target="_blank" rel="noopener" href="{safe(portal.terms_url)}">Terms of Service</a> ({safe(commercial.terms_version)}).</span></label>
        <label class="check"><input type="checkbox" name="accept_privacy" value="yes" required><span>I acknowledge the <a target="_blank" rel="noopener" href="{safe(portal.privacy_url)}">Privacy Policy</a> ({safe(commercial.privacy_version)}).</span></label>
        <label class="check"><input type="checkbox" name="accept_beta" value="yes" required><span>I accept the <a target="_blank" rel="noopener" href="{safe(portal.beta_terms_url)}">Private Beta Terms</a> ({safe(commercial.beta_terms_version)}).</span></label>
        <p><button type="submit">Create account</button> &nbsp; <a href="/login">Already registered?</a></p>
      </form>
    </section>
    """
    return shell("Create account", body)


def render_check_email(email: str) -> str:
    body = f"""
    <h1>Check your email.</h1>
    <p>We sent a verification link to <strong>{safe(email)}</strong>. It expires in 24 hours.</p>
    <section class="card"><h2>Next step</h2><p>After verification your account moves to <strong>Payment Pending</strong>. You can then sign in and view the exact payment reference and instructions.</p><a class="button" href="/login">Go to sign in</a></section>
    """
    return shell("Check your email", body)


def render_verification_error(message: str) -> str:
    body = f"""
    <h1>We could not verify this link.</h1>
    <section class="card"><div class="error">{safe(message)}</div><p>The link may be invalid or expired.</p><a class="button" href="/login">Return to sign in</a></section>
    """
    return shell("Verification failed", body)


def render_billing(*, identity, billing, commercial) -> str:
    if identity.account_status == "PAYMENT_PENDING":
        notice = '<div class="warn"><strong>Payment pending.</strong> Your email is verified, but document processing stays locked until payment is confirmed.</div>'
    elif identity.account_status in {"ACTIVE", "PILOT"}:
        notice = '<div class="ok"><strong>Account active.</strong> Billing is verified for this workspace.</div>'
    else:
        notice = f'<div class="warn">Account status: {safe(identity.account_status)}</div>'

    body = f"""
    <h1>{safe(identity.legal_name)}</h1>
    <p>Billing status for {safe(identity.email)}.</p>
    <section class="card">{notice}
      <div class="meta">
        <div class="metric"><small>Private beta price</small><br><strong>{safe(money(billing.price_cents, billing.currency))}</strong></div>
        <div class="metric"><small>Operations</small><br><strong>{safe(billing.used_operations)} / {safe(billing.monthly_operation_limit)}</strong></div>
        <div class="metric"><small>Subscription</small><br><strong>{safe(billing.subscription_status)}</strong></div>
        <div class="metric"><small>Payment</small><br><strong>{safe(billing.payment_status)}</strong></div>
      </div>
      <h2>Payment reference</h2>
      <p>Include this exact reference with the payment:</p>
      <div class="reference">{safe(billing.payment_reference)}</div>
      <h3>{safe(billing.payment_provider.replace("_", " ").title())}</h3>
      <div class="instructions">{safe(commercial.bank_transfer_instructions)}</div>
      <p>Activation occurs only after server-side payment verification. The browser cannot activate its own account.</p>
    </section>
    """
    return shell("Billing", body, authenticated=True)
