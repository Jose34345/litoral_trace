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
    @media(max-width:700px){.row,.meta{grid-template-columns:1fr}}
    """
    return f'<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="robots" content="noindex,nofollow"><title>{safe(title)} · Litoral Trace</title><style>{css}</style></head><body><div class="wrap"><nav><strong>Litoral Trace · U.S. Lacey</strong><div>{nav}</div></nav><main>{body}</main></div></body></html>'
