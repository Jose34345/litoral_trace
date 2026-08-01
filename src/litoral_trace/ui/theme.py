"""Sistema de Diseño Enterprise B2B para Litoral Trace."""
from __future__ import annotations

PALETTE = {
    "primary": "#1a1a1a",         # Carbón vegetal
    "success": "#1e5f3a",         # Verde compliance
    "error": "#dc2626",           # Rojo riesgo
    "background": "#f8f6f3",      # Blanco cálido
    "border": "#e2e8f0",
}

ENTERPRISE_THEME_CSS = """
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    html, body, [class*="css"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
        background-color: #f8fafc !important;
        color: #0f172a !important;
    }
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    .block-container {
        padding-top: 1.5rem !important;
        padding-bottom: 2rem !important;
        max-width: 1300px !important;
    }
    .kpi-container {
        background-color: #ffffff;
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        padding: 20px 24px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.04);
        margin-bottom: 12px;
    }
    .kpi-title {
        font-size: 0.80rem;
        font-weight: 600;
        color: #64748b;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .kpi-value {
        font-size: 2rem;
        font-weight: 700;
        color: #0f172a;
        margin-top: 6px;
        line-height: 1.1;
    }
    .kpi-sub {
        font-size: 0.85rem;
        color: #10b981;
        font-weight: 500;
        margin-top: 4px;
    }
</style>
"""

def apply_enterprise_theme() -> None:
    """Inyecta el tema corporativo Enterprise en la aplicación."""
    try:
        import streamlit as st
        st.markdown(ENTERPRISE_THEME_CSS, unsafe_allow_html=True)
    except Exception:
        pass

def render_kpi_card(title: str, value: str, subtext: str = "", is_alert: bool = False) -> None:
    """Renderiza una tarjeta de métrica KPI estilo B2B SaaS."""
    try:
        import streamlit as st
        sub_color = "#ef4444" if is_alert else "#10b981"
        sub_html = f'<div class="kpi-sub" style="color: {sub_color};">{subtext}</div>' if subtext else ''
        html = f"""
        <div class="kpi-container">
            <div class="kpi-title">{title}</div>
            <div class="kpi-value">{value}</div>
            {sub_html}
        </div>
        """
        st.markdown(html, unsafe_allow_html=True)
    except Exception:
        pass
