"""Navegación B2B Enterprise para Litoral Trace."""
from __future__ import annotations
import streamlit as st

PANTALLA_DASHBOARD = "dashboard"
PANTALLA_AUDITORIA = "auditoria"

def init_pantalla() -> None:
    if "pantalla" not in st.session_state:
        st.session_state["pantalla"] = PANTALLA_DASHBOARD

def render_sidebar_header() -> None:
    st.markdown("""
    <div style="padding: 10px 0 15px 0; border-bottom: 1px solid #334155;">
        <h2 style="margin: 0; font-size: 1.3rem; font-weight: 700; color: #38bdf8 !important;">
            🛰️ Litoral Trace
        </h2>
        <span style="font-size: 0.75rem; color: #94a3b8 !important; text-transform: uppercase; letter-spacing: 0.05em;">
            Compliance Intelligence B2B
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    org_name = st.session_state.get("organization_name", "Organización Demo")
    user = st.session_state.get("username", "admin")
    role = st.session_state.get("rol", "cliente").upper()
    
    st.markdown(f"""
    <div style="margin-top: 12px; font-size: 0.85rem; color: #cbd5e1 !important;">
        🏢 <b>{org_name}</b><br/>
        👤 {user} <span style="font-size: 0.75rem; background: #334155; padding: 2px 6px; border-radius: 4px;">{role}</span>
    </div>
    <div style="margin-bottom: 15px;"></div>
    """, unsafe_allow_html=True)

def render_nav_buttons() -> None:
    if st.button("📊 Panel de Control", use_container_width=True, key="nav_dashboard"):
        st.session_state["pantalla"] = PANTALLA_DASHBOARD
        st.rerun()
        
    if st.button("📜 Registro de Auditoría", use_container_width=True, key="nav_auditoria"):
        st.session_state["pantalla"] = PANTALLA_AUDITORIA
        st.rerun()

def render_logout_button() -> None:
    st.markdown("---")
    if st.button("🚪 Cerrar Sesión", use_container_width=True, key="nav_logout"):
        st.session_state["logged_in"] = False
        st.session_state["pantalla"] = PANTALLA_DASHBOARD
        st.rerun()
