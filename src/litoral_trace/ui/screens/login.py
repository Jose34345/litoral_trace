"""Pantalla de Login B2B Enterprise - Litoral Trace."""
from __future__ import annotations
import streamlit as st

def login_screen() -> None:
    # CSS Glassmorphism
    st.markdown("""
    <style>
        .login-hero {
            background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
            border-radius: 16px;
            padding: 40px;
            color: #ffffff;
            margin-bottom: 24px;
            text-align: center;
        }
        .login-card {
            background: #ffffff;
            border: 1px solid #e2e8f0;
            border-radius: 12px;
            padding: 30px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.05);
            max-width: 450px;
            margin: 0 auto;
        }
    </style>
    """, unsafe_allow_html=True)

    # Hero Banner
    st.markdown("""
    <div class="login-hero">
        <h1 style="font-size: 2.2rem; font-weight: 700; margin-bottom: 8px; color: #38bdf8;">
            🛰️ Litoral Trace
        </h1>
        <p style="font-size: 1.05rem; color: #94a3b8; margin: 0;">
            Compliance Intelligence & Trazabilidad Forestal (EUDR)
        </p>
    </div>
    """, unsafe_allow_html=True)

    # Card Login
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("### Acceso Corporativo B2B")
        with st.form("login_form"):
            username = st.text_input("Usuario / Credencial", value="admin", placeholder="Ingrese su usuario")
            password = st.text_input("Clave de Seguridad", value="admin123", type="password")
            submit = st.form_submit_button("Iniciar Sesión", type="primary", use_container_width=True)

            if submit:
                if username == "admin" and password == "admin123":
                    st.session_state["logged_in"] = True
                    st.session_state["username"] = username
                    st.session_state["organization_name"] = "Exportadora Forestal del Chaco S.A."
                    st.session_state["rol"] = "admin"
                    st.session_state["organization_id"] = 1
                    st.success("✅ Acceso autorizado.")
                    st.rerun()
                else:
                    st.error("Credenciales incorrectas.")

        st.markdown("---")
        st.markdown("""
        <div style="text-align: center; font-size: 0.85rem; color: #64748b;">
            <b>¿Necesitas una Demostración Comercial en Vivo?</b><br/>
            📍 Resistencia, Chaco | Corrientes Capital<br/>
            📧 <b>comercial@litoraltrace.com</b> | 📲 <b>+54 9 379 4631300</b>
        </div>
        """, unsafe_allow_html=True)
