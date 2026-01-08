import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import requests
import yaml
from yaml.loader import SafeLoader
import streamlit_authenticator as stauth
from ml_forecasting import predecir_produccion 

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(
    page_title="Vaca Muerta Intelligence", 
    page_icon="⚡", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- PALETA DE COLORES CORPORATIVOS ---
COMPANY_COLORS = {
    "YPF": "#00519E",
    "VISTA": "#00A86B",
    "SHELL": "#FBCE07",
    "PAE": "#1F4E3D",
    "TECPETROL": "#F37021",
    "PLUSPETROL": "#56A0D3",
    "TOTAL": "#E01E37",
    "EXXONMOBIL": "#FF0000",
    "PHOENIX": "#800080"
}
DEFAULT_COLOR_SEQ = px.colors.qualitative.Vivid

# --- ESTILOS CSS ---
st.markdown("""
<style>
    [data-testid="stMetric"] {
        background-color: #1E1E1E;
        border-radius: 10px;
        padding: 15px;
        border: 1px solid #333;
    }
    .stTabs [data-baseweb="tab-list"] { gap: 10px; }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background-color: #0E1117;
        border-radius: 4px 4px 0px 0px;
    }
    .stTabs [aria-selected="true"] {
        background-color: #262730;
        border-bottom: 2px solid #FF4B4B;
    }
</style>
""", unsafe_allow_html=True)

# --- SISTEMA DE AUTENTICACIÓN (HÍBRIDO) ---
try:
    if 'credentials' in st.secrets:
        config = st.secrets
    else:
        with open('config.yaml') as file:
            config = yaml.load(file, Loader=SafeLoader)
except:
    st.error("⚠️ Error en configuración de acceso.")
    st.stop()

credentials = config['credentials'].to_dict() if hasattr(config['credentials'], 'to_dict') else config['credentials']
authenticator = stauth.Authenticate(
    credentials,
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days']
)

authenticator.login()

# --- LÓGICA PRINCIPAL ---
if st.session_state["authentication_status"]:
    API_URL = "https://vaca-muerta-intel.onrender.com"

    # --- FUNCIONES DE CONEXIÓN ---
    @st.cache_data(ttl=300)
    def get_api_data(endpoint):
        try:
            response = requests.get(f"{API_URL}/{endpoint}")
            return pd.DataFrame(response.json()) if response.status_code == 200 else pd.DataFrame()
        except: return pd.DataFrame()

    @st.cache_data(ttl=300)
    def get_lista_empresas():
        try:
            r = requests.get(f"{API_URL}/empresas")
            return r.json()['data'] if r.status_code == 200 else []
        except: return []

    # --- SIDEBAR (ESTILO ANTERIOR RECUPERADO) ---
    with st.sidebar:
        st.image("https://cdn-icons-png.flaticon.com/512/2103/2103633.png", width=50)
        st.title("Panel de Control")
        st.write(f"Hola, *{st.session_state['name']}* 👋")
        authenticator.logout('Cerrar Sesión', 'sidebar')
        st.divider()

        lista_empresas = get_lista_empresas()
        
        if lista_empresas:
            st.subheader("🎯 Selección Rápida")
            # Lista de Majors para los botones
            top_majors = ["YPF", "VISTA", "PAE", "SHELL", "PLUSPETROL", "TECPETROL"]
            majors_avail = [e for e in top_majors if e in lista_empresas]
            
            selected_majors = st.pills(
                "Principales Operadoras", 
                majors_avail, 
                selection_mode="multi", 
                default=["YPF"] if "YPF" in majors_avail else None
            )
            
            st.markdown("---")
            st.caption("Otras Operadoras")
            other_options = [e for e in lista_empresas if e not in majors_avail]
            selected_others = st.multiselect("Buscar en el listado completo:", other_options)
            
            # Combinamos ambas selecciones
            final_selection = list(set((selected_majors or []) + (selected_others or [])))
        else:
            final_selection = []
            st.error("No se pudo conectar con la API para listar empresas.")

    # --- CONTENIDO PRINCIPAL ---
    st.title("⚡ Vaca Muerta Intelligence 2.0")
    
    if final_selection:
        # Carga de datos de producción
        all_data = []
        for emp in final_selection:
            df_temp = get_api_data(f"produccion/{emp}")
            if not df_temp.empty:
                df_temp['fecha'] = pd.to_datetime(df_temp['fecha_data'])
                df_temp['empresa'] = emp
                all_data.append(df_temp)
        
        if all_data:
            df_view = pd.concat(all_data)
            df_mapa = get_api_data("ducs") # Traemos datos GIS para el mapa y logística
            
            # KPIs en el Header
            c1, c2, c3 = st.columns(3)
            c1.metric("🛢️ Petróleo Total", f"{df_view['petroleo'].sum()/1e6:,.1f} M m³")
            c2.metric("💵 Facturación Est.", f"US$ {df_view['revenue_usd'].sum()/1e6:,.1f} M")
            c3.metric("🚜 Stock DUCs", f"{len(df_mapa) if not df_mapa.empty else 0}")

            # PESTAÑAS
            tab1, tab2, tab3, tab4, tab5 = st.tabs([
                "📍 Mapa & Activos", "💰 Finanzas", "🔮 IA Predictiva", "⚙️ Ingeniería", "📉 Benchmarking"
            ])

            # TAB 1: MAPA
            with tab1:
                st.header("📍 Inteligencia Territorial")
                if not df_mapa.empty and "latitud" in df_mapa.columns:
                    col_map_text, col_map_viz = st.columns([1, 3])
                    with col_map_text:
                        st.info("Burbujas: Inventario de pozos DUCs por operadora.")
                        if "capex_conexion_usd" in df_mapa.columns:
                            st.metric("CAPEX Conexión Promedio", f"US$ {df_mapa['capex_conexion_usd'].mean()/1000:.0f} k")
                    
                    with col_map_viz:
                        fig_map = px.scatter_mapbox(
                            df_mapa, lat="latitud", lon="longitud", color="empresa", 
                            size="ducs", hover_name="empresa", zoom=8, height=500,
                            color_discrete_map=COMPANY_COLORS, mapbox_style="carto-darkmatter"
                        )
                        st.plotly_chart(fig_map, use_container_width=True)
                else:
                    st.warning("Datos geográficos no disponibles.")

            # TAB 2: FINANZAS
            with tab2:
                st.subheader("💰 Simulación de Ingresos")
                brent_sim = st.slider("Precio Brent Proyectado (USD/bbl)", 40, 110, 75)
                actual_rev = df_view['revenue_usd'].sum()
                proy_rev = actual_rev * (brent_sim / 75.0)
                st.metric("Revenue Proyectado", f"US$ {proy_rev/1e6:,.1f} M", delta=f"{((proy_rev/actual_rev)-1)*100:.1f}%")
                
                st.plotly_chart(px.bar(df_view.groupby('empresa')['revenue_usd'].sum().reset_index(), 
                                       x='revenue_usd', y='empresa', orientation='h', color='empresa',
                                       color_discrete_map=COMPANY_COLORS, title="Ranking de Facturación"))

            # TAB 3: IA
            with tab3:
                st.subheader("🔮 Proyección de Producción (ML)")
                emp_ia = st.selectbox("Seleccionar Empresa:", final_selection)
                df_ia = df_view[df_view['empresa'] == emp_ia].copy().rename(columns={'petroleo': 'prod_pet'})
                if len(df_ia) > 6:
                    pred = predecir_produccion(df_ia)
                    st.plotly_chart(px.line(pred, x='fecha', y='prod_pet_pred', title=f"Predicción 12 meses: {emp_ia}"))

            # TAB 4: INGENIERÍA
            with tab4:
                st.header("⚙️ Ingeniería & ESG")
                df_venteo = get_api_data("venteo")
                if not df_venteo.empty:
                    st.plotly_chart(px.bar(df_venteo, x='empresa', y='ratio_venteo', title="Intensidad de Venteo (%)", color_discrete_sequence=['#FF4B4B']))

            # TAB 5: BENCHMARKING (NUEVO KPI LOGÍSTICO)
            with tab5:
                st.header("📉 Benchmarking de Eficiencia Logística")
                if not df_mapa.empty and "distancia_ducto_km" in df_mapa.columns:
                    c_b1, c_b2 = st.columns(2)
                    with c_b1:
                        st.subheader("🚚 Distancia a Infraestructura")
                        st.plotly_chart(px.bar(df_mapa, x='empresa', y='distancia_ducto_km', color='empresa', 
                                              color_discrete_map=COMPANY_COLORS, labels={'distancia_ducto_km': 'KM'}), use_container_width=True)
                    with c_b2:
                        st.subheader("💰 CAPEX de Conexión (USD)")
                        st.plotly_chart(px.bar(df_mapa, x='empresa', y='capex_conexion_usd', color='empresa', 
                                              color_discrete_map=COMPANY_COLORS, labels={'capex_conexion_usd': 'USD'}), use_container_width=True)
                    st.info("💡 Este análisis permite identificar qué operadoras tienen activos con logística más eficiente.")
                else:
                    st.info("Selecciona operadoras con datos de DUCs para ver el benchmarking logístico.")

    else:
        st.info("👈 Selecciona operadoras en el panel lateral para visualizar el análisis.")

elif st.session_state["authentication_status"] is False:
    st.error('❌ Usuario o contraseña incorrectos')