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

# --- PALETA DE COLORES CORPORATIVOS (BRANDING) ---
COMPANY_COLORS = {
    "YPF": "#00519E",          # Azul YPF
    "YPF. S.A.": "#00519E",
    "YPF S.A.": "#00519E",
    "VISTA": "#00A86B",        # Verde Vista
    "VISTA ENERGY": "#00A86B",
    "SHELL": "#FBCE07",        # Amarillo Shell
    "SHELL ARGENTINA": "#FBCE07",
    "PAN AMERICAN ENERGY": "#1F4E3D", # Verde Oscuro PAE
    "PAE": "#1F4E3D",
    "TECPETROL": "#F37021",    # Naranja Techint
    "PLUSPETROL": "#56A0D3",   # Celeste Pluspetrol
    "TOTAL": "#E01E37",        # Rojo Total
    "TOTAL AUSTRAL": "#E01E37",
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
        box-shadow: 2px 2px 5px rgba(0,0,0,0.3);
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 10px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background-color: #0E1117;
        border-radius: 4px 4px 0px 0px;
        gap: 1px;
        padding-top: 10px;
        padding-bottom: 10px;
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
except FileNotFoundError:
    st.error("⚠️ No se encontró configuración de acceso. Sube 'config.yaml' o configura los Secrets.")
    st.stop()

credentials = config['credentials'].to_dict() if hasattr(config['credentials'], 'to_dict') else config['credentials']
cookie_cfg = config['cookie'].to_dict() if hasattr(config['cookie'], 'to_dict') else config['cookie']

authenticator = stauth.Authenticate(
    credentials,
    cookie_cfg['name'],
    cookie_cfg['key'],
    cookie_cfg['expiry_days']
)

authenticator.login()

# --- LÓGICA DE ACCESO ---
if st.session_state["authentication_status"]:
    
    with st.sidebar:
        st.write(f"Hola, *{st.session_state['name']}* 👋")
        authenticator.logout('Cerrar Sesión', 'sidebar')
        st.divider()

    # --- INICIO DE LA APP ---
    API_URL = "https://vaca-muerta-intel.onrender.com"

    # --- FUNCIONES DE CONEXIÓN ---
    @st.cache_data(ttl=300)
    def get_lista_empresas():
        try:
            response = requests.get(f"{API_URL}/empresas")
            if response.status_code == 200:
                return response.json()['data']
            return []
        except:
            return []

    def get_data_empresa(empresa):
        try:
            response = requests.get(f"{API_URL}/produccion/{empresa}")
            if response.status_code == 200:
                df = pd.DataFrame(response.json())
                if not df.empty:
                    df['fecha'] = pd.to_datetime(df['fecha_data'])
                    df['empresa'] = empresa
                    return df
            return pd.DataFrame()
        except:
            return pd.DataFrame()

    @st.cache_data
    def convert_df(df):
        return df.to_csv(index=False).encode('utf-8')

    def get_eficiencia_empresa(empresa):
        try:
            response = requests.get(f"{API_URL}/eficiencia/{empresa}")
            if response.status_code == 200:
                df = pd.DataFrame(response.json())
                if not df.empty:
                    df['fecha'] = pd.to_datetime(df['fecha_data'])
                    df['empresa'] = empresa
                    return df
            return pd.DataFrame()
        except:
            return pd.DataFrame()

    def get_curva_tipo(empresa):
        try:
            response = requests.get(f"{API_URL}/curvas-tipo/{empresa}")
            if response.status_code == 200:
                return pd.DataFrame(response.json())
            return pd.DataFrame()
        except:
            return pd.DataFrame()

    @st.cache_data(ttl=3600)
    def get_ducs_data():
        try:
            response = requests.get(f"{API_URL}/ducs")
            if response.status_code == 200:
                return pd.DataFrame(response.json())
            return pd.DataFrame()
        except:
            return pd.DataFrame()

    @st.cache_data(ttl=3600)
    def get_venteo_data():
        try:
            response = requests.get(f"{API_URL}/venteo")
            if response.status_code == 200:
                return pd.DataFrame(response.json())
            return pd.DataFrame()
        except:
            return pd.DataFrame()

    @st.cache_data(ttl=3600)
    def get_mapa_data():
        """Obtiene datos geográficos para el mapa."""
        try:
            response = requests.get(f"{API_URL}/ducs")
            if response.status_code == 200:
                data = response.json()
                if not data:
                    return pd.DataFrame(), "API respondió lista vacía []"
                return pd.DataFrame(data), "OK"
            return pd.DataFrame(), f"Error API: Status {response.status_code}"
        except Exception as e:
            return pd.DataFrame(), f"Fallo de conexión: {str(e)}"

    # --- HEADER PRINCIPAL ---
    st.title("⚡ Vaca Muerta Intelligence 2.0")
    st.markdown("Plataforma de análisis estratégico en tiempo real. **Backend:** Online 🟢")
    st.markdown("---")

    # --- SIDEBAR ---
    st.sidebar.image("https://cdn-icons-png.flaticon.com/512/2103/2103633.png", width=50)
    st.sidebar.title("Panel de Control")

    lista_empresas = get_lista_empresas()

    if lista_empresas:
        st.sidebar.subheader("🎯 Selección Rápida")
        
        top_majors = ["YPF", "VISTA", "PAN AMERICAN ENERGY", "SHELL", "PLUSPETROL", "TECPETROL"]
        majors_avail = [e for e in top_majors if e in lista_empresas]
        
        selected_majors = st.sidebar.pills(
            "Principales Operadoras", 
            majors_avail, 
            selection_mode="multi", 
            default=["YPF"] if "YPF" in majors_avail else None
        )
        
        st.sidebar.markdown("---")
        st.sidebar.caption("Otras Operadoras")
        other_options = [e for e in lista_empresas if e not in majors_avail]
        selected_others = st.sidebar.multiselect("Buscar en el listado completo:", other_options)
        
        if selected_majors is None:
            selected_majors = []
            
        empresas_sel = list(set(selected_majors + selected_others))

        if empresas_sel:
            # --- CARGA DE DATOS ---
            all_data = []
            for emp in empresas_sel:
                df_temp = get_data_empresa(emp)
                if not df_temp.empty:
                    all_data.append(df_temp)
            
            if all_data:
                df_view = pd.concat(all_data)
                
                # --- KPIs ---
                c1, c2, c3 = st.columns(3, gap="medium")
                total_rev = df_view['revenue_usd'].sum()
                
                c1.metric("🛢️ Petróleo Total", f"{df_view['petroleo'].sum()/1e6:,.1f} M m³", delta_color="normal")
                c2.metric("🔥 Gas Total", f"{df_view['gas'].sum()/1e6:,.1f} B m³", delta_color="normal")
                c3.metric("💵 Facturación (Est.)", f"US$ {total_rev/1_000_000:,.1f} M", delta="YTD 2024")
                
                st.markdown(" ") 

                # --- PESTAÑAS ---
                tab1, tab2, tab3, tab4, tab5 = st.tabs([
                    "📍 Mapa & Producción", "💰 Finanzas", "🔮 IA Predictiva", "⚙️ Ingeniería", "📉 Benchmarking"
                ])
                
                # PESTAÑA 1: MAPA + PRODUCCIÓN
                with tab1:
                    st.header("📍 Inteligencia Territorial")
                    
                    # --- BLOQUE DE DIAGNÓSTICO (NUEVO) ---
                    df_mapa, status_msg = get_mapa_data()
                    
                    with st.expander("🔍 Estado Técnico de Datos GIS (Mapa)"):
                        st.write(f"**URL API:** {API_URL}/ducs")
                        st.write(f"**Resultado Conexión:** {status_msg}")
                        
                        if not df_mapa.empty:
                            st.success(f"¡Datos recibidos! Filas: {len(df_mapa)}")
                            st.write("**Columnas detectadas:**", df_mapa.columns.tolist())
                            st.write("**Vista previa de coordenadas:**")
                            st.dataframe(df_mapa[['empresa', 'latitud', 'longitud']].head())
                            
                            if st.button("♻️ Forzar Recarga de Datos (Limpiar Caché)"):
                                st.cache_data.clear()
                                st.rerun()
                        else:
                            st.error("No se recibieron datos para el mapa.")
                    
                    # --- RENDERIZADO DEL MAPA ---
                    if not df_mapa.empty and "latitud" in df_mapa.columns and "longitud" in df_mapa.columns:
                        col_map_text, col_map_viz = st.columns([1, 3])
                        
                        with col_map_text:
                            st.info("Activos geolocalizados en la formación Vaca Muerta.")
                            if "distancia_ducto_km" in df_mapa.columns:
                                avg_dist = df_mapa['distancia_ducto_km'].mean()
                                avg_capex = df_mapa['capex_conexion_usd'].mean() if "capex_conexion_usd" in df_mapa.columns else 0
                                st.metric("Distancia Media a Ducto", f"{avg_dist:.1f} km")
                                st.metric("CAPEX Conexión Promedio", f"US$ {avg_capex/1000:.0f} k")
                        
                        with col_map_viz:
                            # Configuración del color según datos disponibles
                            color_col = "capex_conexion_usd" if "capex_conexion_usd" in df_mapa.columns else "empresa"
                            color_scale = "Jet" if "capex_conexion_usd" in df_mapa.columns else None
                            
                            fig_mapa = px.scatter_mapbox(
                                df_mapa, 
                                lat="latitud", 
                                lon="longitud",
                                color=color_col, 
                                size="ducs" if "ducs" in df_mapa.columns else None,
                                color_continuous_scale=color_scale,
                                color_discrete_map=COMPANY_COLORS,
                                hover_name="empresa",
                                hover_data=["ducs"] if "ducs" in df_mapa.columns else [],
                                zoom=8, 
                                height=600,
                                title="Mapa de Activos e Infraestructura"
                            )
                            
                            fig_mapa.update_layout(
                                mapbox_style="carto-darkmatter", 
                                margin={"r":0,"t":40,"l":0,"b":0}
                            )
                            st.plotly_chart(fig_mapa, use_container_width=True)
                    else:
                        st.warning("⚠️ No se pueden mostrar los puntos: Faltan coordenadas en la base de datos.")

                    st.divider()

                    # --- SECCIÓN PRODUCCIÓN ---
                    st.subheader("📊 Curva de Producción de Petróleo")
                    col_title, col_download = st.columns([4, 1])
                    with col_download:
                        csv = convert_df(df_view)
                        st.download_button(
                            label="📥 Descargar Data",
                            data=csv,
                            file_name=f'produccion_vaca_muerta_{pd.Timestamp.now().date()}.csv',
                            mime='text/csv',
                            key='download-prod'
                        )
                    
                    fig = px.area(df_view, x='fecha', y='petroleo', color='empresa', 
                                  color_discrete_map=COMPANY_COLORS,
                                  color_discrete_sequence=DEFAULT_COLOR_SEQ)
                    
                    fig.update_layout(
                        xaxis_title="Fecha", yaxis_title="Producción (m³)",
                        legend_title="Operadora", hovermode="x unified"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # PESTAÑA 2: FINANZAS
                with tab2:
                    st.subheader("Ranking Financiero")
                    col_rank, col_pie = st.columns([2,1])
                    with col_rank:
                        df_rank = df_view.groupby('empresa')['revenue_usd'].sum().reset_index().sort_values('revenue_usd', ascending=True)
                        fig_bar = px.bar(df_rank, x='revenue_usd', y='empresa', orientation='h', text_auto='.2s',
                                        color='empresa', color_discrete_map=COMPANY_COLORS)
                        fig_bar.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
                        fig_bar.update_layout(showlegend=False)
                        st.plotly_chart(fig_bar, use_container_width=True)
                    
                    with col_pie:
                        fig_pie = px.pie(df_rank, values='revenue_usd', names='empresa', hole=0.4,
                                        color='empresa', color_discrete_map=COMPANY_COLORS)
                        st.plotly_chart(fig_pie, use_container_width=True)

                    st.divider()
                    st.header("💎 Simulación de Escenarios Financieros")
                    c_scen, c_oil, c_gas = st.columns([2, 1, 1])
                    with c_scen:
                        scenario_type = st.radio("Perfil de Mercado:", ["🐻 Bear (Pesimista)", "⚓ Base (Actual)", "🐂 Bull (Optimista)"], horizontal=True, index=1)

                    if "Bear" in scenario_type:
                        p_b, p_g, p_f = 55.0, 2.5, 0.90
                    elif "Bull" in scenario_type:
                        p_b, p_g, p_f = 95.0, 6.0, 1.15
                    else: 
                        p_b, p_g, p_f = 75.0, 4.0, 1.0

                    with c_oil: brent_in = st.number_input("Precio Brent (USD/bbl)", value=p_b, step=1.0)
                    with c_gas: gas_in = st.number_input("Precio Gas (USD/MMBtu)", value=p_g, step=0.1)

                    actual_rev = df_view['revenue_usd'].sum()
                    sim_rev = actual_rev * (brent_in / 75.0) * p_f
                    st.metric("Revenue Proyectado", f"US$ {sim_rev/1e6:,.1f} M", delta=f"{((sim_rev/actual_rev)-1)*100:.1f}%")

                # PESTAÑA 3: IA
                with tab3:
                    st.subheader("IA Predictiva")
                    empresa_pred = st.selectbox("Operadora para IA:", empresas_sel)
                    if empresa_pred:
                        df_ia = df_view[df_view['empresa'] == empresa_pred].copy().rename(columns={'petroleo': 'prod_pet'})
                        if len(df_ia) > 6:
                            pred = predecir_produccion(df_ia)
                            fig_ia = px.line(pred, x='fecha', y='prod_pet_pred', title=f"Predicción: {empresa_pred}",
                                           color_discrete_sequence=[COMPANY_COLORS.get(empresa_pred, "white")])
                            st.plotly_chart(fig_ia, use_container_width=True)

                # PESTAÑA 4: INGENIERÍA
                with tab4:
                    st.header("⚙️ Ingeniería & ESG")
                    df_venteo = get_venteo_data()
                    if not df_venteo.empty:
                        st.plotly_chart(px.bar(df_venteo, x='empresa', y='ratio_venteo', title="Gas Flaring (%)"), use_container_width=True)
                    
                    df_ing = pd.DataFrame()
                    for emp in empresas_sel:
                        df_ing = pd.concat([df_ing, get_eficiencia_empresa(emp)])
                    if not df_ing.empty:
                        st.plotly_chart(px.line(df_ing, x='fecha', y='gor_promedio', color='empresa', title="Gas-Oil Ratio"), use_container_width=True)

                # PESTAÑA 5: BENCHMARKING
                with tab5:
                    st.header("Benchmarking")
                    df_ducs_b = get_ducs_data()
                    if not df_ducs_b.empty:
                        st.plotly_chart(px.bar(df_ducs_b, x='empresa', y='ducs', title="Inventario de DUCs"), use_container_width=True)
                    
                    df_curves = pd.DataFrame()
                    for emp in empresas_sel:
                        dt = get_curva_tipo(emp)
                        if not dt.empty:
                            dt['empresa'] = emp
                            df_curves = pd.concat([df_curves, dt])
                    if not df_curves.empty:
                        st.plotly_chart(px.line(df_curves, x='mes_n', y='promedio_petroleo', color='empresa', title="Curvas Tipo"), use_container_width=True)
        else:
            st.info("👈 Selecciona una o más operadoras en el panel lateral.")
    else:
        st.error("Error de conexión con el Servidor. Revisa el estado de Render.")

elif st.session_state["authentication_status"] is False:
    st.error('❌ Usuario o contraseña incorrectos')
elif st.session_state["authentication_status"] is None:
    st.warning('🔒 Por favor, ingresa tus credenciales para acceder.')