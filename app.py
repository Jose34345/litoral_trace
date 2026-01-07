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

# --- SISTEMA DE AUTENTICACIÓN (CORREGIDO) ---
# Intentamos cargar desde Secrets (Nube) o Archivo Local (PC) para que no falle.
try:
    if 'credentials' in st.secrets:
        config = st.secrets
    else:
        with open('config.yaml') as file:
            config = yaml.load(file, Loader=SafeLoader)
except FileNotFoundError:
    st.error("⚠️ No se encontró configuración de acceso. Sube 'config.yaml' o configura los Secrets.")
    st.stop()

# Manejo seguro de diccionarios para compatibilidad entre YAML y Secrets
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
                return pd.DataFrame(response.json())
            return pd.DataFrame()
        except:
            return pd.DataFrame()

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
                    # --- SECCIÓN 1: MAPA INTERACTIVO ---
                    st.header("📍 Inteligencia Territorial")
                    
                    df_mapa = get_mapa_data()
                    
                    if not df_mapa.empty and "latitud" in df_mapa.columns:
                        col_map_text, col_map_viz = st.columns([1, 3])
                        
                        with col_map_text:
                            st.info("Visualización de activos geolocalizados en la formación Vaca Muerta.")
                            
                            if "distancia_ducto_km" in df_mapa.columns:
                                avg_dist = df_mapa['distancia_ducto_km'].mean()
                                avg_capex = df_mapa['capex_conexion_usd'].mean() if "capex_conexion_usd" in df_mapa.columns else 0
                                st.metric("Distancia Media a Ducto", f"{avg_dist:.1f} km")
                                st.metric("CAPEX Conexión Promedio", f"US$ {avg_capex/1000:.0f} k")
                                st.caption("Costo estimado de flowlines.")
                        
                        with col_map_viz:
                            # Lógica inteligente de coloreado
                            if "capex_conexion_usd" in df_mapa.columns:
                                color_col = "capex_conexion_usd"
                                title_map = "Mapa de Costos Logísticos (CAPEX Conexión)"
                                color_scale = "Jet" # Rojo es caro
                            else:
                                color_col = "empresa"
                                title_map = "Ubicación de Activos por Operadora"
                                color_scale = None # Usa colores discretos
                            
                            fig_mapa = px.scatter_mapbox(
                                df_mapa, 
                                lat="latitud", 
                                lon="longitud",
                                color=color_col, 
                                size="ducs", 
                                color_continuous_scale=color_scale,
                                color_discrete_map=COMPANY_COLORS, 
                                hover_name="empresa",
                                hover_data=["ducs"],
                                zoom=8, 
                                height=500,
                                title=title_map
                            )
                            
                            fig_mapa.update_layout(
                                mapbox_style="carto-darkmatter", 
                                margin={"r":0,"t":40,"l":0,"b":0}
                            )
                            st.plotly_chart(fig_mapa, use_container_width=True)
                    else:
                        st.warning("⚠️ Cargando datos geográficos... (Si esto persiste, verifica el script de carga GIS)")

                    st.divider()

                    # --- SECCIÓN 2: PRODUCCIÓN ---
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
                        xaxis_title="Fecha",
                        yaxis_title="Producción (m³)",
                        legend_title="Operadora",
                        hovermode="x unified"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # PESTAÑA 2: FINANZAS (SIMULADOR)
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
                        csv_rank = convert_df(df_rank)
                        st.download_button(
                            label="📥 Bajar Ranking",
                            data=csv_rank,
                            file_name='ranking_financiero.csv',
                            mime='text/csv',
                            key='download-rank'
                        )
                        fig_pie = px.pie(df_rank, values='revenue_usd', names='empresa', hole=0.4,
                                        color='empresa', color_discrete_map=COMPANY_COLORS)
                        st.plotly_chart(fig_pie, use_container_width=True)

                    st.divider()

                    # --- SIMULADOR FINANCIERO (Cockpit) ---
                    st.header("💎 Simulación de Escenarios Financieros")
                    st.markdown("Stress-test de ingresos bajo diferentes condiciones de mercado.")

                    c_scen, c_oil, c_gas = st.columns([2, 1, 1])
                    
                    with c_scen:
                        st.subheader("1. Elegir Escenario")
                        scenario_type = st.radio(
                            "Perfil de Mercado:",
                            ["🐻 Bear (Pesimista)", "⚓ Base (Actual)", "🐂 Bull (Optimista)"],
                            horizontal=True,
                            index=1
                        )

                    if "Bear" in scenario_type:
                        price_brent_sim = 55.0
                        price_gas_sim = 2.5
                        prod_factor = 0.90
                    elif "Bull" in scenario_type:
                        price_brent_sim = 95.0
                        price_gas_sim = 6.0
                        prod_factor = 1.15
                    else: 
                        price_brent_sim = 75.0
                        price_gas_sim = 4.0
                        prod_factor = 1.0

                    with c_oil:
                        brent_input = st.number_input("Precio Brent (USD/bbl)", value=price_brent_sim, step=1.0, format="%.2f")
                    
                    with c_gas:
                        gas_input = st.number_input("Precio Gas (USD/MMBtu)", value=price_gas_sim, step=0.1, format="%.2f")

                    # CÁLCULOS
                    df_sim = df_view.copy()
                    total_revenue_actual = df_sim['revenue_usd'].sum()
                    
                    factor_ajuste_precio = (brent_input / 75.0) 
                    total_revenue_simulado = total_revenue_actual * factor_ajuste_precio * prod_factor
                    
                    delta_rev = total_revenue_simulado - total_revenue_actual
                    delta_pct = (delta_rev / total_revenue_actual) * 100 if total_revenue_actual > 0 else 0

                    # MÉTRICAS
                    st.markdown("---")
                    c_m1, c_m2, c_m3 = st.columns(3)
                    c_m1.metric("Revenue Proyectado (Anual)", f"US$ {total_revenue_simulado/1e6:,.1f} M", 
                                delta=f"{delta_pct:.1f}% vs Actual", delta_color="normal")
                    c_m2.metric("Impacto en Caja", f"US$ {delta_rev/1e6:,.1f} M", 
                                delta="Ganancia/Pérdida Neta", delta_color="off")
                    c_m3.metric("Sensibilidad Precio", f"{brent_input} USD/bbl", 
                                delta=f"{(brent_input-75):.1f} vs Base")

                    # HEATMAP RENTABILIDAD
                    st.markdown("### 🌡️ Matriz de Rentabilidad (Profit Heatmap)")
                    st.caption(f"Margen Operativo estimado sobre Breakeven de USD 45/bbl.")

                    rango_precios = np.linspace(brent_input * 0.7, brent_input * 1.3, 5)
                    rango_prod = np.linspace(prod_factor * 0.8, prod_factor * 1.2, 5)
                    
                    # Lógica Marín (Breakeven 45 USD)
                    breakeven_full = 45.0
                    z_data = []
                    for p_fac in rango_prod:
                        row = []
                        for pr in rango_precios:
                            margen = pr - breakeven_full
                            # Resultado proxy
                            res = (total_revenue_actual / 75.0) * margen * p_fac
                            row.append(res / 1e6) 
                        z_data.append(row)

                    fig_heat = go.Figure(data=go.Heatmap(
                        z=z_data,
                        x=[f"${x:.0f}" for x in rango_precios],
                        y=[f"{y*100:.0f}%" for y in rango_prod],
                        colorscale='RdYlGn',
                        zmid=0, # Punto cero es el blanco
                        texttemplate="$%{z:.0f}M",
                        textfont={"size": 12}
                    ))
                    
                    fig_heat.update_layout(
                        title="Impacto en Margen Operativo (Millones USD)",
                        xaxis_title="Precio del Barril (Brent)",
                        yaxis_title="% Eficiencia Producción",
                        height=400
                    )
                    
                    st.plotly_chart(fig_heat, use_container_width=True)

                # PESTAÑA 3: IA PREDICTIVA
                with tab3:
                    st.subheader("Simulación de Escenarios Futuros")
                    c_sim1, c_sim2 = st.columns([1, 3])
                    with c_sim1:
                        empresa_pred = st.selectbox("Seleccionar Operadora para IA:", empresas_sel)
                        precio_futuro = st.slider("Precio Brent Futuro (IA) US$:", 40, 100, 75)
                    
                    with c_sim2:
                        if empresa_pred:
                            df_ia = df_view[df_view['empresa'] == empresa_pred].copy()
                            df_ia = df_ia.rename(columns={'petroleo': 'prod_pet'}).sort_values('fecha')
                            
                            if len(df_ia) > 6:
                                pred = predecir_produccion(df_ia)
                                pred['revenue_proyectado'] = pred['prod_pet_pred'] * precio_futuro
                                
                                fig_ia = px.line(pred, x='fecha', y='prod_pet_pred', title=f"Proyección 12 Meses: {empresa_pred}",
                                               color_discrete_sequence=[COMPANY_COLORS.get(empresa_pred, "white")])
                                fig_ia.add_vline(x=pd.Timestamp.now().timestamp()*1000, line_dash="dash", annotation_text="Hoy")
                                st.plotly_chart(fig_ia, use_container_width=True)
                                st.success(f"💰 Revenue Proyectado (Próx. Año): **US$ {pred['revenue_proyectado'].sum()/1e6:.1f} Millones**")

                # PESTAÑA 4: INGENIERÍA + ESG
                with tab4:
                    st.header("⚙️ Ingeniería & Sustentabilidad (ESG)")
                    
                    st.subheader("🔥 Intensidad de Venteo (Gas Flaring)")
                    
                    df_venteo = get_venteo_data()
                    
                    if not df_venteo.empty:
                        col_esg1, col_esg2 = st.columns([1, 3])
                        with col_esg1:
                            avg_venteo = df_venteo['ratio_venteo'].mean()
                            st.metric("Promedio Vaca Muerta", f"{avg_venteo:.1f}%", delta="Objetivo < 1%", delta_color="inverse")
                        with col_esg2:
                            fig_venteo = px.bar(df_venteo, x='empresa', y='ratio_venteo',
                                              title="Ranking de Intensidad de Venteo (%)",
                                              color='ratio_venteo', color_continuous_scale='RdYlGn_r')
                            st.plotly_chart(fig_venteo, use_container_width=True)
                    else:
                        st.info("Datos de venteo no disponibles.")

                    st.divider()

                    df_ing = pd.DataFrame()
                    for emp in empresas_sel:
                        df_temp = get_eficiencia_empresa(emp)
                        if not df_temp.empty:
                            df_ing = pd.concat([df_ing, df_temp])
                    
                    if not df_ing.empty:
                        col_agua, col_gor = st.columns(2)
                        with col_agua:
                            st.markdown("##### 💧 Gestión de Agua")
                            fig_agua = px.line(df_ing, x='fecha', y='agua_m3', color='empresa', markers=True,
                                             color_discrete_map=COMPANY_COLORS)
                            st.plotly_chart(fig_agua, use_container_width=True)
                        with col_gor:
                            st.markdown("##### ⛽ Gas-Oil Ratio (GOR)")
                            fig_gor = px.line(df_ing, x='fecha', y='gor_promedio', color='empresa',
                                            color_discrete_map=COMPANY_COLORS)
                            st.plotly_chart(fig_gor, use_container_width=True)

                # PESTAÑA 5: BENCHMARKING
                with tab5:
                    st.header("Benchmarking Operativo")
                    
                    st.markdown("### 🚜 DUC Inventory (Drilled but Uncompleted)")
                    st.caption("Pozos perforados desde 2023 que aún no han entrado en producción.")
                    
                    df_ducs = get_ducs_data()
                    
                    if not df_ducs.empty:
                        col_kpi, col_chart = st.columns([1, 3])
                        with col_kpi:
                            total_ducs = df_ducs['ducs'].sum()
                            st.metric("Total DUCs", f"{total_ducs}", delta="Stock Disponible")
                            st.info("💡 Un nivel alto indica alta demanda de fractura.")
                        with col_chart:
                            fig_duc = px.bar(df_ducs, x='empresa', y='ducs', text='ducs', 
                                             title="Stock de DUCs por Operadora",
                                             color='ducs', color_continuous_scale='Oranges')
                            fig_duc.update_traces(textposition='outside')
                            st.plotly_chart(fig_duc, use_container_width=True)
                    else:
                        st.warning("⚠️ No se encontraron datos de DUCs.")
                    
                    st.divider()
                    
                    st.markdown("### 📉 Curvas Tipo (Type Curves)")
                    st.caption("Comparativa de eficiencia inicial (Normalizado Mes 0).")
                    
                    df_curves = pd.DataFrame()
                    for emp in empresas_sel:
                        df_temp = get_curva_tipo(emp)
                        if not df_temp.empty:
                            df_temp['empresa'] = emp
                            df_curves = pd.concat([df_curves, df_temp])
                    
                    if not df_curves.empty:
                        fig_type = px.line(df_curves, x='mes_n', y='promedio_petroleo', color='empresa',
                                         title="Rendimiento Promedio por Pozo", markers=True,
                                         color_discrete_map=COMPANY_COLORS)
                        st.plotly_chart(fig_type, use_container_width=True)
                    else:
                        st.info("Selecciona más operadoras para comparar curvas.")
        else:
            st.info("👈 Selecciona una o más operadoras en el panel lateral.")
    else:
        st.error("Error de conexión con el Servidor. Revisa el estado de Render.")

elif st.session_state["authentication_status"] is False:
    st.error('❌ Usuario o contraseña incorrectos')
    
elif st.session_state["authentication_status"] is None:
    # MENSAJE DE VENTA PARA VISITANTES
    st.warning('🔒 Por favor, ingresa tus credenciales para acceder al Dashboard.')
    
    st.markdown("---")
    col_promo, col_contact = st.columns(2)
    
    with col_promo:
        st.info("**¿No tenés cuenta?**")
        st.markdown("""
        Esta es una plataforma privada de inteligencia de mercado para Vaca Muerta.
        
        **Incluye:**
        * 📊 Producción en tiempo real.
        * 💰 Modelado Financiero.
        * 📉 Curvas Tipo y Benchmarking.
        """)
        
    with col_contact:
        st.success("🚀 **Solicitar Demo**")
        st.markdown("""
        Escríbenos para obtener un usuario de prueba por 7 días.
        
        📧 **Ventas:** lezcanojose7@gmail.com
        📞 **Tel:** 3794631300
        """)