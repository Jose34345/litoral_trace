import streamlit as st
import pandas as pd
import plotly.express as px
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
</style>
""", unsafe_allow_html=True)

# --- SISTEMA DE AUTENTICACIÓN ---
# 1. Cargar configuración de usuarios
with open('config.yaml') as file:
    config = yaml.load(file, Loader=SafeLoader)

# 2. Crear objeto autenticador
authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days']
)

# 3. Renderizar widget de Login
authenticator.login()

# --- LÓGICA DE ACCESO ---
if st.session_state["authentication_status"]:
    # 🔓 SI EL USUARIO ENTRÓ CORRECTAMENTE:
    
    # Botón de Logout en el sidebar
    with st.sidebar:
        st.write(f"Hola, *{st.session_state['name']}* 👋")
        authenticator.logout('Cerrar Sesión', 'sidebar')
        st.divider()

    # --- INICIO DE LA APP (Toda la lógica va indentada aquí) ---
    API_URL = "https://vaca-muerta-intel.onrender.com"

    # --- FUNCIONES DE CONEXIÓN ---

    def get_ducs_data():
        try:
            response = requests.get(f"{API_URL}/ducs")
            if response.status_code == 200:
                return pd.DataFrame(response.json())
            return pd.DataFrame()
        except:
            return pd.DataFrame()

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
        """
        Convierte un DataFrame a CSV optimizado para Excel (utf-8).
        """
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

    # --- HEADER PRINCIPAL ---
    st.title("⚡ Vaca Muerta Intelligence 2.0")
    st.markdown("Plataforma de análisis estratégico en tiempo real. **Backend:** Online 🟢")
    st.markdown("---")

    # --- SIDEBAR MEJORADO (UX) ---
    st.sidebar.image("https://cdn-icons-png.flaticon.com/512/2103/2103633.png", width=50) # Icono de pozo
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
                    "📊 Producción", "💰 Finanzas", "🔮 IA Predictiva", "⚙️ Ingeniería", "📉 Benchmarking"
                ])
                
                # PESTAÑA 1: PRODUCCIÓN + DESCARGA
                with tab1:
                    col_title, col_download = st.columns([4, 1])
                    with col_title:
                        st.subheader("Curva de Producción de Petróleo")
                    with col_download:
                        # 📥 BOTÓN DE DESCARGA
                        csv = convert_df(df_view)
                        st.download_button(
                            label="📥 Descargar Data",
                            data=csv,
                            file_name=f'produccion_vaca_muerta_{pd.Timestamp.now().date()}.csv',
                            mime='text/csv',
                            key='download-prod'
                        )
                    
                    fig = px.area(df_view, x='fecha', y='petroleo', color='empresa', 
                                  color_discrete_sequence=px.colors.qualitative.Vivid)
                    st.plotly_chart(fig, use_container_width=True)
                
                # PESTAÑA 2: FINANZAS + DESCARGA
                with tab2:
                    st.subheader("Ranking Financiero")
                    col_rank, col_pie = st.columns([2,1])
                    with col_rank:
                        df_rank = df_view.groupby('empresa')['revenue_usd'].sum().reset_index().sort_values('revenue_usd', ascending=True)
                        fig_bar = px.bar(df_rank, x='revenue_usd', y='empresa', orientation='h', text_auto='.2s')
                        fig_bar.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
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
                        fig_pie = px.pie(df_rank, values='revenue_usd', names='empresa', hole=0.4)
                        st.plotly_chart(fig_pie, use_container_width=True)

                with tab3:
                    st.subheader("Simulación de Escenarios Futuros")
                    c_sim1, c_sim2 = st.columns([1, 3])
                    with c_sim1:
                        empresa_pred = st.selectbox("Seleccionar Operadora para IA:", empresas_sel)
                        precio_futuro = st.slider("Precio Brent Futuro (US$):", 40, 100, 75)
                    
                    with c_sim2:
                        if empresa_pred:
                            df_ia = df_view[df_view['empresa'] == empresa_pred].copy()
                            df_ia = df_ia.rename(columns={'petroleo': 'prod_pet'}).sort_values('fecha')
                            
                            if len(df_ia) > 6:
                                pred = predecir_produccion(df_ia)
                                pred['revenue_proyectado'] = pred['prod_pet_pred'] * precio_futuro
                                
                                fig_ia = px.line(pred, x='fecha', y='prod_pet_pred', title=f"Proyección 12 Meses: {empresa_pred}")
                                fig_ia.add_vline(x=pd.Timestamp.now().timestamp()*1000, line_dash="dash", annotation_text="Hoy")
                                st.plotly_chart(fig_ia, use_container_width=True)
                                
                                st.success(f"💰 Revenue Proyectado (Próx. Año): **US$ {pred['revenue_proyectado'].sum()/1e6:.1f} Millones**")

                with tab4:
                    df_ing = pd.DataFrame()
                    for emp in empresas_sel:
                        df_temp = get_eficiencia_empresa(emp)
                        if not df_temp.empty:
                            df_ing = pd.concat([df_ing, df_temp])
                    
                    if not df_ing.empty:
                        col_agua, col_gor = st.columns(2)
                        with col_agua:
                            st.markdown("##### 💧 Gestión de Agua")
                            fig_agua = px.line(df_ing, x='fecha', y='agua_m3', color='empresa', markers=True)
                            st.plotly_chart(fig_agua, use_container_width=True)
                        with col_gor:
                            st.markdown("##### ⛽ Gas-Oil Ratio (GOR)")
                            fig_gor = px.line(df_ing, x='fecha', y='gor_promedio', color='empresa')
                            st.plotly_chart(fig_gor, use_container_width=True)

                with tab5:
                    st.header("Benchmarking Operativo")
                    
                    # --- SECCIÓN 1: DUC INVENTORY (NUEVO) ---
                    st.markdown("### 🚜 DUC Inventory (Drilled but Uncompleted)")
                    st.caption("Pozos perforados desde 2023 que aún no han entrado en producción. Indica stock de trabajo futuro.")
                    
                    # Llamamos a la función que conecta con el endpoint /ducs
                    df_ducs = get_ducs_data()
                    
                    if not df_ducs.empty:
                        col_kpi, col_chart = st.columns([1, 3])
                        
                        with col_kpi:
                            # KPI destacado a la izquierda
                            total_ducs = df_ducs['ducs'].sum()
                            st.metric("Total DUCs (Vaca Muerta)", f"{total_ducs}", delta="Stock Disponible")
                            st.info("💡 Un nivel alto de DUCs suele indicar alta demanda de sets de fractura (Cuello de botella).")
                        
                        with col_chart:
                            # Gráfico de barras a la derecha
                            fig_duc = px.bar(df_ducs, x='empresa', y='ducs', text='ducs', 
                                             title="Stock de DUCs por Operadora (Top 10)",
                                             color='ducs', color_continuous_scale='Oranges')
                            fig_duc.update_traces(textposition='outside')
                            fig_duc.update_layout(xaxis_title="Operadora", yaxis_title="Cantidad de Pozos")
                            st.plotly_chart(fig_duc, use_container_width=True)
                    else:
                        st.warning("⚠️ No se encontraron datos de DUCs. Asegurate de haber corrido el ETL actualizado (tabla 'padron').")
                    
                    st.divider() # Línea separadora visual
                    
                    # --- SECCIÓN 2: CURVAS TIPO (EXISTENTE) ---
                    st.markdown("### 📉 Curvas Tipo (Type Curves)")
                    st.caption("Comparativa de eficiencia inicial de pozos (Normalizado al Mes 0).")
                    
                    df_curves = pd.DataFrame()
                    # Iteramos sobre las empresas seleccionadas en el sidebar
                    for emp in empresas_sel:
                        df_temp = get_curva_tipo(emp)
                        if not df_temp.empty:
                            df_temp['empresa'] = emp
                            df_curves = pd.concat([df_curves, df_temp])
                    
                    if not df_curves.empty:
                        fig_type = px.line(df_curves, x='mes_n', y='promedio_petroleo', color='empresa',
                                         title="Rendimiento Promedio por Pozo (Primeros 24 Meses)", markers=True)
                        fig_type.update_layout(xaxis_title="Meses desde inicio perforación", yaxis_title="Producción Promedio (m³)")
                        st.plotly_chart(fig_type, use_container_width=True)
                    else:
                        st.info("Selecciona más operadoras para comparar sus curvas de declino.")
        else:
            st.info("👈 Selecciona una o más operadoras en el panel lateral para comenzar.")
    else:
        st.error("Error de conexión con el Servidor. Revisa el estado de Render.")

# ... (Todo el código anterior sigue igual) ...

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
        *  Producción en tiempo real.
        *  Modelado Financiero.
        *  Curvas Tipo y Benchmarking.
        """)
        
    with col_contact:
        st.success(" **Solicitar Demo**")
        st.markdown("""
        Escríbenos para obtener un usuario de prueba por 7 días.
        
        📧 **Ventas:** lezcanojose7@gmail.com
        📞 **Tel:** 3794631300
        """)