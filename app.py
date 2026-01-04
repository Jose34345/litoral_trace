import streamlit as st
import pandas as pd
import plotly.express as px
import requests
# Mantenemos la función de IA local
from ml_forecasting import predecir_produccion 

# --- CONFIGURACIÓN ESTÉTICA ---
st.set_page_config(
    page_title="Vaca Muerta Intelligence", 
    page_icon="⚡", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# ESTILOS CSS PERSONALIZADOS
st.markdown("""
<style>
    /* Estilo para las métricas (KPIs) */
    [data-testid="stMetric"] {
        background-color: #1E1E1E;
        border-radius: 10px;
        padding: 15px;
        border: 1px solid #333;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.3);
    }
    /* Título del Sidebar */
    [data-testid="stSidebar"] h1 {
        font-size: 20px;
        color: #00BFFF;
    }
</style>
""", unsafe_allow_html=True)

# URL DE LA API
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
    
    # 1. TOP PLAYERS (BOTONES/PILLS)
    # Definimos las "Majors" para acceso rápido
    top_majors = ["YPF", "VISTA", "PAN AMERICAN ENERGY", "SHELL", "PLUSPETROL", "TECPETROL"]
    # Filtramos para asegurarnos que existen en la lista real
    majors_avail = [e for e in top_majors if e in lista_empresas]
    
    # Usamos PILLS: Botones modernos de selección múltiple
    selected_majors = st.sidebar.pills(
        "Principales Operadoras", 
        majors_avail, 
        selection_mode="multi", 
        default=["YPF"] if "YPF" in majors_avail else None
    )
    
    # 2. DROPDOWN (RESTO DE EMPRESAS)
    st.sidebar.markdown("---")
    st.sidebar.caption("Otras Operadoras")
    # Excluimos las que ya están en los botones de arriba para no duplicar
    other_options = [e for e in lista_empresas if e not in majors_avail]
    selected_others = st.sidebar.multiselect("Buscar en el listado completo:", other_options)
    
    # 3. UNIFICAR SELECCIÓN
    # Si selected_majors es None (nadie clickeado), lo convertimos a lista vacía
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
            
            # --- KPIs (Tarjetas Estilizadas) ---
            # Usamos columnas con espacio (gap) para que respire el diseño
            c1, c2, c3 = st.columns(3, gap="medium")
            
            total_rev = df_view['revenue_usd'].sum()
            
            # Los números grandes se leen mejor
            c1.metric("🛢️ Petróleo Total", f"{df_view['petroleo'].sum()/1e6:,.1f} M m³", delta_color="normal")
            c2.metric("🔥 Gas Total", f"{df_view['gas'].sum()/1e6:,.1f} B m³", delta_color="normal")
            c3.metric("💵 Facturación (Est.)", f"US$ {total_rev/1_000_000:,.1f} M", delta="YTD 2024")
            
            st.markdown(" ") # Espacio vacío

            # --- PESTAÑAS ---
            tab1, tab2, tab3, tab4, tab5 = st.tabs([
                "📊 Producción", "💰 Finanzas", "🔮 IA Predictiva", "⚙️ Ingeniería", "📉 Benchmarking"
            ])
            
            with tab1:
                st.subheader("Curva de Producción de Petróleo")
                fig = px.area(df_view, x='fecha', y='petroleo', color='empresa', 
                              color_discrete_sequence=px.colors.qualitative.Vivid)
                st.plotly_chart(fig, use_container_width=True)
                
            with tab2:
                col_rank, col_pie = st.columns([2,1])
                with col_rank:
                    st.subheader("Ranking por Ingresos")
                    df_rank = df_view.groupby('empresa')['revenue_usd'].sum().reset_index().sort_values('revenue_usd', ascending=True)
                    fig_bar = px.bar(df_rank, x='revenue_usd', y='empresa', orientation='h', text_auto='.2s')
                    fig_bar.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
                    st.plotly_chart(fig_bar, use_container_width=True)
                with col_pie:
                    st.subheader("Market Share")
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
                            # Zona de predicción sombreada
                            st.plotly_chart(fig_ia, use_container_width=True)
                            
                            st.success(f"💰 Revenue Proyectado (Próx. Año): **US$ {pred['revenue_proyectado'].sum()/1e6:.1f} Millones**")

            with tab4:
                # Buscamos los datos de eficiencia
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
                st.subheader("Curvas Tipo (Type Curves)")
                st.caption("Comparativa de eficiencia inicial de pozos (Normalizado al Mes 0)")
                
                df_curves = pd.DataFrame()
                for emp in empresas_sel:
                    df_temp = get_curva_tipo(emp)
                    if not df_temp.empty:
                        df_temp['empresa'] = emp
                        df_curves = pd.concat([df_curves, df_temp])
                
                if not df_curves.empty:
                    fig_type = px.line(df_curves, x='mes_n', y='promedio_petroleo', color='empresa',
                                     title="Rendimiento Promedio por Pozo", markers=True)
                    fig_type.update_layout(xaxis_title="Meses desde inicio perforación", yaxis_title="Producción (m³)")
                    st.plotly_chart(fig_type, use_container_width=True)

    else:
        st.info("👈 Selecciona una o más operadoras en el panel lateral para comenzar.")
else:
    st.error("Error de conexión con el Servidor. Revisa el estado de Render.")