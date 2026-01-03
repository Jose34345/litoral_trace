import streamlit as st
import pandas as pd
import plotly.express as px
import requests
# Mantenemos la función de IA local por ahora (idealmente también debería ir a la API)
from ml_forecasting import predecir_produccion 

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Vaca Muerta Intelligence", page_icon="🛢️", layout="wide")
API_URL = "https://vaca-muerta-intel.onrender.com"

# --- FUNCIONES DE CONEXIÓN A LA API ---
@st.cache_data(ttl=300)
def get_lista_empresas():
    """Llama a la API para pedir la lista de empresas."""
    try:
        response = requests.get(f"{API_URL}/empresas")
        if response.status_code == 200:
            return response.json()['data']
        else:
            st.error("Error conectando con el cerebro (API).")
            return []
    except Exception as e:
        st.error(f"API caída: {e}")
        return []

def get_data_empresa(empresa):
    """Llama a la API para pedir los datos de UNA empresa."""
    try:
        response = requests.get(f"{API_URL}/produccion/{empresa}")
        if response.status_code == 200:
            data = response.json()
            if data:
                df = pd.DataFrame(data)
                df['fecha'] = pd.to_datetime(df['fecha_data']) # Convertimos texto a fecha real
                df['empresa'] = empresa # Agregamos columna para identificar
                return df
        return pd.DataFrame()
    except:
        return pd.DataFrame()
def get_eficiencia_empresa(empresa):
    """Consulta el endpoint de eficiencia operativa."""
    try:
        response = requests.get(f"{API_URL}/eficiencia/{empresa}")
        if response.status_code == 200:
            data = response.json()
            if data:
                df = pd.DataFrame(data)
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

# --- INTERFAZ ---
st.title("🛢️ Vaca Muerta Intelligence 2.0")
st.caption(f"Architecture: Microservices | Backend: {API_URL}")

# 1. CARGAMOS EMPRESAS DESDE LA API
lista_empresas = get_lista_empresas()

if lista_empresas:
    # FILTROS
    st.sidebar.header("Panel de Control")
    # Seleccionamos YPF por defecto si existe
    default_idx = lista_empresas.index("YPF") if "YPF" in lista_empresas else 0
    empresas_sel = st.sidebar.multiselect("Operadoras", lista_empresas, default=[lista_empresas[default_idx]])
    
    if empresas_sel:
        # 2. CARGAMOS DATOS BAJO DEMANDA (Lazy Loading)
        # En lugar de traer todo, traemos solo lo que el usuario pide. Eso es escalabilidad.
        all_data = []
        for emp in empresas_sel:
            df_temp = get_data_empresa(emp)
            if not df_temp.empty:
                all_data.append(df_temp)
        
        if all_data:
            df_view = pd.concat(all_data)
            
            # --- KPIs ---
            c1, c2, c3 = st.columns(3)
            total_rev = df_view['revenue_usd'].sum()
            c1.metric("Petróleo Total (m³)", f"{df_view['petroleo'].sum():,.0f}")
            c2.metric("Gas Total (Mm³)", f"{df_view['gas'].sum():,.0f}")
            c3.metric("Facturación Estimada", f"US$ {total_rev/1_000_000:,.1f} M")
            
            st.markdown("---")
            
            tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Producción", "💰 Finanzas", "🔮 Predicción IA", "⚙️ Ingeniería", "📉 Benchmarking"])            
            
            with tab1:
                fig = px.line(df_view, x='fecha', y='petroleo', color='empresa', title="Curva de Producción")
                st.plotly_chart(fig, use_container_width=True)
                
            with tab2:
                # Ranking dinámico
                df_rank = df_view.groupby('empresa')['revenue_usd'].sum().reset_index().sort_values('revenue_usd')
                fig_bar = px.bar(df_rank, x='revenue_usd', y='empresa', orientation='h', title="Ranking Financiero (Selección)")
                st.plotly_chart(fig_bar, use_container_width=True)

            with tab3:
                st.subheader("Simulación de Escenarios")
                empresa_pred = st.selectbox("Simular para:", empresas_sel)
                
                # Slider de precio
                precio_futuro = st.slider("Precio Futuro (US$):", 40, 100, 70)
                
                if empresa_pred:
                    df_ia = df_view[df_view['empresa'] == empresa_pred].copy()
                    # Preparamos dataframe para la IA (necesita columna 'prod_pet')
                    df_ia = df_ia.rename(columns={'petroleo': 'prod_pet'}).sort_values('fecha')
                    
                    if len(df_ia) > 6:
                        pred = predecir_produccion(df_ia)
                        
                        # Calculamos revenue futuro
                        pred['revenue_proyectado'] = pred['prod_pet_pred'] * precio_futuro
                        
                        # Gráfico combinado
                        fig_ia = px.line(pred, x='fecha', y='prod_pet_pred', title="Proyección Volumen 2026")
                        fig_ia.add_vline(x=pd.Timestamp.now().timestamp()*1000, line_dash="dash", annotation_text="Hoy")
                        st.plotly_chart(fig_ia, use_container_width=True)
                        
                        st.success(f"Facturación proyectada (12 meses): US$ {pred['revenue_proyectado'].sum()/1e6:.1f} M")
            with tab4:
                st.subheader("Eficiencia Operativa & Manejo de Fluidos")
                
                # Buscamos los datos de eficiencia
                df_ing = pd.DataFrame()
                for emp in empresas_sel:
                    df_temp = get_eficiencia_empresa(emp)
                    if not df_temp.empty:
                        df_ing = pd.concat([df_ing, df_temp])
                
                if not df_ing.empty:
                    col_agua, col_gor = st.columns(2)
                    
                    with col_agua:
                        st.markdown("##### 💧 Gestión de Agua (Water Cut)")
                        fig_agua = px.area(df_ing, x='fecha', y='agua_m3', color='empresa', 
                                         title="Producción de Agua (m³)",
                                         color_discrete_sequence=px.colors.sequential.Blues)
                        st.plotly_chart(fig_agua, use_container_width=True)
                        st.info("💡 Insight: Altos volúmenes de agua implican mayores costos operativos (tratamiento y reinyección).")
                        
                    with col_gor:
                        st.markdown("##### ⛽ Relación Gas-Petróleo (GOR)")
                        fig_gor = px.line(df_ing, x='fecha', y='gor_promedio', color='empresa',
                                        title="Evolución del GOR (m³ gas / m³ oil)")
                        st.plotly_chart(fig_gor, use_container_width=True)
                        st.info("💡 Insight: Un aumento rápido del GOR puede indicar despresurización del pozo.")
            with tab5:
                st.subheader("Curvas Tipo (Eficiencia de Pozos)")
                st.markdown("Comparativa de rendimiento promedio por pozo en sus primeros 24 meses de vida.")
                
                df_curves = pd.DataFrame()
                for emp in empresas_sel:
                    df_temp = get_curva_tipo(emp)
                    if not df_temp.empty:
                        df_temp['empresa'] = emp
                        df_curves = pd.concat([df_curves, df_temp])
                
                if not df_curves.empty:
                    fig_type = px.line(df_curves, x='mes_n', y='promedio_petroleo', color='empresa',
                                     title="Curva de Declino Promedio (Type Curve)",
                                     labels={'mes_n': 'Meses desde inicio de perforación', 'promedio_petroleo': 'Producción Promedio (m³)'})
                    st.plotly_chart(fig_type, use_container_width=True)
                    st.info("🧠 Análisis: Quien tenga la curva más alta al principio tiene mejores técnicas de fractura (Fracking). Quien tenga la curva más plana, tiene mejor mantenimiento de presión.")
    else:
        st.info("Selecciona al menos una operadora para conectar al servidor.")
else:
    st.error("No se pudo obtener la lista de empresas. Verifica que 'api/main.py' esté corriendo.")



# Forzando actualizacion de la app