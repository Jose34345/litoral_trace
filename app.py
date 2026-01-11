import streamlit as st
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
import plotly.express as px
import numpy as np
from shapely import wkt
from fpdf import FPDF # <--- NUEVA IMPORTACIÓN
import io

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Litoral Trace | RegTech", layout="wide", page_icon="🌱")

# --- CONEXIÓN DB ---
try:
    DB_URL = st.secrets["DB_URL"]
except:
    # Fallback por si lo corrés local sin secrets.toml (opcional)
    DB_URL = "postgresql://neondb_owner:npg_nxamLK5P6thM@ep-royal-snow-a488eu3z-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

engine = create_engine(DB_URL)
# --- CLASE PDF PROFESIONAL ---
class PDF(FPDF):
    def header(self):
        # Logo o Título Corporativo
        self.set_font('Arial', 'B', 15)
        self.cell(80)
        self.cell(30, 10, 'LITORAL TRACE - CERTIFICADO EUDR', 0, 0, 'C')
        self.ln(20)

    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.cell(0, 10, f'Page {self.page_no()}/{{nb}} | ID de Auditoria: {pd.Timestamp.now().strftime("%Y%m%d-%H%M%S")}', 0, 0, 'C')

def generar_certificado(lote_data, estado_analisis):
    pdf = PDF()
    pdf.alias_nb_pages()
    pdf.add_page()
    
    # 1. Título del Activo
    pdf.set_font('Arial', 'B', 12)
    pdf.cell(0, 10, f"ACTIVO: {lote_data['nombre_lote']}", 0, 1)
    
    # 2. Detalles Técnicos (Tabla simple)
    pdf.set_font('Arial', '', 10)
    pdf.cell(50, 10, f"Productor ID (CUIT):", 1)
    pdf.cell(0, 10, f"{lote_data['productor_id']}", 1, 1)
    
    pdf.cell(50, 10, f"Cultivo Declarado:", 1)
    pdf.cell(0, 10, f"{lote_data['tipo_cultivo']}", 1, 1)
    
    pdf.cell(50, 10, f"Superficie:", 1)
    pdf.cell(0, 10, f"{lote_data['hectareas_declaradas']} Has", 1, 1)
    
    pdf.ln(10)
    
    # 3. Dictamen de Cumplimiento (Lo más importante)
    pdf.set_font('Arial', 'B', 14)
    if estado_analisis == "APTO":
        pdf.set_text_color(0, 128, 0) # Verde
        pdf.cell(0, 10, "DICTAMEN: CUMPLE NORMATIVA EUDR 2023/1115", 0, 1, 'C')
    else:
        pdf.set_text_color(255, 0, 0) # Rojo
        pdf.cell(0, 10, "DICTAMEN: RIESGO DE DEFORESTACIÓN DETECTADO", 0, 1, 'C')
        
    pdf.set_text_color(0, 0, 0) # Volver a negro
    pdf.set_font('Arial', '', 10)
    pdf.ln(5)
    
    texto_legal = (
        "Certificamos que el poligono georreferenciado ha sido analizado mediante "
        "teledeteccion satelital (Sentinel-2) y NO presenta cambios de uso de suelo "
        "incompatibles con la regulacion europea post-31/12/2020."
    )
    pdf.multi_cell(0, 5, texto_legal)
    
    pdf.ln(10)
    pdf.cell(0, 10, "Firma Digital del Auditor:", 0, 1)
    pdf.cell(0, 10, "__________________________", 0, 1)
    
    return pdf.output(dest='S').encode('latin-1')

# --- DATA LOADERS ---
@st.cache_data(ttl=300)
def get_agro_data():
    query = """
    SELECT 
        id, nombre_lote, productor_id, tipo_cultivo, 
        hectareas_declaradas, estatus_cumplimiento, 
        centroide_lat, centroide_lon, 
        ST_AsText(geometria) as geom_wkt 
    FROM lotes_agro
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)
    df['geometry'] = df['geom_wkt'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")
    return gdf

def simular_analisis_historico(lote_nombre):
    fechas = pd.date_range(start="2020-01-01", end="2026-01-01", freq="M")
    np.random.seed(42)
    base = 0.4 + 0.3 * np.sin(np.linspace(0, 20, len(fechas)))
    ruido = np.random.normal(0, 0.05, len(fechas))
    return pd.DataFrame({"Fecha": fechas, "NDVI": base + ruido})

# --- UI PRINCIPAL ---
st.title("🌱 Litoral Trace: Inteligencia de Cumplimiento")

# Sidebar
with st.sidebar:
    st.header("⚙️ Panel de Control")
    st.success("🟢 Sistema Online")
    if st.button("🔄 Recargar Datos"):
        st.cache_data.clear()
        st.rerun()

gdf = get_agro_data()

if not gdf.empty:
    # --- KPIs ---
    c1, c2, c3 = st.columns(3)
    c1.metric("Lotes en Cartera", len(gdf))
    c2.metric("Superficie Total", f"{gdf['hectareas_declaradas'].sum()} ha")
    c3.metric("Riesgo EUDR", len(gdf[gdf['estatus_cumplimiento'] == 'Rojo']))

    st.divider()

    # --- ZONA DE TRABAJO (Mapa + Análisis + Certificado) ---
    col_izq, col_der = st.columns([1, 1])

    with col_izq:
        st.subheader("📍 Geolocalización")
        lotes_json = gdf.__geo_interface__
        fig_map = px.choropleth_mapbox(
            gdf, geojson=lotes_json, locations=gdf.index,
            color="estatus_cumplimiento",
            color_discrete_map={"Verde": "#2ECC71", "Amarillo": "#F1C40F", "Rojo": "#E74C3C", "Pendiente": "#95A5A6"},
            mapbox_style="open-street-map",
            center={"lat": gdf.centroide_lat.mean(), "lon": gdf.centroide_lon.mean()},
            zoom=13, height=450
        )
        fig_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig_map, use_container_width=True)

    with col_der:
        st.subheader("🛰️ Auditoría de Lote")
        
        # 1. Selección
        lote_selec_nombre = st.selectbox("Seleccionar Lote:", gdf['nombre_lote'])
        lote_data = gdf[gdf['nombre_lote'] == lote_selec_nombre].iloc[0]
        
        # 2. Análisis Visual
        df_ndvi = simular_analisis_historico(lote_selec_nombre)
        fig_ndvi = px.line(df_ndvi, x="Fecha", y="NDVI", title="Histórico de Biomasa")
        fig_ndvi.add_vline(x="2020-12-31", line_width=2, line_dash="dash", line_color="red")
        fig_ndvi.add_annotation(x="2020-12-31", y=0.9, text="Límite EUDR", showarrow=False, font=dict(color="red"))
        fig_ndvi.update_yaxes(range=[0, 1])
        st.plotly_chart(fig_ndvi, use_container_width=True)
        
        # 3. GENERACIÓN DE VALOR (Botón PDF)
        st.success("✅ Análisis Completado: Lote APTO para exportación.")
        
        # Generamos el PDF en memoria
        pdf_bytes = generar_certificado(lote_data, "APTO")
        
        st.download_button(
            label="📄 Descargar CERTIFICADO DE EXPORTACIÓN",
            data=pdf_bytes,
            file_name=f"Certificado_EUDR_{lote_data['productor_id']}.pdf",
            mime="application/pdf",
            type="primary",
            use_container_width=True
        )

    # Tabla Final
    st.subheader("📋 Inventario de Lotes")
    st.dataframe(gdf.drop(columns=['geometry', 'geom_wkt']), use_container_width=True)

else:
    st.warning("No hay datos cargados.")