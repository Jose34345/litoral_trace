import streamlit as st
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
import plotly.express as px
import numpy as np
from shapely import wkt
from fpdf import FPDF
import io

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Litoral Trace | RegTech", layout="wide", page_icon="🌱")

# --- CONEXIÓN DB (Segura para Nube y Local) ---
try:
    DB_URL = st.secrets["DB_URL"]
except:
    # Fallback para local si no hay secrets
    DB_URL = "postgresql://neondb_owner:npg_nxamLK5P6thM@ep-royal-snow-a488eu3z-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require"

engine = create_engine(DB_URL)

# --- CLASE PDF ---
class PDF(FPDF):
    def header(self):
        self.set_font('Arial', 'B', 15)
        self.cell(80)
        self.cell(30, 10, 'LITORAL TRACE - CERTIFICADO EUDR', 0, 0, 'C')
        self.ln(20)

    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.cell(0, 10, f'Page {self.page_no()}/{{nb}} | ID Auditoria: {pd.Timestamp.now().strftime("%Y%m%d")}', 0, 0, 'C')

def generar_certificado(lote_data, estado_analisis):
    pdf = PDF()
    pdf.alias_nb_pages()
    pdf.add_page()
    pdf.set_font('Arial', 'B', 12)
    pdf.cell(0, 10, f"ACTIVO: {lote_data['nombre_lote']}", 0, 1)
    pdf.set_font('Arial', '', 10)
    pdf.cell(50, 10, f"Productor ID:", 1)
    pdf.cell(0, 10, f"{lote_data['productor_id']}", 1, 1)
    pdf.cell(50, 10, f"Cultivo:", 1)
    pdf.cell(0, 10, f"{lote_data['tipo_cultivo']}", 1, 1)
    pdf.ln(10)
    
    if estado_analisis == "APTO":
        pdf.set_text_color(0, 128, 0)
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(0, 10, "DICTAMEN: CUMPLE NORMATIVA EUDR", 0, 1, 'C')
    
    pdf.set_text_color(0)
    pdf.set_font('Arial', '', 10)
    pdf.ln(5)
    pdf.multi_cell(0, 5, "Certificacion digital generada via Litoral Trace Satellite Engine.")
    return pdf.output(dest='S').encode('latin-1')

# --- DATA LOADERS & ACCIONES DB ---
@st.cache_data(ttl=60) # Bajamos el cache a 60 seg para ver los cambios rapido
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
    
    if not df.empty:
        df['geometry'] = df['geom_wkt'].apply(wkt.loads)
        gdf = gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")
        return gdf
    return gpd.GeoDataFrame()

def guardar_nuevo_lote(nombre, prod_id, cultivo, has, lat, lon):
    """
    Crea un polígono cuadrado alrededor del punto Lat/Lon y lo guarda en NeonDB.
    """
    # Creamos un cuadrado simple (aprox 500m x 500m alrededor del punto)
    delta = 0.004 # Grados aprox para crear visualización
    wkt_poly = f"POLYGON(({lon-delta} {lat-delta}, {lon+delta} {lat-delta}, {lon+delta} {lat+delta}, {lon-delta} {lat+delta}, {lon-delta} {lat-delta}))"
    
    insert_query = text("""
        INSERT INTO lotes_agro 
        (nombre_lote, productor_id, tipo_cultivo, hectareas_declaradas, estatus_cumplimiento, centroide_lat, centroide_lon, geometria)
        VALUES 
        (:nombre, :prod, :cultivo, :has, 'Pendiente', :lat, :lon, ST_GeomFromText(:wkt, 4326))
    """)
    
    with engine.begin() as conn:
        conn.execute(insert_query, {
            "nombre": nombre, "prod": prod_id, "cultivo": cultivo, 
            "has": has, "lat": lat, "lon": lon, "wkt": wkt_poly
        })

def simular_analisis(lote_nombre):
    fechas = pd.date_range(start="2020-01-01", end="2026-01-01", freq="M")
    np.random.seed(len(lote_nombre)) # Seed basada en el nombre para que varíe por lote
    base = 0.4 + 0.3 * np.sin(np.linspace(0, 20, len(fechas)))
    ruido = np.random.normal(0, 0.05, len(fechas))
    return pd.DataFrame({"Fecha": fechas, "NDVI": base + ruido})

# --- INTERFAZ GRAFICA ---
st.title("🌱 Litoral Trace: Inteligencia de Cumplimiento")

# --- SIDEBAR: Carga de Datos ---
with st.sidebar:
    st.header("⚙️ Panel de Control")
    
    # SECCIÓN DE CARGA (NUEVA)
    with st.expander("➕ Cargar Nuevo Lote", expanded=False):
        st.write("Ingrese los datos del activo:")
        with st.form("form_alta_lote"):
            frm_nombre = st.text_input("Nombre del Lote")
            frm_cuit = st.text_input("CUIT / ID Productor")
            frm_cultivo = st.selectbox("Cultivo", ["Algodón", "Soja", "Maíz", "Girasol", "Forestal"])
            frm_has = st.number_input("Hectáreas", min_value=1, value=50)
            
            st.markdown("---")
            st.markdown("**Ubicación (Centro):**")
            col_lat, col_lon = st.columns(2)
            # Valores default en Chaco para facilitar demo
            frm_lat = col_lat.number_input("Latitud", value=-27.45, format="%.5f") 
            frm_lon = col_lon.number_input("Longitud", value=-59.05, format="%.5f")
            
            submitted = st.form_submit_button("💾 Guardar Lote en Base de Datos")
            
            if submitted:
                if frm_nombre and frm_cuit:
                    try:
                        guardar_nuevo_lote(frm_nombre, frm_cuit, frm_cultivo, frm_has, frm_lat, frm_lon)
                        st.success("¡Lote cargado exitosamente!")
                        st.cache_data.clear() # Limpiamos cache para ver el nuevo lote
                    except Exception as e:
                        st.error(f"Error al guardar: {e}")
                else:
                    st.warning("Complete Nombre y CUIT.")

    st.divider()
    if st.button("🔄 Refrescar Datos"):
        st.cache_data.clear()
        st.rerun()

# --- DASHBOARD PRINCIPAL ---
gdf = get_agro_data()

if not gdf.empty:
    c1, c2, c3 = st.columns(3)
    c1.metric("Lotes en Cartera", len(gdf))
    c2.metric("Superficie Total", f"{gdf['hectareas_declaradas'].sum()} ha")
    riesgo = len(gdf[gdf['estatus_cumplimiento'] == 'Rojo'])
    c3.metric("Riesgo EUDR", riesgo, delta_color="inverse")

    st.divider()

    col_map, col_analisis = st.columns([1, 1])

    with col_map:
        st.subheader("📍 Mapa de Activos")
        lotes_json = gdf.__geo_interface__
        fig_map = px.choropleth_mapbox(
            gdf, geojson=lotes_json, locations=gdf.index,
            color="estatus_cumplimiento",
            color_discrete_map={"Verde": "#2ECC71", "Amarillo": "#F1C40F", "Rojo": "#E74C3C", "Pendiente": "#95A5A6"},
            mapbox_style="open-street-map",
            center={"lat": gdf.centroide_lat.mean(), "lon": gdf.centroide_lon.mean()},
            zoom=10, height=500,
            hover_data=["nombre_lote", "tipo_cultivo"]
        )
        fig_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig_map, use_container_width=True)

    with col_analisis:
        st.subheader("🛰️ Auditoría Individual")
        lista_lotes = gdf['nombre_lote'].tolist()
        lote_selec = st.selectbox("Seleccionar Activo:", lista_lotes)
        
        # Datos del seleccionado
        row = gdf[gdf['nombre_lote'] == lote_selec].iloc[0]
        
        # Análisis
        df_ndvi = simular_analisis(lote_selec)
        fig_ndvi = px.line(df_ndvi, x="Fecha", y="NDVI", title=f"Evolución - {lote_selec}")
        fig_ndvi.add_vline(x="2020-12-31", line_dash="dash", line_color="red")
        fig_ndvi.add_annotation(x="2020-12-31", y=0.9, text="EUDR", showarrow=False, font=dict(color="red"))
        st.plotly_chart(fig_ndvi, use_container_width=True)
        
        # PDF
        st.success(f"Estado: {row['estatus_cumplimiento']}")
        pdf_bytes = generar_certificado(row, "APTO")
        st.download_button("📄 Descargar Certificado", data=pdf_bytes, file_name=f"Cert_{row['productor_id']}.pdf", mime="application/pdf", use_container_width=True)

    st.subheader("📋 Base de Datos")
    st.dataframe(gdf.drop(columns=['geometry', 'geom_wkt']), use_container_width=True)

else:
    st.info("👈 Utilizá el panel lateral para cargar tu primer lote.")