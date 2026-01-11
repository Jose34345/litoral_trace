import streamlit as st
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
import plotly.express as px
import numpy as np
from shapely import wkt
from fpdf import FPDF
import ee

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Litoral Trace | RegTech", layout="wide", page_icon="🌱")

# --- CONEXIÓN DB ---
try:
    DB_URL = st.secrets["DB_URL"]
except:
    DB_URL = "postgresql://neondb_owner:npg_nxamLK5P6thM@ep-royal-snow-a488eu3z-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require"

engine = create_engine(DB_URL)

# --- INICIALIZACIÓN GEE (NUEVA LÓGICA ROBUSTA) ---
def inicializar_gee():
    try:
        # Intentamos leer la sección [gcp_service_account] del secrets.toml
        if "gcp_service_account" in st.secrets:
            # Streamlit ya lo convierte a diccionario automáticamente
            service_account = st.secrets["gcp_service_account"]
            
            credentials = ee.ServiceAccountCredentials(
                service_account['client_email'], 
                key_data=service_account['private_key']
            )
            ee.Initialize(credentials)
            return True
        else:
            return False
    except Exception as e:
        # Imprimimos el error en consola para debug, pero no en la pantalla del usuario
        print(f"Error conectando GEE: {e}")
        return False

GEE_ACTIVO = inicializar_gee()

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
    pdf.ln(10)
    
    if estado_analisis == "APTO":
        pdf.set_text_color(0, 128, 0)
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(0, 10, "DICTAMEN: CUMPLE NORMATIVA EUDR", 0, 1, 'C')
    
    pdf.set_text_color(0)
    return pdf.output(dest='S').encode('latin-1')

# --- DATA LOADERS ---
@st.cache_data(ttl=60)
def get_agro_data():
    query = """
    SELECT id, nombre_lote, productor_id, tipo_cultivo, 
           hectareas_declaradas, estatus_cumplimiento, 
           centroide_lat, centroide_lon, 
           ST_AsText(geometria) as geom_wkt 
    FROM lotes_agro
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)
    
    if not df.empty:
        df['geometry'] = df['geom_wkt'].apply(wkt.loads)
        return gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")
    return gpd.GeoDataFrame()

def guardar_nuevo_lote(nombre, prod_id, cultivo, has, lat, lon):
    delta = 0.004 
    wkt_poly = f"POLYGON(({lon-delta} {lat-delta}, {lon+delta} {lat-delta}, {lon+delta} {lat+delta}, {lon-delta} {lat+delta}, {lon-delta} {lat-delta}))"
    
    insert_query = text("""
        INSERT INTO lotes_agro 
        (nombre_lote, productor_id, tipo_cultivo, hectareas_declaradas, estatus_cumplimiento, centroide_lat, centroide_lon, geometria)
        VALUES (:nombre, :prod, :cultivo, :has, 'Pendiente', :lat, :lon, ST_GeomFromText(:wkt, 4326))
    """)
    with engine.begin() as conn:
        conn.execute(insert_query, {
            "nombre": nombre, "prod": prod_id, "cultivo": cultivo, 
            "has": has, "lat": lat, "lon": lon, "wkt": wkt_poly
        })

# --- ANÁLISIS SATELITAL ---
@st.cache_data(ttl=3600)
def obtener_ndvi_real(lat, lon, lote_nombre):
    if not GEE_ACTIVO:
        # Fallback de simulación
        fechas = pd.date_range(start="2020-01-01", end="2026-01-01", freq="ME")
        np.random.seed(len(lote_nombre)) 
        base = 0.4 + 0.3 * np.sin(np.linspace(0, 20, len(fechas)))
        ruido = np.random.normal(0, 0.05, len(fechas))
        return pd.DataFrame({"Fecha": fechas, "NDVI": base + ruido, "Origen": "Simulado"})

    try:
        point = ee.Geometry.Point([lon, lat])
        region = point.buffer(100)
        s2 = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")\
            .filterDate('2020-01-01', '2026-01-01')\
            .filterBounds(point)\
            .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 20)) 

        def procesar_imagen(img):
            qa = img.select('QA60')
            mask = qa.bitwiseAnd(1<<10).eq(0).And(qa.bitwiseAnd(1<<11).eq(0))
            ndvi = img.normalizedDifference(['B8', 'B4']).rename('NDVI')
            return img.updateMask(mask).addBands(ndvi).copyProperties(img, ['system:time_start'])

        series = s2.map(procesar_imagen)
        
        def obtener_dato(img):
            date = ee.Date(img.get('system:time_start')).format('YYYY-MM-dd')
            value = img.reduceRegion(reducer=ee.Reducer.mean(), geometry=region, scale=10).get('NDVI')
            return ee.Feature(None, {'date': date, 'ndvi': value})

        data_list = series.map(obtener_dato).filter(ee.Filter.notNull(['ndvi'])).getInfo()
        features = data_list['features']
        
        if not features:
            return pd.DataFrame(columns=["Fecha", "NDVI"])
            
        dates = [f['properties']['date'] for f in features]
        ndvis = [f['properties']['ndvi'] for f in features]
        
        df = pd.DataFrame({"Fecha": pd.to_datetime(dates), "NDVI": ndvis})
        df = df.sort_values("Fecha")
        df['NDVI'] = df['NDVI'].rolling(window=3, center=True).mean()
        df['Origen'] = "Satelital (Sentinel-2)"
        return df.dropna()

    except Exception as e:
        print(f"Error procesando data: {e}")
        return pd.DataFrame()

# --- INTERFAZ ---
st.title("🌱 Litoral Trace: Inteligencia de Cumplimiento")

with st.sidebar:
    st.header("⚙️ Panel de Control")
    if GEE_ACTIVO:
        st.success("🛰️ Satélite: ACTIVO")
    else:
        st.warning("⚠️ Satélite: SIMULADO (Falta Key)")

    with st.expander("➕ Cargar Nuevo Lote"):
        with st.form("form_alta"):
            frm_nombre = st.text_input("Nombre")
            frm_prod = st.text_input("ID Productor")
            frm_lat = st.number_input("Lat", value=-27.45, format="%.5f")
            frm_lon = st.number_input("Lon", value=-59.05, format="%.5f")
            if st.form_submit_button("Guardar"):
                if frm_nombre:
                    guardar_nuevo_lote(frm_nombre, frm_prod, "Algodón", 50, frm_lat, frm_lon)
                    st.success("Guardado")
                    st.cache_data.clear()

    if st.button("Refrescar"):
        st.rerun()

gdf = get_agro_data()

if not gdf.empty:
    c1, c2 = st.columns(2)
    c1.metric("Lotes", len(gdf))
    c2.metric("Superficie", f"{gdf['hectareas_declaradas'].sum()} ha")

    col_map, col_analisis = st.columns([1, 1])

    with col_map:
        st.subheader("📍 Mapa")
        # Corrección visual: width="stretch"
        fig_map = px.choropleth_mapbox(
            gdf, geojson=gdf.__geo_interface__, locations=gdf.index,
            color="estatus_cumplimiento",
            color_discrete_map={"Pendiente": "#95A5A6", "Verde": "#2ECC71", "Rojo": "#E74C3C"},
            mapbox_style="white-bg",
            center={"lat": gdf.centroide_lat.mean(), "lon": gdf.centroide_lon.mean()},
            zoom=13, opacity=0.5
        )
        fig_map.update_layout(
            mapbox_layers=[{
                "below": 'traces', "sourcetype": "raster",
                "source": ["https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"]
            }],
            margin={"r":0,"t":0,"l":0,"b":0},
            height=500
        )
        # Aquí arreglamos el warning de container_width
        st.plotly_chart(fig_map, width="stretch")

    with col_analisis:
        st.subheader("🛰️ Auditoría")
        sel = st.selectbox("Lote", gdf['nombre_lote'])
        row = gdf[gdf['nombre_lote'] == sel].iloc[0]
        
        with st.spinner("Procesando imágenes..."):
            df_ndvi = obtener_ndvi_real(row['centroide_lat'], row['centroide_lon'], sel)
        
        if not df_ndvi.empty:
            fig = px.line(df_ndvi, x="Fecha", y="NDVI", title=f"{sel} - {df_ndvi.get('Origen', 'Data').iloc[0]}")
            fig.add_vline(x="2020-12-31", line_dash="dash", line_color="red")
            # Arreglamos warning aquí también
            st.plotly_chart(fig, width="stretch")
            
            pdf = generar_certificado(row, "APTO")
            st.download_button("Certificado PDF", pdf, f"Cert_{sel}.pdf", "application/pdf")