import streamlit as st
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
import plotly.express as px
import numpy as np
from shapely import wkt
from fpdf import FPDF
import ee
import hashlib
import time
import yfinance as yf

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Litoral Trace | Enterprise", layout="wide", page_icon="🌱")

# --- ESTILOS CSS PROFESIONALES (CORREGIDO) ---
st.markdown("""
<style>
    .stButton>button { width: 100%; margin-top: 10px; }
    
    /* TARJETA VERDE (GANANCIA/VALOR) */
    .metric-card { 
        background-color: #ffffff; 
        color: #1f2937; /* TEXTO OSCURO OBLIGATORIO */
        padding: 20px; 
        border-radius: 12px; 
        border-left: 8px solid #2ecc71; 
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        margin-bottom: 10px;
    }
    
    /* TARJETA ROJA (RIESGO) */
    .loss-card { 
        background-color: #fff5f5; 
        color: #991b1b; /* ROJO OSCURO PARA TEXTO */
        padding: 20px; 
        border-radius: 12px; 
        border-left: 8px solid #e74c3c; 
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        margin-bottom: 10px;
    }

    /* TARJETA AZUL (INFO/CERTIFICADO) */
    .info-card {
        background-color: #f0f9ff;
        color: #0c4a6e; /* AZUL OSCURO PARA TEXTO */
        padding: 20px;
        border-radius: 12px;
        border-left: 8px solid #3498db;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        margin-bottom: 10px;
    }
    
    /* TEXTOS DENTRO DE TARJETAS */
    .card-label { font-size: 14px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; opacity: 0.8; }
    .card-value { font-size: 28px; font-weight: 800; margin-top: 5px; }
    
    /* LOGIN */
    .login-box { padding: 2rem; border-radius: 10px; background-color: white; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
</style>
""", unsafe_allow_html=True)

# --- CONEXIÓN DB ---
try:
    DB_URL = st.secrets["DB_URL"]
except:
    DB_URL = "postgresql://neondb_owner:npg_nxamLK5P6thM@ep-royal-snow-a488eu3z-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require"

engine = create_engine(DB_URL)

# --- SEGURIDAD Y USUARIOS 🔐 ---
def hash_pass(password):
    return hashlib.sha256(str.encode(password)).hexdigest()

def init_users_db():
    create_table = text("""
        CREATE TABLE IF NOT EXISTS usuarios (
            username TEXT PRIMARY KEY, password_hash TEXT, rol TEXT
        );
    """)
    check_admin = text("SELECT * FROM usuarios WHERE username = 'admin'")
    insert_admin = text("INSERT INTO usuarios VALUES ('admin', :p, 'admin')")
    
    with engine.begin() as conn:
        conn.execute(create_table)
        if not conn.execute(check_admin).fetchone():
            conn.execute(insert_admin, {"p": hash_pass("admin123")})

def verificar_login(u, p):
    q = text("SELECT * FROM usuarios WHERE username = :u AND password_hash = :p")
    with engine.connect() as conn:
        return conn.execute(q, {"u": u, "p": hash_pass(p)}).fetchone() is not None

init_users_db()

# --- GEE SETUP ---
def inicializar_gee():
    try:
        if "gcp_service_account" in st.secrets:
            service_account = st.secrets["gcp_service_account"]
            creds = ee.ServiceAccountCredentials(
                service_account['client_email'], 
                key_data=service_account['private_key']
            )
            ee.Initialize(creds)
            return True
        return False
    except:
        return False

GEE_ACTIVO = inicializar_gee()

# --- MOTOR FINANCIERO (ARGENTINA FAS) 🇦🇷 ---
@st.cache_data(ttl=300)
def get_market_prices():
    tickers = {"Soja": "ZS=F", "Maíz": "ZC=F", "Trigo": "ZW=F"}
    retenciones = {"Soja": 0.33, "Maíz": 0.12, "Trigo": 0.12, "Algodón": 0.05}
    data = {}
    try:
        for nombre, ticker in tickers.items():
            stock = yf.Ticker(ticker)
            hist = stock.history(period="1d")
            price_bushel = hist['Close'].iloc[-1] if not hist.empty else (1200.0 if nombre == "Soja" else 500.0)
            factor = 39.37 if nombre == "Maíz" else 36.74
            price_chicago = (price_bushel / 100) * factor
            precio_arg = price_chicago * (1 - retenciones.get(nombre, 0))
            data[nombre] = round(precio_arg, 2)
        data["Algodón"] = 750.00
        return data
    except:
        return {"Soja": 295.0, "Maíz": 180.0, "Trigo": 210.0, "Algodón": 750.0}

# --- MOTOR IA ---
def diagnostico_eudr_ia(df):
    if df.empty or 'Fecha' not in df.columns: return "Pendiente", "Sin datos", 0, 0
    df['Año'] = df['Fecha'].dt.year
    df_base = df[df['Año'] == 2020]
    if df_base.empty: df_base = df.head(6)
    base = df_base['NDVI'].mean()
    recent = df[df['Fecha'] > (df['Fecha'].max() - pd.DateOffset(months=12))]
    if recent.empty: return "Pendiente", "Faltan datos", base, 0
    actual = recent['NDVI'].mean()
    var = ((actual - base) / base) * 100
    decision = "Rojo" if var < -15 else "Verde"
    razon = f"Variación Biomasa: {var:.1f}%"
    return decision, razon, base, actual

# --- PDF ---
class PDF(FPDF):
    def header(self):
        self.set_font('Arial', 'B', 15); self.cell(0, 10, 'LITORAL TRACE - CERTIFICADO EXPORTACION', 0, 1, 'C'); self.ln(10)
    def footer(self):
        self.set_y(-15); self.set_font('Arial', 'I', 8); self.cell(0, 10, 'Audit ID: ' + pd.Timestamp.now().strftime("%Y%m%d"), 0, 0, 'C')

def generar_pdf(lote, estado, razon):
    pdf = PDF(); pdf.add_page(); pdf.set_font('Arial', '', 12)
    pdf.cell(0, 10, f"Activo: {lote['nombre_lote']}", 0, 1)
    pdf.cell(0, 10, f"Productor ID: {lote['productor_id']}", 0, 1)
    pdf.ln(10)
    pdf.set_font('Arial', 'B', 14)
    color = (0, 128, 0) if estado == "Verde" else (255, 0, 0)
    pdf.set_text_color(*color)
    pdf.cell(0, 10, f"DICTAMEN: {estado.upper()}", 0, 1, 'C')
    pdf.set_text_color(0)
    pdf.set_font('Arial', '', 10)
    pdf.cell(0, 10, f"Detalle Tecnico: {razon}", 0, 1, 'C')
    return pdf.output(dest='S').encode('latin-1')

# --- DATA ---
@st.cache_data(ttl=60)
def get_data():
    q = "SELECT *, ST_AsText(geometria) as wkt FROM lotes_agro"
    with engine.connect() as conn: df = pd.read_sql(text(q), conn)
    if not df.empty:
        df['geometry'] = df['wkt'].apply(wkt.loads)
        return gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")
    return gpd.GeoDataFrame()

def save_lote(n, p, c, h, lat, lon):
    delta = 0.004
    poly = f"POLYGON(({lon-delta} {lat-delta}, {lon+delta} {lat-delta}, {lon+delta} {lat+delta}, {lon-delta} {lat+delta}, {lon-delta} {lat-delta}))"
    q = text("INSERT INTO lotes_agro (nombre_lote, productor_id, tipo_cultivo, hectareas_declaradas, estatus_cumplimiento, centroide_lat, centroide_lon, geometria) VALUES (:n, :p, :c, :h, 'Pendiente', :lat, :lon, ST_GeomFromText(:poly, 4326))")
    with engine.begin() as conn: conn.execute(q, {"n":n, "p":p, "c":c, "h":h, "lat":lat, "lon":lon, "poly":poly})

@st.cache_data(ttl=3600)
def get_ndvi(lat, lon, nombre):
    if not GEE_ACTIVO:
        fechas = pd.date_range("2020-01-01", "2026-01-01", freq="ME")
        return pd.DataFrame({"Fecha": fechas, "NDVI": 0.4 + 0.3 * np.sin(np.linspace(0, 20, len(fechas))), "Origen": "Simulado"})
    try:
        pt = ee.Geometry.Point([lon, lat])
        s2 = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED").filterDate('2020-01-01', '2026-01-01').filterBounds(pt).filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 20))
        def proc(img): return img.addBands(img.normalizedDifference(['B8', 'B4']).rename('NDVI')).copyProperties(img, ['system:time_start'])
        data = s2.map(proc).map(lambda i: ee.Feature(None, {'date': i.date().format('YYYY-MM-dd'), 'ndvi': i.reduceRegion(ee.Reducer.mean(), pt.buffer(100), 10).get('NDVI')})).filter(ee.Filter.notNull(['ndvi'])).getInfo()
        df = pd.DataFrame([{'Fecha': pd.to_datetime(f['properties']['date']), 'NDVI': f['properties']['ndvi']} for f in data['features']]).sort_values('Fecha')
        df['NDVI'] = df['NDVI'].rolling(3, center=True).mean(); df['Origen'] = "Satelital"
        return df.dropna()
    except: return pd.DataFrame()

# --- PANTALLAS ---
if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False

def login_screen():
    c1, c2, c3 = st.columns([1, 1, 1])
    with c2:
        st.markdown("<br><br><h1 style='text-align: center;'>🔐 Litoral Trace</h1>", unsafe_allow_html=True)
        st.markdown("<p style='text-align: center;'>Plataforma de Inteligencia Territorial</p>", unsafe_allow_html=True)
        with st.form("login_form"):
            username = st.text_input("Usuario Corporativo")
            password = st.text_input("Contraseña", type="password")
            submit = st.form_submit_button("Iniciar Sesión")
            if submit:
                if verificar_login(username, password):
                    st.session_state['logged_in'] = True
                    st.success("Credenciales Verificadas"); time.sleep(0.5); st.rerun()
                else: st.error("Acceso Denegado")
        st.caption("Soporte Técnico: admin / admin123")

def dashboard_screen():
    # SIDEBAR
    with st.sidebar:
        st.write("👤 **Admin Session**")
        if st.button("Cerrar Sesión"): st.session_state['logged_in'] = False; st.rerun()
        st.divider()
        st.subheader("🇦🇷 Calculadora FAS")
        precios = get_market_prices()
        c_calc = st.selectbox("Cultivo", list(precios.keys()))
        p_ref = precios[c_calc]
        st.metric(f"Precio Productor ({c_calc})", f"USD {p_ref:.2f}/tn", delta="Descontando Retenciones", delta_color="normal")
        rend = st.number_input("Rinde (tn/ha)", value=3.0)
        desc = st.slider("Descuento No-EUDR", 0, 20, 7, format="-%d%%")
        st.divider()
        st.caption("Estado del Sistema")
        if GEE_ACTIVO: st.success("🛰️ Satélite: ONLINE")
        else: st.warning("⚠️ Satélite: OFFLINE")
        with st.expander("Alta de Lote"):
            with st.form("new"):
                n = st.text_input("Nombre"); p = st.text_input("ID Fiscal")
                c = st.selectbox("Cultivo", ["Soja", "Maíz", "Trigo", "Algodón"])
                h = st.number_input("Hectáreas", 50); lat = st.number_input("Lat", -27.45); lon = st.number_input("Lon", -59.05)
                if st.form_submit_button("Registrar"):
                    if n: save_lote(n, p, c, h, lat, lon); st.success("Registrado"); st.cache_data.clear()

    # MAIN
    st.title("🌱 Monitor de Cumplimiento & Riesgo")
    gdf = get_data()
    
    if not gdf.empty:
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Cartera de Activos", len(gdf))
        total_has = gdf['hectareas_declaradas'].sum()
        k2.metric("Superficie Auditada", f"{total_has} ha")
        val_total = total_has * rend * p_ref
        k3.metric("Valuación Cartera", f"USD {val_total:,.0f}")
        riesgo_usd = val_total * (desc/100)
        k4.metric("Riesgo Exposición", f"USD -{riesgo_usd:,.0f}", delta="Pérdida Potencial", delta_color="inverse")

        c_map, c_det = st.columns([1, 1])
        with c_map:
            st.subheader("🗺️ Geolocalización")
            fig = px.choropleth_mapbox(gdf, geojson=gdf.__geo_interface__, locations=gdf.index, color="estatus_cumplimiento", color_discrete_map={"Pendiente":"#95a5a6","Verde":"#2ecc71","Rojo":"#e74c3c"}, mapbox_style="white-bg", center={"lat":gdf.centroide_lat.mean(), "lon":gdf.centroide_lon.mean()}, zoom=13, opacity=0.5)
            fig.update_layout(mapbox_layers=[{"below": 'traces', "sourcetype": "raster", "source": ["https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"]}], margin={"r":0,"t":0,"l":0,"b":0}, height=500)
            st.plotly_chart(fig, width="stretch")
            
        with c_det:
            st.subheader("📊 Auditoría Financiera/Ambiental")
            sel = st.selectbox("Activo a Auditar:", gdf['nombre_lote'])
            row = gdf[gdf['nombre_lote'] == sel].iloc[0]
            
            with st.spinner("Consultando Sentinel-2 y Mercados..."):
                df_ndvi = get_ndvi(row['centroide_lat'], row['centroide_lon'], sel)
                dec, raz, v_base, v_act = diagnostico_eudr_ia(df_ndvi)
            
            if not df_ndvi.empty:
                lote_val = row['hectareas_declaradas'] * rend * p_ref
                lote_loss = lote_val * (desc/100)
                
                f1, f2 = st.columns(2)
                # AQUÍ ESTÁ LA MAGIA VISUAL: HTML PURO CON ESTILOS INLINE Y CLASES
                f1.markdown(f"""
                <div class="metric-card">
                    <div class="card-label">Valor de Mercado (FAS)</div>
                    <div class="card-value">USD {lote_val:,.0f}</div>
                </div>
                """, unsafe_allow_html=True)
                
                if dec == "Rojo":
                    f2.markdown(f"""
                    <div class="loss-card">
                        <div class="card-label">Impacto Financiero</div>
                        <div class="card-value">USD -{lote_loss:,.0f}</div>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    f2.markdown(f"""
                    <div class="info-card">
                        <div class="card-label">Certificación EUDR</div>
                        <div class="card-value">HABILITADA</div>
                    </div>
                    """, unsafe_allow_html=True)

                st.plotly_chart(px.line(df_ndvi, x="Fecha", y="NDVI", title=f"Curva Biológica ({dec})").add_vline(x=pd.Timestamp("2020-12-31"), line_color="red"), width="stretch")
                
                if dec == "Verde":
                    st.download_button("Emitir Certificado Blockchain", generar_pdf(row, dec, raz), "cert.pdf", "application/pdf", type="primary", use_container_width=True)
                else:
                    st.error(f"🚫 EXPORTACIÓN BLOQUEADA: {raz}")

# --- CONTROL DE FLUJO ---
if st.session_state['logged_in']: dashboard_screen()
else: login_screen()