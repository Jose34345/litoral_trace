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

# --- ESTILOS CSS ---
st.markdown("""
<style>
    .stButton>button { width: 100%; border-radius: 8px; font-weight: bold; }
    
    /* CARDS DASHBOARD */
    .metric-card { background-color: #ffffff; color: #1f2937; padding: 20px; border-radius: 12px; border-left: 8px solid #2ecc71; box-shadow: 0 4px 6px rgba(0,0,0,0.1); margin-bottom: 10px; }
    .loss-card { background-color: #fff5f5; color: #991b1b; padding: 20px; border-radius: 12px; border-left: 8px solid #e74c3c; box-shadow: 0 4px 6px rgba(0,0,0,0.1); margin-bottom: 10px; }
    .info-card { background-color: #f0f9ff; color: #0c4a6e; padding: 20px; border-radius: 12px; border-left: 8px solid #3498db; box-shadow: 0 4px 6px rgba(0,0,0,0.1); margin-bottom: 10px; }
    .card-label { font-size: 14px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; opacity: 0.8; }
    .card-value { font-size: 28px; font-weight: 800; margin-top: 5px; }
    
    /* PRICING TABLE */
    .pricing-card { border: 1px solid #e5e7eb; border-radius: 10px; padding: 20px; text-align: center; background: white; height: 100%; }
    .pricing-header { font-size: 1.2rem; font-weight: bold; color: #374151; margin-bottom: 10px; }
    .pricing-price { font-size: 2rem; font-weight: 800; color: #111827; }
    .pricing-features { text-align: left; margin-top: 15px; font-size: 0.9rem; color: #6b7280; line-height: 1.6; }
    
    /* LOGIN TABS */
    .stTabs [data-baseweb="tab-list"] { gap: 24px; }
    .stTabs [data-baseweb="tab"] { height: 50px; white-space: pre-wrap; background-color: transparent; border-radius: 4px 4px 0px 0px; gap: 1px; padding-top: 10px; padding-bottom: 10px; }
</style>
""", unsafe_allow_html=True)

# --- DB SETUP ---
try:
    DB_URL = st.secrets["DB_URL"]
except:
    DB_URL = "postgresql://neondb_owner:npg_nxamLK5P6thM@ep-royal-snow-a488eu3z-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require"

engine = create_engine(DB_URL)

# --- AUTH & USERS ---
def hash_pass(password): return hashlib.sha256(str.encode(password)).hexdigest()

def init_users_db():
    create_table = text("CREATE TABLE IF NOT EXISTS usuarios (username TEXT PRIMARY KEY, password_hash TEXT, rol TEXT);")
    check_admin = text("SELECT * FROM usuarios WHERE username = 'admin'")
    insert_admin = text("INSERT INTO usuarios VALUES ('admin', :p, 'admin')")
    with engine.begin() as conn:
        conn.execute(create_table)
        if not conn.execute(check_admin).fetchone(): conn.execute(insert_admin, {"p": hash_pass("admin123")})

def verificar_login(u, p):
    q = text("SELECT * FROM usuarios WHERE username = :u AND password_hash = :p")
    with engine.connect() as conn: return conn.execute(q, {"u": u, "p": hash_pass(p)}).fetchone()

def registrar_usuario(u, p):
    # Por defecto, el usuario nuevo es 'cliente' (NO premium)
    check_q = text("SELECT * FROM usuarios WHERE username = :u")
    insert_q = text("INSERT INTO usuarios (username, password_hash, rol) VALUES (:u, :p, 'cliente')")
    with engine.begin() as conn:
        if conn.execute(check_q, {"u": u}).fetchone(): return False
        conn.execute(insert_q, {"u": u, "p": hash_pass(p)})
        return True

init_users_db()

# --- GEE ---
def inicializar_gee():
    try:
        if "gcp_service_account" in st.secrets:
            service_account = st.secrets["gcp_service_account"]
            creds = ee.ServiceAccountCredentials(service_account['client_email'], key_data=service_account['private_key'])
            ee.Initialize(creds)
            return True
        return False
    except: return False
GEE_ACTIVO = inicializar_gee()

# --- FUNCIONES CORE ---
@st.cache_data(ttl=300)
def get_market_prices():
    tickers = {"Soja": "ZS=F", "Maíz": "ZC=F", "Trigo": "ZW=F"}
    retenciones = {"Soja": 0.33, "Maíz": 0.12, "Trigo": 0.12, "Algodón": 0.05}
    data = {}
    try:
        for nombre, ticker in tickers.items():
            stock = yf.Ticker(ticker)
            hist = stock.history(period="1d")
            price = hist['Close'].iloc[-1] if not hist.empty else 1000.0
            factor = 39.37 if nombre == "Maíz" else 36.74
            data[nombre] = round((price / 100) * factor * (1 - retenciones.get(nombre, 0)), 2)
        data["Algodón"] = 750.00
        return data
    except: return {"Soja": 295.0, "Maíz": 180.0, "Trigo": 210.0, "Algodón": 750.0}

def diagnostico_eudr_ia(df):
    if df.empty or 'Fecha' not in df.columns: return "Pendiente", "Sin datos", 0, 0
    df['Año'] = df['Fecha'].dt.year
    base = df[df['Año'] == 2020]['NDVI'].mean()
    if np.isnan(base): base = df.head(6)['NDVI'].mean()
    recent = df[df['Fecha'] > (df['Fecha'].max() - pd.DateOffset(months=12))]
    actual = recent['NDVI'].mean()
    var = ((actual - base) / base) * 100
    decision = "Rojo" if var < -15 else "Verde"
    return decision, f"Variación Biomasa: {var:.1f}%", base, actual

class PDF(FPDF):
    def header(self): self.set_font('Arial', 'B', 15); self.cell(0, 10, 'LITORAL TRACE - CERTIFICADO', 0, 1, 'C'); self.ln(10)
    def footer(self): self.set_y(-15); self.set_font('Arial', 'I', 8); self.cell(0, 10, 'Audit ID: ' + pd.Timestamp.now().strftime("%Y%m%d"), 0, 0, 'C')

def generar_pdf(lote, estado, razon):
    pdf = PDF(); pdf.add_page(); pdf.set_font('Arial', '', 12)
    pdf.cell(0, 10, f"Lote: {lote['nombre_lote']} | Productor: {lote['productor_id']}", 0, 1)
    pdf.ln(5); pdf.set_font('Arial', 'B', 14); pdf.set_text_color(0,128,0) if estado=="Verde" else pdf.set_text_color(255,0,0)
    pdf.cell(0, 10, f"DICTAMEN: {estado.upper()}", 0, 1, 'C'); pdf.set_text_color(0)
    return pdf.output(dest='S').encode('latin-1')

@st.cache_data(ttl=60)
def get_data():
    q = "SELECT *, ST_AsText(geometria) as wkt FROM lotes_agro"
    with engine.connect() as conn: df = pd.read_sql(text(q), conn)
    if not df.empty:
        df['geometry'] = df['wkt'].apply(wkt.loads)
        return gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")
    return gpd.GeoDataFrame()

def save_lote(n, p, c, h, lat, lon):
    delta = 0.004; poly = f"POLYGON(({lon-delta} {lat-delta}, {lon+delta} {lat-delta}, {lon+delta} {lat+delta}, {lon-delta} {lat+delta}, {lon-delta} {lat-delta}))"
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
        data = s2.map(lambda i: i.addBands(i.normalizedDifference(['B8', 'B4']).rename('NDVI')).copyProperties(i, ['system:time_start']))\
                 .map(lambda i: ee.Feature(None, {'date': i.date().format('YYYY-MM-dd'), 'ndvi': i.reduceRegion(ee.Reducer.mean(), pt.buffer(100), 10).get('NDVI')}))\
                 .filter(ee.Filter.notNull(['ndvi'])).getInfo()
        df = pd.DataFrame([{'Fecha': pd.to_datetime(f['properties']['date']), 'NDVI': f['properties']['ndvi']} for f in data['features']]).sort_values('Fecha')
        df['NDVI'] = df['NDVI'].rolling(3, center=True).mean(); df['Origen'] = "Satelital"
        return df.dropna()
    except: return pd.DataFrame()

# --- PANTALLAS DEL SISTEMA ---

def login_screen():
    c1, c2, c3 = st.columns([1, 1, 1])
    with c2:
        st.markdown("<br><h1 style='text-align: center;'>🔐 Litoral Trace</h1>", unsafe_allow_html=True)
        tab1, tab2 = st.tabs(["Ingresar", "Registrarse"])
        with tab1:
            with st.form("login"):
                u = st.text_input("Usuario"); p = st.text_input("Pass", type="password")
                if st.form_submit_button("Entrar"):
                    user = verificar_login(u, p)
                    if user:
                        st.session_state['logged_in'] = True
                        st.session_state['username'] = u
                        st.session_state['rol'] = user.rol # Guardamos el rol
                        st.rerun()
                    else: st.error("Error credenciales")
        with tab2:
            with st.form("reg"):
                nu = st.text_input("Nuevo Usuario"); np1 = st.text_input("Pass", type="password"); np2 = st.text_input("Repetir", type="password")
                if st.form_submit_button("Crear Cuenta"):
                    if np1==np2 and len(np1)>5:
                        if registrar_usuario(nu, np1): st.success("Creado! Ingresa en la otra pestaña.");
                        else: st.error("Usuario existe")
                    else: st.error("Pass insegura o no coinciden")

def subscription_screen():
    """Pantalla de Venta para usuarios FREE"""
    with st.sidebar:
        st.write(f"Hola, **{st.session_state['username']}**")
        if st.button("Cerrar Sesión"): 
            st.session_state['logged_in'] = False
            st.rerun()
    
    st.title("🚀 Activa tu Licencia Enterprise")
    st.markdown("Has dado el primer paso hacia la exportación segura. Para acceder al monitor satelital en tiempo real, validación EUDR y calculadora financiera, selecciona tu plan.")
    
    # VIDEO PROMO (Placeholder)
    st.video("https://www.youtube.com/watch?v=LXb3EKWsInQ") # Video genérico de agricultura tech
    
    st.divider()
    
    # PLANES
    c1, c2, c3 = st.columns(3)
    
    with c1:
        st.markdown("""
        <div class="pricing-card">
            <div class="pricing-header">PILOTO</div>
            <div class="pricing-price">Gratis</div>
            <div class="pricing-features">
                ✅ Registro de Usuario<br>
                ✅ Acceso a Demo Estática<br>
                ❌ Carga de Lotes Propios<br>
                ❌ Certificados PDF<br>
            </div>
            <br>
            <button style="background-color:gray; color:white; border:none; padding:10px; border-radius:5px; width:100%;">Plan Actual</button>
        </div>
        """, unsafe_allow_html=True)
        
    with c2:
        st.markdown("""
        <div class="pricing-card" style="border: 2px solid #2ecc71; transform: scale(1.05);">
            <div class="pricing-header" style="color: #2ecc71;">PRODUCTOR</div>
            <div class="pricing-price">$99 <small>/mes</small></div>
            <div class="pricing-features">
                ✅ <b>Hasta 10 Lotes</b><br>
                ✅ Monitoreo Satelital (S2)<br>
                ✅ Calculadora Financiera<br>
                ✅ Soporte WhatsApp<br>
            </div>
            <br>
        </div>
        """, unsafe_allow_html=True)
        # Botón simulado
        if st.button("Suscribirse (Productor)", type="primary"):
            st.info("Redirigiendo a MercadoPago... (Demo)")
            
    with c3:
        st.markdown("""
        <div class="pricing-card">
            <div class="pricing-header">EXPORTADOR</div>
            <div class="pricing-price">Consultar</div>
            <div class="pricing-features">
                ✅ <b>Lotes Ilimitados</b><br>
                ✅ API para ERP/SAP<br>
                ✅ Multi-Usuario<br>
                ✅ Auditoría Blockchain<br>
            </div>
            <br>
        </div>
        """, unsafe_allow_html=True)
        # Botón Contacto
        st.link_button("Contactar Ventas", "https://wa.me/5491112345678?text=Hola,%20quiero%20info%20sobre%20LitoralTrace%20Enterprise")

    st.info("🔒 ¿Ya pagaste y no ves el acceso? Contacta a soporte para habilitación inmediata.")

def dashboard_screen():
    # ... (Aquí va todo tu código del dashboard actual) ...
    # Sidebar
    with st.sidebar:
        st.write(f"👤 **{st.session_state['username']}** ({st.session_state.get('rol', 'cliente').upper()})")
        if st.button("Cerrar Sesión"): 
            st.session_state['logged_in'] = False
            st.rerun()
        st.divider()
        st.subheader("🇦🇷 Calculadora FAS")
        precios = get_market_prices()
        c_calc = st.selectbox("Cultivo", list(precios.keys()))
        p_ref = precios[c_calc]
        st.metric(f"Precio Productor", f"USD {p_ref:.2f}/tn", delta="FAS Teórico")
        rend = st.number_input("Rinde (tn/ha)", 3.0)
        desc = st.slider("Descuento No-EUDR", 0, 20, 7, format="-%d%%")
        st.divider()
        if GEE_ACTIVO: st.success("🛰️ Satélite: ONLINE")
        else: st.warning("⚠️ Satélite: OFFLINE")
        with st.expander("Alta de Lote"):
            with st.form("new"):
                n = st.text_input("Nombre"); p = st.text_input("ID Fiscal")
                c = st.selectbox("Cultivo", ["Soja", "Maíz", "Trigo", "Algodón"])
                h = st.number_input("Hectáreas", 50); lat = st.number_input("Lat", -27.45); lon = st.number_input("Lon", -59.05)
                if st.form_submit_button("Registrar"):
                    if n: save_lote(n, p, c, h, lat, lon); st.success("Registrado"); st.cache_data.clear()

    # Main
    st.title("🌱 Monitor de Cumplimiento & Riesgo")
    gdf = get_data()
    if not gdf.empty:
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Lotes", len(gdf)); k2.metric("Has Auditadas", f"{gdf['hectareas_declaradas'].sum()} ha")
        val = gdf['hectareas_declaradas'].sum() * rend * p_ref
        k3.metric("Valuación", f"USD {val:,.0f}"); k4.metric("Riesgo", f"USD -{val*(desc/100):,.0f}", delta_color="inverse")
        
        c_map, c_det = st.columns([1, 1])
        with c_map:
            fig = px.choropleth_mapbox(gdf, geojson=gdf.__geo_interface__, locations=gdf.index, color="estatus_cumplimiento", color_discrete_map={"Pendiente":"#95a5a6","Verde":"#2ecc71","Rojo":"#e74c3c"}, mapbox_style="white-bg", center={"lat":gdf.centroide_lat.mean(), "lon":gdf.centroide_lon.mean()}, zoom=13, opacity=0.5)
            fig.update_layout(mapbox_layers=[{"below": 'traces', "sourcetype": "raster", "source": ["https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"]}], margin={"r":0,"t":0,"l":0,"b":0}, height=500)
            st.plotly_chart(fig, width="stretch")
        with c_det:
            sel = st.selectbox("Auditar Activo:", gdf['nombre_lote'])
            row = gdf[gdf['nombre_lote'] == sel].iloc[0]
            with st.spinner("Analizando..."):
                df_ndvi = get_ndvi(row['centroide_lat'], row['centroide_lon'], sel)
                dec, raz, v_base, v_act = diagnostico_eudr_ia(df_ndvi)
            if not df_ndvi.empty:
                f1, f2 = st.columns(2)
                lote_val = row['hectareas_declaradas'] * rend * p_ref
                f1.markdown(f"""<div class="metric-card"><div class="card-label">Valor FAS</div><div class="card-value">USD {lote_val:,.0f}</div></div>""", unsafe_allow_html=True)
                if dec == "Rojo": f2.markdown(f"""<div class="loss-card"><div class="card-label">Riesgo</div><div class="card-value">USD -{lote_val*(desc/100):,.0f}</div></div>""", unsafe_allow_html=True)
                else: f2.markdown(f"""<div class="info-card"><div class="card-label">Certificación</div><div class="card-value">APROBADA</div></div>""", unsafe_allow_html=True)
                st.plotly_chart(px.line(df_ndvi, x="Fecha", y="NDVI", title=f"Biomasa ({dec})").add_vline(x=pd.Timestamp("2020-12-31"), line_color="red"), width="stretch")
                if dec == "Verde": st.download_button("Certificado Blockchain", generar_pdf(row, dec, raz), "cert.pdf", "application/pdf", type="primary", use_container_width=True)
                else: st.error(f"Bloqueado: {raz}")

# --- RUTEO PRINCIPAL ---
if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False

if not st.session_state['logged_in']:
    login_screen()
else:
    # LÓGICA DE ROLES
    rol_usuario = st.session_state.get('rol', 'cliente')
    
    if rol_usuario == 'admin' or rol_usuario == 'premium':
        dashboard_screen()
    else:
        subscription_screen()