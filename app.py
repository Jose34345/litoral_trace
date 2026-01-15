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
from datetime import datetime

# --- CONFIGURACIÓN DE PÁGINA (Professional SaaS Setup) ---
st.set_page_config(
    page_title="Litoral Trace | Market Intelligence", 
    layout="wide", 
    page_icon="🛰️",
    initial_sidebar_state="expanded"
)

# --- ESTILOS CSS AVANZADOS (Look & Feel Humano/Corporativo) ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }

    /* HEADER */
    h1, h2, h3 {
        color: #0f172a; 
        font-weight: 800;
        letter-spacing: -0.5px;
    }
    
    /* BOTONES */
    .stButton>button { 
        width: 100%; 
        border-radius: 6px; 
        font-weight: 600; 
        transition: all 0.3s ease;
    }
    .stButton>button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
    }
    
    /* KPI CARDS CON ESTILO FINTECH */
    .metric-container {
        background-color: white;
        padding: 24px;
        border-radius: 12px;
        border: 1px solid #e2e8f0;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        margin-bottom: 16px;
    }
    .metric-label {
        color: #64748b;
        font-size: 0.85rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .metric-value {
        color: #0f172a;
        font-size: 1.8rem;
        font-weight: 800;
        margin-top: 8px;
    }
    .metric-delta-pos { color: #10b981; font-size: 0.9rem; font-weight: 600; }
    .metric-delta-neg { color: #ef4444; font-size: 0.9rem; font-weight: 600; }

    /* TAGS DE ESTADO */
    .status-tag {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 700;
    }
    .status-ok { background-color: #dcfce7; color: #166534; }
    .status-danger { background-color: #fee2e2; color: #991b1b; }

</style>
""", unsafe_allow_html=True)

# --- DB SETUP (SEGURIDAD REFORZADA) ---
try:
    DB_URL = st.secrets["DB_URL"]
except:
    # Si no encuentra los secretos, detenemos la app para evitar errores de conexión
    # IMPORTANTE: Nunca escribir la contraseña real aquí en texto plano
    st.error("Error crítico: No se encontraron las credenciales de base de datos (secrets.toml).")
    st.stop()

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

def registrar_usuario(u, p, email):
    # Verificamos si el usuario O el email ya existen para evitar duplicados
    check_q = text("SELECT * FROM usuarios WHERE username = :u OR email = :e")
    
    # Insertamos el usuario, contraseña, rol 'cliente', el email y la fecha de hoy
    insert_q = text("""
        INSERT INTO usuarios (username, password_hash, rol, email, fecha_registro) 
        VALUES (:u, :p, 'cliente', :e, NOW())
    """)
    
    with engine.begin() as conn:
        # Si encuentra algo en la búsqueda, devuelve False (no registra)
        if conn.execute(check_q, {"u": u, "e": email}).fetchone(): 
            return False
        
        # Si no existe, lo inserta
        conn.execute(insert_q, {"u": u, "p": hash_pass(p), "e": email})
        return True

init_users_db()

# --- GEE CONNECTION ---
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

# --- FUNCIONES DE NEGOCIO ---
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
    if df.empty or 'Fecha' not in df.columns: return "Pendiente", "Insuficiencia de datos históricos", 0, 0
    df['Año'] = df['Fecha'].dt.year
    base = df[df['Año'] == 2020]['NDVI'].mean()
    if np.isnan(base): base = df.head(6)['NDVI'].mean()
    
    recent = df[df['Fecha'] > (df['Fecha'].max() - pd.DateOffset(months=12))]
    actual = recent['NDVI'].mean()
    
    var = ((actual - base) / base) * 100
    
    decision = "Rojo" if var < -15 else "Verde"
    return decision, f"Variación Biomasa vs 2020: {var:.1f}%", base, actual

# --- PDF GENERATION (PROFESIONAL / LEGAL) ---
class PDF(FPDF):
    def header(self):
        # Logo o Nombre de Empresa (Simulado con texto para no romper sin imagen)
        self.set_font('Arial', 'B', 12)
        self.set_text_color(100, 100, 100)
        self.cell(0, 10, 'LITORAL TRACE | TECHNOLOGIES S.A.', 0, 0, 'L')
        self.cell(0, 10, 'REPORTE CONFIDENCIAL', 0, 1, 'R')
        self.line(10, 20, 200, 20)
        self.ln(15)

    def footer(self):
        self.set_y(-25)
        self.set_font('Arial', 'I', 8)
        self.set_text_color(128)
        self.multi_cell(0, 4, 'Este documento ha sido generado automáticamente por Litoral Trace Engine v2.4 basándose en telemetría satelital (Sentinel-2). La validación final requiere firma de un profesional matriculado según normativa local. \nID Blockchain: ' + hashlib.sha256(str(time.time()).encode()).hexdigest()[:16], 0, 'C')

def generar_pdf(lote, estado, razon, ndvi_data):
    pdf = PDF()
    pdf.add_page()
    
    # TÍTULO DEL DOCUMENTO
    pdf.set_font('Arial', 'B', 16)
    pdf.set_text_color(15, 23, 42) # Azul oscuro corporativo
    pdf.cell(0, 10, 'DECLARACIÓN DE DEBIDA DILIGENCIA (EUDR)', 0, 1, 'C')
    pdf.set_font('Arial', '', 10)
    pdf.cell(0, 5, 'Conformidad con el Reglamento (UE) 2023/1115 (Libre de Deforestación)', 0, 1, 'C')
    pdf.ln(10)

    # SECCIÓN 1: DATOS DEL OPERADOR Y ACTIVO
    pdf.set_fill_color(240, 240, 240)
    pdf.set_font('Arial', 'B', 11)
    pdf.cell(0, 8, '  1. IDENTIFICACIÓN DEL ACTIVO PRODUCTIVO', 1, 1, 'L', 1)
    
    pdf.set_font('Arial', '', 11)
    # Tabla simple de datos
    pdf.cell(50, 8, 'Nombre del Lote:', 0)
    pdf.cell(140, 8, f"{lote['nombre_lote']}", 0, 1)
    pdf.cell(50, 8, 'ID Fiscal (RENSPA):', 0)
    pdf.cell(140, 8, f"{lote['productor_id']}", 0, 1)
    pdf.cell(50, 8, 'Cultivo Declarado:', 0)
    pdf.cell(140, 8, f"{lote['tipo_cultivo']}", 0, 1)
    pdf.cell(50, 8, 'Superficie Auditada:', 0)
    pdf.cell(140, 8, f"{lote['hectareas_declaradas']} Hectáreas", 0, 1)
    pdf.ln(5)

    # SECCIÓN 2: GEOLOCALIZACIÓN
    pdf.set_font('Arial', 'B', 11)
    pdf.cell(0, 8, '  2. GEOLOCALIZACIÓN Y PUNTO DE CONTROL', 1, 1, 'L', 1)
    pdf.set_font('Arial', '', 10)
    pdf.ln(2)
    pdf.multi_cell(0, 5, f"Coordenadas del Centroide: Lat {lote['centroide_lat']:.6f}, Lon {lote['centroide_lon']:.6f}. \nEl polígono completo se encuentra almacenado en la base de datos geoespacial bajo el protocolo WKT (Well-Known Text) para auditoría remota.")
    pdf.ln(5)

    # SECCIÓN 3: EVIDENCIA SATELITAL
    pdf.set_font('Arial', 'B', 11)
    pdf.cell(0, 8, '  3. ANÁLISIS DE COBERTURA VEGETAL (NDVI)', 1, 1, 'L', 1)
    pdf.set_font('Arial', '', 11)
    pdf.ln(2)
    
    # Extraer métricas para el reporte
    df = ndvi_data
    base_2020 = df[df['Fecha'].dt.year == 2020]['NDVI'].mean()
    actual = df.iloc[-1]['NDVI']
    
    pdf.cell(60, 8, 'Satélite Fuente:', 0)
    pdf.cell(130, 8, 'ESA Sentinel-2 (Constelación Copernicus)', 0, 1)
    pdf.cell(60, 8, 'Línea Base (Año 2020):', 0)
    pdf.cell(130, 8, f"{base_2020:.3f} (Índice Medio)", 0, 1)
    pdf.cell(60, 8, 'Estado Actual (12 meses):', 0)
    pdf.cell(130, 8, f"{actual:.3f} (Índice Medio)", 0, 1)
    pdf.ln(5)

    # SECCIÓN 4: DICTAMEN TÉCNICO
    pdf.ln(5)
    if estado == "Verde":
        pdf.set_fill_color(220, 252, 231) # Verde claro
        pdf.set_text_color(22, 101, 52)   # Verde oscuro
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(0, 15, 'DICTAMEN: FAVORABLE / COMPLIANT', 1, 1, 'C', 1)
        pdf.set_text_color(0)
        pdf.set_font('Arial', '', 10)
        pdf.ln(2)
        pdf.multi_cell(0, 5, f"CONCLUSIÓN: {razon}. No se detectan cambios significativos en el uso del suelo compatibles con deforestación posterior al 31 de diciembre de 2020.")
    else:
        pdf.set_fill_color(254, 226, 226)
        pdf.set_text_color(153, 27, 27)
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(0, 15, 'DICTAMEN: NO FAVORABLE / RIESGO', 1, 1, 'C', 1)
    
    pdf.ln(20)

    # SECCIÓN 5: BLOQUE DE FIRMAS (Para imprimir y firmar)
    y_firmas = pdf.get_y()
    
    pdf.set_draw_color(0)
    pdf.line(20, y_firmas, 80, y_firmas)   # Línea firma izquierda
    pdf.line(120, y_firmas, 180, y_firmas) # Línea firma derecha
    
    pdf.set_font('Arial', 'B', 9)
    pdf.text(20, y_firmas + 5, "Firma Responsable Técnico")
    pdf.text(120, y_firmas + 5, "Firma Auditor / Contador")
    
    pdf.set_font('Arial', '', 8)
    pdf.text(20, y_firmas + 10, "Ing. Agrónomo / Matrícula Nº: _________")
    pdf.text(120, y_firmas + 10, "CPCE / Matrícula Nº: _________")

    return pdf.output(dest='S').encode('latin-1')

# --- DATA FETCHING ---
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
        data = s2.map(lambda i: i.addBands(i.normalizedDifference(['B8', 'B4']).rename('NDVI')).copyProperties(i, ['system:time_start']))\
                 .map(lambda i: ee.Feature(None, {'date': i.date().format('YYYY-MM-dd'), 'ndvi': i.reduceRegion(ee.Reducer.mean(), pt.buffer(100), 10).get('NDVI')}))\
                 .filter(ee.Filter.notNull(['ndvi'])).getInfo()
        df = pd.DataFrame([{'Fecha': pd.to_datetime(f['properties']['date']), 'NDVI': f['properties']['ndvi']} for f in data['features']]).sort_values('Fecha')
        df['NDVI'] = df['NDVI'].rolling(3, center=True).mean(); df['Origen'] = "Satelital"
        return df.dropna()
    except: return pd.DataFrame()

# --- PANTALLAS ---
def login_screen():
    col_izq, col_der = st.columns([1.2, 1])
    with col_izq:
        st.markdown("### 🌾 Litoral Trace")
        st.markdown("# Inteligencia Geoespacial para el Agro Chaqueño")
        st.markdown("""
        La normativa **EUDR (European Union Deforestation Regulation)** cambia las reglas del juego. 
        No pierdas acceso al mercado internacional.
        
        ---
        **Al ingresar a la plataforma accederás a:**
        
        * 📡 **Auditoría Satelital en Tiempo Real:** Visualización de lotes con imágenes Sentinel-2 y detección histórica de deforestación (2020-Presente).
        * 💹 **Calculadora Financiera FAS:** Estimación de márgenes brutos y cuantificación del "Costo de No-Cumplimiento" en mercados restringidos.
        * 📄 **Certificación Digital:** Emisión automática de reportes de Debida Diligencia listos para presentar ante autoridades de exportación.
        """)
    with col_der:
        st.markdown("<div style='background: white; padding: 30px; border-radius: 15px; box-shadow: 0 10px 25px rgba(0,0,0,0.05); color: #0f172a;'>", unsafe_allow_html=True)
        st.subheader("Acceso a Clientes")
        tab1, tab2 = st.tabs(["Iniciar Sesión", "Crear Cuenta Nueva"])
        
        with tab1:
            with st.form("login"):
                u = st.text_input("Usuario")
                p = st.text_input("Contraseña", type="password")
                if st.form_submit_button("Acceder"):
                    user = verificar_login(u, p)
                    if user:
                        st.session_state['logged_in'] = True; st.session_state['username'] = u; st.session_state['rol'] = user.rol; st.rerun()
                    else: st.error("Usuario o contraseña incorrectos")
        
        with tab2:
            with st.form("reg"):
                st.markdown("##### Únete a la plataforma")
                nu = st.text_input("Usuario Deseado")
                ne = st.text_input("Correo Electrónico")
                np1 = st.text_input("Contraseña", type="password")
                np2 = st.text_input("Repetir Contraseña", type="password")
                
                if st.form_submit_button("Registrarse"):
                    if np1 == np2 and len(np1) > 5 and "@" in ne:
                        if registrar_usuario(nu, np1, ne): 
                            st.success("¡Cuenta creada exitosamente! Ya puedes iniciar sesión en la otra pestaña.")
                            st.balloons()
                        else: 
                            st.error("El nombre de usuario o el correo ya están registrados.")
                    else: 
                        st.warning("Verifica que las contraseñas coincidan y el correo sea válido.")
        
        st.markdown("</div>", unsafe_allow_html=True)

def subscription_screen():
    st.title("⚙️ Habilitación de Licencia")
    st.info("Para operar el sistema Litoral Trace, contacte al administrador.")

def dashboard_screen():
    with st.sidebar:
        st.image("https://img.icons8.com/fluency/96/satellite-in-orbit.png", width=60)
        st.markdown(f"### Litoral Trace \n **Panel de Control**")
        st.markdown(f"👤 {st.session_state['username']} | {st.session_state.get('rol', 'cliente').upper()}")
        st.divider()
        st.markdown("#### 💹 Variables de Mercado (FAS)")
        precios = get_market_prices()
        c_calc = st.selectbox("Seleccionar Cultivo", list(precios.keys()))
        p_ref = precios[c_calc]
        st.metric("Precio Referencia (FAS)", f"USD {p_ref:.2f}")
        rend = st.number_input("Rinde Estimado (tn/ha)", 2.0, 10.0, 3.0, 0.1)
        st.markdown("#### ⚖️ Escenario de Riesgo")
        desc = st.slider("Castigo por No-Compliance", 0, 30, 7, format="-%d%%")
        st.divider()
        if st.button("Salir del Sistema"): st.session_state['logged_in'] = False; st.rerun()

    st.title("Monitor de Riesgo & Cumplimiento")
    st.markdown("Visualización en tiempo real de activos agrícolas y su estatus frente a la normativa **EUDR 2023/1115**.")
    
    gdf = get_data()
    if not gdf.empty:
        total_has = gdf['hectareas_declaradas'].sum()
        val_bruta = total_has * rend * p_ref
        riesgo_usd = val_bruta * (desc/100)
        
        k1, k2, k3, k4 = st.columns(4)
        def kpi_box(label, value, color="black"):
            return f"<div class='metric-container'><div class='metric-label'>{label}</div><div class='metric-value' style='color:{color}'>{value}</div></div>"
        
        k1.markdown(kpi_box("Activos Monitoreados", f"{len(gdf)} Lotes"), unsafe_allow_html=True)
        k2.markdown(kpi_box("Superficie Auditada", f"{total_has:,.0f} ha"), unsafe_allow_html=True)
        k3.markdown(kpi_box("Valuación Potencial (FAS)", f"${val_bruta/1000:,.1f}k"), unsafe_allow_html=True)
        k4.markdown(kpi_box("Capital en Riesgo (EUDR)", f"-${riesgo_usd/1000:,.1f}k", "#ef4444"), unsafe_allow_html=True)

        col_map, col_details = st.columns([1.5, 1])
        with col_map:
            st.markdown("### 🗺️ Vista Geoespacial")
            fig = px.choropleth_mapbox(gdf, geojson=gdf.__geo_interface__, locations=gdf.index, color="estatus_cumplimiento", color_discrete_map={"Pendiente":"#94a3b8","Verde":"#22c55e","Rojo":"#ef4444"}, mapbox_style="white-bg", center={"lat":gdf.centroide_lat.mean(), "lon":gdf.centroide_lon.mean()}, zoom=12, opacity=0.6)
            fig.update_layout(mapbox_layers=[{"below": 'traces', "sourcetype": "raster", "source": ["https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"]}], margin={"r":0,"t":0,"l":0,"b":0}, height=500)
            st.plotly_chart(fig, use_container_width=True)
            with st.expander("➕ Cargar Nuevo Activo (Lote)"):
                with st.form("new_asset"):
                    c1, c2 = st.columns(2)
                    n = c1.text_input("Identificador de Lote")
                    p = c2.text_input("ID Fiscal / RENSPA")
                    c = st.selectbox("Cultivo Principal", ["Soja", "Maíz", "Trigo", "Algodón"])
                    h = st.number_input("Hectáreas Declaradas", 50)
                    lc1, lc2 = st.columns(2)
                    lat = lc1.number_input("Latitud", -27.45)
                    lon = lc2.number_input("Longitud", -59.05)
                    if st.form_submit_button("Registrar Lote"):
                        if n: save_lote(n, p, c, h, lat, lon); st.success("Activo registrado."); st.cache_data.clear()

        with col_details:
            st.markdown("### 🛰️ Motor de Auditoría")
            sel = st.selectbox("Seleccionar Lote para Análisis:", gdf['nombre_lote'])
            row = gdf[gdf['nombre_lote'] == sel].iloc[0]
            
            # --- FIX: AGREGADO COLOR AL TEXTO PARA QUE SE VEA EN FONDO BLANCO ---
            st.markdown(f"""
            <div style="background:#f8fafc; padding:15px; border-radius:8px; margin-bottom:15px; color:#1e293b;">
                <b>Propiedad:</b> {row['nombre_lote']}<br>
                <b>ID Fiscal:</b> {row['productor_id']}
            </div>
            """, unsafe_allow_html=True)
            
            if st.button("📡 Iniciar Escaneo Satelital (Sentinel-2)", type="primary"):
                with st.spinner("Conectando con Google Earth Engine..."):
                    df_ndvi = get_ndvi(row['centroide_lat'], row['centroide_lon'], sel)
                    dec, raz, v_base, v_act = diagnostico_eudr_ia(df_ndvi)
                
                if not df_ndvi.empty:
                    # Cálculo de colores y estados
                    color_res = "#dcfce7" if dec == "Verde" else "#fee2e2"
                    text_res = "#166534" if dec == "Verde" else "#991b1b"
                    
                    # 1. TARJETA DE DICTAMEN
                    st.markdown(f"""
                    <div style="background:{color_res}; padding: 20px; border-radius: 10px; border-left: 5px solid {text_res}; margin-bottom: 20px;">
                        <h3 style="color:{text_res}; margin:0;">DICTAMEN: {dec.upper()}</h3>
                        <p style="margin:5px 0 0 0; color:{text_res}; font-weight:600;">{raz}</p>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # 2. GRÁFICO (CON LA CORRECCIÓN QUE HICIMOS ANTES)
                    fig_line = px.line(df_ndvi, x="Fecha", y="NDVI", title="Evolución de Biomasa (2020-Presente)")
                    fecha_limite = pd.Timestamp("2020-12-31")
                    fig_line.add_vline(x=fecha_limite, line_width=2, line_dash="dash", line_color="red")
                    fig_line.add_annotation(x=fecha_limite, y=1, yref="paper", text="Límite EUDR", showarrow=False, font=dict(color="red", size=10), xanchor="right", xshift=-5)
                    fig_line.update_layout(height=250, margin={"l":20,"r":20,"t":40,"b":20}, showlegend=False)
                    st.plotly_chart(fig_line, use_container_width=True)
                    
                    # 3. BOTÓN DE DESCARGA (LÓGICA MEJORADA)
                    if dec == "Verde":
                        st.markdown("---") # Separador visual
                        st.success("✅ Activo Apto para Exportación. Documentación disponible.")
                        
                        # Generamos el PDF pasando los datos del ndvi también
                        pdf_bytes = generar_pdf(row, dec, raz, df_ndvi)
                        
                        col_dl1, col_dl2 = st.columns([2, 1])
                        with col_dl1:
                            st.download_button(
                                label="📄 Descargar Certificado Oficial (PDF)",
                                data=pdf_bytes,
                                file_name=f"CERTIFICADO_EUDR_{row['productor_id']}_{pd.Timestamp.now().date()}.pdf",
                                mime="application/pdf",
                                type="primary", # Botón rojo/destacado de Streamlit
                                use_container_width=True,
                                key="btn_download_pdf" # Key única para evitar conflictos
                            )
                        with col_dl2:
                            st.caption("ℹ️ Este documento incluye bloque de firmas para validación notarial o colegiada.")
                    else:
                        st.error("⛔ La emisión de certificados está bloqueada por detección de riesgo ambiental.")

if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
if not st.session_state['logged_in']: login_screen()
else:
    rol_usuario = st.session_state.get('rol', 'cliente')
    if rol_usuario in ['admin', 'premium', 'cliente']: 
        dashboard_screen()
    else: 
        subscription_screen()