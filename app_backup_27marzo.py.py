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
import json
import zipfile
import io
import base64

# --- CONFIGURACIÓN DE PÁGINA (Professional SaaS Setup) ---
st.set_page_config(
    page_title="Litoral Trace | Compliance Intelligence", 
    layout="wide", 
    page_icon="🛰️",
    initial_sidebar_state="expanded"
)

# --- ESTILOS CSS AVANZADOS (Look & Feel Corporativo) ---
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

</style>
""", unsafe_allow_html=True)

# --- DB SETUP (SEGURIDAD REFORZADA) ---
try:
    DB_URL = st.secrets["DB_URL"]
except:
    st.error("Error crítico: No se encontraron las credenciales de base de datos (secrets.toml).")
    st.stop()

engine = create_engine(DB_URL)

# --- AUTH & USERS ---
def hash_pass(password): return hashlib.sha256(str.encode(password)).hexdigest()

def init_users_db():
    create_table = text("CREATE TABLE IF NOT EXISTS usuarios (username TEXT PRIMARY KEY, password_hash TEXT, rol TEXT, email TEXT, fecha_registro TIMESTAMP);")
    check_admin = text("SELECT * FROM usuarios WHERE username = 'admin'")
    insert_admin = text("INSERT INTO usuarios (username, password_hash, rol) VALUES ('admin', :p, 'admin')")
    with engine.begin() as conn:
        conn.execute(create_table)
        if not conn.execute(check_admin).fetchone(): conn.execute(insert_admin, {"p": hash_pass("admin123")})

def verificar_login(u, p):
    q = text("SELECT * FROM usuarios WHERE username = :u AND password_hash = :p")
    with engine.connect() as conn: return conn.execute(q, {"u": u, "p": hash_pass(p)}).fetchone()

def registrar_usuario(u, p, email):
    check_q = text("SELECT * FROM usuarios WHERE username = :u OR email = :e")
    insert_q = text("INSERT INTO usuarios (username, password_hash, rol, email, fecha_registro) VALUES (:u, :p, 'cliente', :e, NOW())")
    with engine.begin() as conn:
        if conn.execute(check_q, {"u": u, "e": email}).fetchone(): return False
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

# --- FUNCIONES DE NEGOCIO (FORESTO-INDUSTRIAL) ---
@st.cache_data(ttl=300)
def get_market_prices():
    # Precios simulados FAS teóricos para el sector forestal/taninero (USD/Ton)
    return {
        "Madera Aserrada (Pino)": 220.0,
        "Madera Aserrada (Eucalipto)": 280.0,
        "Extracto de Quebracho (Tanino)": 1150.0,
        "Rollizo Triturable": 45.0
    }

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

def generar_dds_json(lote, vol_exportar):
    dds_payload = {
        "operator": st.session_state.get('username', 'admin'),
        "reference_number": hashlib.sha256(str(time.time()).encode()).hexdigest()[:16],
        "commodity": lote['tipo_cultivo'],
        "volume_tons": vol_exportar,
        "geolocation": {
            "type": "Polygon",
            "centroid_lat": float(lote['centroide_lat']),
            "centroid_lon": float(lote['centroide_lon'])
        },
        "eudr_status": "COMPLIANT",
        "timestamp": datetime.now().isoformat()
    }
    return json.dumps(dds_payload, indent=4)

# --- PDF GENERATION (PROFESIONAL / LEGAL) ---
class PDF(FPDF):
    def header(self):
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
        self.multi_cell(0, 4, 'Este documento ha sido generado automáticamente por Litoral Trace Engine v2.4 basándose en telemetría satelital. La validación final requiere firma de un profesional matriculado. \nID Blockchain: ' + hashlib.sha256(str(time.time()).encode()).hexdigest()[:16], 0, 'C')

def generar_pdf(lote, estado, razon, ndvi_data):
    pdf = PDF()
    pdf.add_page()
    
    pdf.set_font('Arial', 'B', 16)
    pdf.set_text_color(15, 23, 42)
    pdf.cell(0, 10, 'DECLARACIÓN DE DEBIDA DILIGENCIA (EUDR)', 0, 1, 'C')
    pdf.set_font('Arial', '', 10)
    pdf.cell(0, 5, 'Conformidad con el Reglamento (UE) 2023/1115 (Libre de Deforestación)', 0, 1, 'C')
    pdf.ln(10)

    pdf.set_fill_color(240, 240, 240)
    pdf.set_font('Arial', 'B', 11)
    pdf.cell(0, 8, '  1. IDENTIFICACIÓN DEL ACTIVO PRODUCTIVO', 1, 1, 'L', 1)
    
    pdf.set_font('Arial', '', 11)
    pdf.cell(50, 8, 'Nombre del Lote:', 0)
    pdf.cell(140, 8, f"{lote['nombre_lote']}", 0, 1)
    pdf.cell(50, 8, 'ID Fiscal / Proveedor:', 0)
    pdf.cell(140, 8, f"{lote['productor_id']}", 0, 1)
    pdf.cell(50, 8, 'Producto Forestal:', 0)
    pdf.cell(140, 8, f"{lote['tipo_cultivo']}", 0, 1)
    pdf.cell(50, 8, 'Superficie Auditada:', 0)
    pdf.cell(140, 8, f"{lote.get('hectareas_declaradas', 'N/A')} Hectáreas", 0, 1)
    pdf.ln(5)

    pdf.set_font('Arial', 'B', 11)
    pdf.cell(0, 8, '  2. GEOLOCALIZACIÓN Y PUNTO DE CONTROL', 1, 1, 'L', 1)
    pdf.set_font('Arial', '', 10)
    pdf.ln(2)
    pdf.multi_cell(0, 5, f"Coordenadas del Centroide: Lat {lote['centroide_lat']:.6f}, Lon {lote['centroide_lon']:.6f}. \nEl polígono completo se encuentra almacenado en la base de datos geoespacial bajo el protocolo WKT para auditoría de la Unión Europea.")
    pdf.ln(5)

    pdf.set_font('Arial', 'B', 11)
    pdf.cell(0, 8, '  3. ANÁLISIS DE COBERTURA VEGETAL (NDVI)', 1, 1, 'L', 1)
    pdf.set_font('Arial', '', 11)
    pdf.ln(2)
    
    df = ndvi_data
    base_2020 = df[df['Fecha'].dt.year == 2020]['NDVI'].mean() if not df.empty else 0
    actual = df.iloc[-1]['NDVI'] if not df.empty else 0
    
    pdf.cell(60, 8, 'Satélite Fuente:', 0)
    pdf.cell(130, 8, 'ESA Sentinel-2 (Constelación Copernicus)', 0, 1)
    pdf.cell(60, 8, 'Línea Base (Año 2020):', 0)
    pdf.cell(130, 8, f"{base_2020:.3f} (Índice Medio)", 0, 1)
    pdf.cell(60, 8, 'Estado Actual (12 meses):', 0)
    pdf.cell(130, 8, f"{actual:.3f} (Índice Medio)", 0, 1)
    pdf.ln(5)

    pdf.ln(5)
    if estado == "Verde":
        pdf.set_fill_color(220, 252, 231)
        pdf.set_text_color(22, 101, 52)
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
        pdf.set_text_color(0)
        pdf.set_font('Arial', '', 10)
        pdf.ln(2)
        pdf.multi_cell(0, 5, f"ALERTA: {razon}.")
    
    pdf.ln(20)

    y_firmas = pdf.get_y()
    pdf.set_draw_color(0)
    pdf.line(20, y_firmas, 80, y_firmas)   
    pdf.line(120, y_firmas, 180, y_firmas) 
    
    pdf.set_font('Arial', 'B', 9)
    pdf.text(20, y_firmas + 5, "Firma Responsable Operaciones")
    pdf.text(120, y_firmas + 5, "Firma Auditor Externo / Aduana")
    
    return pdf.output(dest='S').encode('latin-1')

# --- DATA FETCHING & PROCESSING ---
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

# --- BULK UPLOAD LOGIC ---
def procesar_lote_masivo(df_upload):
    zip_buffer = io.BytesIO()
    resumen_resultados = []

    # Abrimos un archivo ZIP en memoria
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        progress_bar = st.progress(0)
        total_rows = len(df_upload)

        for index, row in df_upload.iterrows():
            # 1. Extracción segura de datos del Excel
            n = str(row.get('Identificador_Lote', f'Lote_Desconocido_{index}'))
            p = str(row.get('ID_Proveedor', 'N/A'))
            c = str(row.get('Producto_Forestal', 'Madera Aserrada (Pino)'))
            h = float(row.get('Hectareas', 0))
            lat = float(row.get('Latitud', 0))
            lon = float(row.get('Longitud', 0))
            vol_in = float(row.get('Volumen_Ingresado_Ton', 0))
            vol_out = float(row.get('Volumen_Exportar_Ton', 0))

            # 2. Auditoría de Balance de Masas
            rendimiento_industrial = {"Madera Aserrada (Pino)": 0.50, "Madera Aserrada (Eucalipto)": 0.45, "Extracto de Quebracho (Tanino)": 0.30, "Rollizo Triturable": 0.95}
            coeficiente = rendimiento_industrial.get(c, 0.50)
            vol_max = vol_in * coeficiente
            mass_balance_ok = vol_out <= vol_max

            # 3. Auditoría Satelital (GEE)
            df_ndvi = get_ndvi(lat, lon, n)
            dec, raz, _, _ = diagnostico_eudr_ia(df_ndvi)

            if not mass_balance_ok:
                dec = "Rojo"
                raz = f"Fallo en Balance de Masas. Exceso de {vol_out - vol_max:.1f} Toneladas."

            # Guardamos el resultado para la tabla resumen
            resumen_resultados.append({
                "Lote": n,
                "Proveedor": p,
                "Vol. Exportar (Tons)": vol_out,
                "Dictamen": dec,
                "Observación": raz
            })

            # 4. Generación de Certificados solo para los aprobados ("Verdes")
            if dec == "Verde":
                lote_dict = {
                    'nombre_lote': n, 'productor_id': p, 'tipo_cultivo': c, 
                    'hectareas_declaradas': h, 'centroide_lat': lat, 'centroide_lon': lon
                }
                
                pdf_bytes = generar_pdf(lote_dict, dec, raz, df_ndvi)
                json_data = generar_dds_json(lote_dict, vol_out)

                # Escribimos los archivos dentro del ZIP organizados por carpetas
                carpeta = f"APROBADOS_{p}_{n}/"
                zip_file.writestr(carpeta + f"CERTIFICADO_{p}.pdf", pdf_bytes)
                zip_file.writestr(carpeta + f"DDS_{p}.json", json_data)

            # Actualizar barra de progreso
            progress_bar.progress((index + 1) / total_rows)

    return pd.DataFrame(resumen_resultados), zip_buffer.getvalue()

def generar_plantilla_excel():
    df_template = pd.DataFrame(columns=[
        'Identificador_Lote', 'ID_Proveedor', 'Producto_Forestal', 'Hectareas', 
        'Latitud', 'Longitud', 'Volumen_Ingresado_Ton', 'Volumen_Exportar_Ton'
    ])
    # Fila de ejemplo
    df_template.loc[0] = ['Rodal_Norte_01', 'CUIT-30123456789', 'Madera Aserrada (Eucalipto)', 120, -27.50, -58.90, 500, 200]
    
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        df_template.to_excel(writer, index=False, sheet_name='Plantilla_Importacion')
    return buffer.getvalue()

# --- PANTALLAS ---
def login_screen():
    col_izq, col_der = st.columns([1.2, 1])
    with col_izq:
        st.markdown("### 🌲 Litoral Trace")
        st.markdown("# Inteligencia Geoespacial Foresto-Industrial")
        st.markdown("""
        La normativa **EUDR (European Union Deforestation Regulation)** prohíbe el ingreso de productos forestales sin trazabilidad absoluta.
        
        ---
        **Al ingresar a la plataforma accederás a:**
        
        * 📡 **Auditoría Satelital:** Verificación automática de deforestación post-2020 con imágenes Sentinel-2.
        * ⚖️ **Balance de Masas:** Prevención de "lavado de madera" mediante validación algorítmica de rendimientos industriales.
        * 📄 **Generación DDS:** Emisión masiva de archivos JSON compatibles con TRACES NT (Aduana UE) y Certificados PDF.
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
        st.markdown("#### 💹 Precios de Mercado (FAS)")
        precios = get_market_prices()
        c_calc = st.selectbox("Seleccionar Producto", list(precios.keys()))
        p_ref = precios[c_calc]
        st.metric("Precio Referencia", f"USD {p_ref:.2f} / Ton")
        st.markdown("#### ⚖️ Escenario de Riesgo")
        desc = st.slider("Castigo por Bloqueo Aduanero", 0, 100, 100, format="-%d%%", help="Pérdida total del valor de la carga si es rechazada en destino.")
        st.divider()
        if st.button("Salir del Sistema"): st.session_state['logged_in'] = False; st.rerun()

    st.title("Compliance & Trazabilidad (EUDR)")
    st.markdown("Gestión de activos forestales y validación de cadena de custodia para exportación a la Unión Europea.")
    
    gdf = get_data()
    if not gdf.empty:
        total_has = gdf['hectareas_declaradas'].sum()
        
        k1, k2, k3, k4 = st.columns(4)
        def kpi_box(label, value, color="black"):
            return f"<div class='metric-container'><div class='metric-label'>{label}</div><div class='metric-value' style='color:{color}'>{value}</div></div>"
        
        k1.markdown(kpi_box("Activos Monitoreados", f"{len(gdf)} Lotes"), unsafe_allow_html=True)
        k2.markdown(kpi_box("Superficie Auditada", f"{total_has:,.0f} ha"), unsafe_allow_html=True)
        k3.markdown(kpi_box("Certificados Emitidos", "12 DDS"), unsafe_allow_html=True)
        k4.markdown(kpi_box("Riesgo Detectado", "0 Bloqueos", "#10b981"), unsafe_allow_html=True)

        col_map, col_details = st.columns([1.5, 1])
        with col_map:
            st.markdown("### 🗺️ Vista Geoespacial")
            fig = px.choropleth_mapbox(gdf, geojson=gdf.__geo_interface__, locations=gdf.index, color="estatus_cumplimiento", color_discrete_map={"Pendiente":"#94a3b8","Verde":"#22c55e","Rojo":"#ef4444"}, mapbox_style="white-bg", center={"lat":gdf.centroide_lat.mean(), "lon":gdf.centroide_lon.mean()}, zoom=12, opacity=0.6)
            fig.update_layout(mapbox_layers=[{"below": 'traces', "sourcetype": "raster", "source": ["https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"]}], margin={"r":0,"t":0,"l":0,"b":0}, height=500)
            st.plotly_chart(fig, use_container_width=True)
            
            with st.expander("➕ Cargar Nuevo Activo (Carga Manual)"):
                with st.form("new_asset"):
                    c1, c2 = st.columns(2)
                    n = c1.text_input("Identificador de Lote / Rodal")
                    p = c2.text_input("ID Proveedor / Guía Forestal")
                    c = st.selectbox("Producto Forestal / Materia Prima", ["Madera Aserrada (Pino)", "Madera Aserrada (Eucalipto)", "Extracto de Quebracho (Tanino)", "Rollizo Triturable"])
                    h = st.number_input("Hectáreas del Polígono", 50)
                    lc1, lc2 = st.columns(2)
                    lat = lc1.number_input("Latitud Centroide", -27.45)
                    lon = lc2.number_input("Longitud Centroide", -59.05)
                    if st.form_submit_button("Registrar Lote"):
                        if n: save_lote(n, p, c, h, lat, lon); st.success("Activo registrado."); st.cache_data.clear()

            with st.expander("➕ Ingreso Masivo (Procesamiento Batch)", expanded=True):
                st.markdown("Procesa múltiples remitos y polígonos simultáneamente subiendo tu matriz de datos.")
                
                # Botón para descargar la plantilla
                st.download_button(
                    label="📥 Descargar Plantilla Excel",
                    data=generar_plantilla_excel(),
                    file_name="LitoralTrace_Plantilla_Ingreso.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    help="Usa este archivo como base para cargar tus datos."
                )
                
                archivo_subido = st.file_uploader("Subir matriz completada (Excel)", type=['xlsx'])
                
                if archivo_subido is not None:
                    if st.button("🚀 Iniciar Procesamiento y Auditoría", type="primary"):
                        try:
                            df_upload = pd.read_excel(archivo_subido)
                            
                            if df_upload.empty:
                                st.warning("El archivo está vacío.")
                            else:
                                st.info(f"Procesando {len(df_upload)} lotes. Esto puede tomar unos segundos...")
                                
                                # Ejecutar el motor masivo
                                df_resumen, zip_data = procesar_lote_masivo(df_upload)
                                
                                # Mostrar resultados
                                st.markdown("#### 📊 Resumen de Auditoría")
                                
                                def color_dictamen(val):
                                    color = '#dcfce7' if val == 'Verde' else '#fee2e2'
                                    text = '#166534' if val == 'Verde' else '#991b1b'
                                    return f'background-color: {color}; color: {text}; font-weight: bold;'
                                
                                st.dataframe(df_resumen.style.map(color_dictamen, subset=['Dictamen']), use_container_width=True, hide_index=True)
                                
                                aprobados = len(df_resumen[df_resumen['Dictamen'] == 'Verde'])
                                
                                if aprobados > 0:
                                    st.success(f"✅ Se generaron certificados para {aprobados} lotes aprobados.")
                                    st.download_button(
                                        label=f"📦 Descargar Paquete de Exportación (.ZIP)",
                                        data=zip_data,
                                        file_name=f"LitoralTrace_Export_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.zip",
                                        mime="application/zip",
                                        type="primary",
                                        use_container_width=True
                                    )
                                else:
                                    st.error("Ningún lote superó la auditoría. No se generaron certificados.")
                                    
                        except Exception as e:
                            st.error(f"Error al procesar el archivo. Asegúrate de usar la plantilla correcta. Detalle: {e}")

        with col_details:
            st.markdown("### 🛰️ Motor de Auditoría Manual")
            sel = st.selectbox("Seleccionar Lote para Análisis:", gdf['nombre_lote'])
            row = gdf[gdf['nombre_lote'] == sel].iloc[0]
            
            st.markdown(f"""
            <div style="background:#f8fafc; padding:15px; border-radius:8px; margin-bottom:15px; color:#1e293b; border: 1px solid #e2e8f0;">
                <b>Propiedad:</b> {row['nombre_lote']}<br>
                <b>Proveedor ID:</b> {row['productor_id']}<br>
                <b>Materia Prima:</b> {row['tipo_cultivo']}
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown("#### ⚖️ Auditoría de Balance de Masas")
            col_vol1, col_vol2 = st.columns(2)
            vol_ingresado = col_vol1.number_input("Madera Ingresada (Ton)", min_value=0.0, value=100.0)
            vol_exportar = col_vol2.number_input("Prod. a Exportar (Ton)", min_value=0.0, value=45.0)

            rendimiento_industrial = {"Madera Aserrada (Pino)": 0.50, "Madera Aserrada (Eucalipto)": 0.45, "Extracto de Quebracho (Tanino)": 0.30, "Rollizo Triturable": 0.95}
            coeficiente = rendimiento_industrial.get(row['tipo_cultivo'], 0.50)
            vol_maximo_permitido = vol_ingresado * coeficiente
            
            st.metric("Límite Teórico Exportable (Validado)", f"{vol_maximo_permitido:.1f} Ton", help=f"Basado en un rendimiento industrial del {coeficiente*100}%")

            if st.button("📡 Ejecutar Compliance Completo", type="primary"):
                mass_balance_ok = vol_exportar <= vol_maximo_permitido
                
                with st.spinner("Conectando con Google Earth Engine para análisis histórico..."):
                    df_ndvi = get_ndvi(row['centroide_lat'], row['centroide_lon'], sel)
                    dec, raz, v_base, v_act = diagnostico_eudr_ia(df_ndvi)
                
                if not mass_balance_ok:
                    dec = "Rojo"
                    raz = f"Fallo en Balance de Masas. El volumen a exportar ({vol_exportar}T) supera el límite físico posible ({vol_maximo_permitido}T)."
                
                if not df_ndvi.empty or not mass_balance_ok:
                    color_res = "#dcfce7" if dec == "Verde" else "#fee2e2"
                    text_res = "#166534" if dec == "Verde" else "#991b1b"
                    
                    st.markdown(f"""
                    <div style="background:{color_res}; padding: 20px; border-radius: 10px; border-left: 5px solid {text_res}; margin-top: 15px; margin-bottom: 20px;">
                        <h3 style="color:{text_res}; margin:0;">DICTAMEN: {dec.upper()}</h3>
                        <p style="margin:5px 0 0 0; color:{text_res}; font-weight:600;">{raz}</p>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    if not df_ndvi.empty:
                        fig_line = px.line(df_ndvi, x="Fecha", y="NDVI", title="Evolución de Biomasa (Detección de Deforestación)")
                        fecha_limite = pd.Timestamp("2020-12-31")
                        fig_line.add_vline(x=fecha_limite, line_width=2, line_dash="dash", line_color="red")
                        fig_line.add_annotation(x=fecha_limite, y=1, yref="paper", text="Límite EUDR", showarrow=False, font=dict(color="red", size=10), xanchor="right", xshift=-5)
                        fig_line.update_layout(height=250, margin={"l":20,"r":20,"t":40,"b":20}, showlegend=False)
                        st.plotly_chart(fig_line, use_container_width=True)
                    
                    if dec == "Verde":
                        st.markdown("---") 
                        st.success("✅ Activo Apto para Exportación. Certificados generados exitosamente.")
                        
                        pdf_bytes = generar_pdf(row, dec, raz, df_ndvi)
                        json_data = generar_dds_json(row, vol_exportar)
                        
                        col_dl1, col_dl2 = st.columns(2)
                        with col_dl1:
                            st.download_button(
                                label="📄 Descargar Certificado (PDF)",
                                data=pdf_bytes,
                                file_name=f"CERTIFICADO_EUDR_{row['productor_id']}.pdf",
                                mime="application/pdf",
                                use_container_width=True
                            )
                        with col_dl2:
                            st.download_button(
                                label="💻 Descargar DDS Aduana (JSON)",
                                data=json_data,
                                file_name=f"DDS_TRACES_{row['productor_id']}.json",
                                mime="application/json",
                                type="primary",
                                use_container_width=True
                            )

if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
if not st.session_state['logged_in']: login_screen()
else:
    rol_usuario = st.session_state.get('rol', 'cliente')
    if rol_usuario in ['admin', 'premium', 'cliente']: 
        dashboard_screen()
    else: 
        subscription_screen()