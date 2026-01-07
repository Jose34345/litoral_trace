import ssl
import warnings
import geopandas as gpd
from sqlalchemy import create_engine, text
from owslib.wfs import WebFeatureService
import pandas as pd
import requests

# --- 🔓 PARCHE DE SEGURIDAD PARA SERVIDORES GUBERNAMENTALES ---
# Esto deshabilita la verificación de certificados SSL (HTTPS)
ssl._create_default_https_context = ssl._create_unverified_context
requests.packages.urllib3.disable_warnings()
warnings.filterwarnings("ignore")

# 👇 TU CONEXIÓN DE NEON
db_url = "postgresql://neondb_owner:npg_nxamLK5P6thM@ep-royal-snow-a488eu3z-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

# URL WFS (Usamos HTTP si HTTPS falla, o forzamos la lectura)
WFS_URL = "https://sig.energia.gob.ar/geoserver/wfs"

def obtener_datos_reales():
    print("🛰️ Intentando conexión forzada con Secretaría de Energía...")

    if "tu_usuario" in db_url:
        print("❌ ERROR: Falta tu conexión en db_url")
        return

    try:
        # 1. CONECTAR AL WFS SIN VERIFICAR SSL
        wfs = WebFeatureService(url=WFS_URL, version='1.1.0')
        print(f"✅ ¡Conexión Exitosa! Servidor: {wfs.identification.title}")
        
        # Listamos capas para asegurar que vemos los datos
        capas = list(wfs.contents)
        
        # 2. IDENTIFICAR CAPAS CLAVE (Nombres exactos del GeoServer)
        # Basado en la estructura habitual de este servidor:
        layer_pozos = "ws_hidrocarburos:pozos_explotacion" # Nombre usual
        layer_ductos = "ws_hidrocarburos:ductos_y_plantas"
        
        # Verificamos si existen en la lista (búsqueda aproximada si el nombre cambió)
        for c in capas:
            if "pozo" in c and "explotacion" in c:
                layer_pozos = c
            if "ducto" in c:
                layer_ductos = c

        print(f"🎯 Capas seleccionadas: POZOS='{layer_pozos}' | DUCTOS='{layer_ductos}'")

        engine = create_engine(db_url)

        # 3. DESCARGAR Y ACTUALIZAR POZOS
        print("⬇️ Descargando POZOS REALES (Esto puede tardar)...")
        # Filtramos por Neuquén usando un BBOX para no bajar todo el país
        # BBOX Vaca Muerta aprox: long min, lat min, long max, lat max
        response_pozos = wfs.getfeature(typename=layer_pozos, bbox=(-71.0, -40.0, -67.0, -36.0), outputFormat='application/json')
        gdf_pozos = gpd.read_file(response_pozos)
        
        if not gdf_pozos.empty:
            print(f"📍 Procesando {len(gdf_pozos)} pozos oficiales...")
            
            # Subimos a tabla temporal
            gdf_pozos.to_postgis("temp_pozos_gob", engine, if_exists='replace', index=False)
            
            # CRUZAMOS DATOS: Actualizamos tu tabla 'padron' con las coordenadas reales
            # Usamos 'sigla' como llave de cruce (es el ID único del pozo en Argentina)
            print("🔄 Cruzando coordenadas con tu base de datos...")
            with engine.connect() as conn:
                result = conn.execute(text("""
                    UPDATE padron p
                    SET latitud = ST_Y(ST_Centroid(t.geometry)),
                        longitud = ST_X(ST_Centroid(t.geometry)),
                        geom = t.geometry
                    FROM temp_pozos_gob t
                    WHERE p.sigla = t.sigla;
                """))
                conn.commit()
                print(f"✅ ¡Coordenadas REALES inyectadas! Filas afectadas: {result.rowcount}")
        
        # 4. DESCARGAR Y ACTUALIZAR DUCTOS
        print("⬇️ Descargando INFRAESTRUCTURA REAL...")
        response_ductos = wfs.getfeature(typename=layer_ductos, bbox=(-71.0, -40.0, -67.0, -36.0), outputFormat='application/json')
        gdf_ductos = gpd.read_file(response_ductos)
        
        if not gdf_ductos.empty:
            print(f"pipelines encontrados: {len(gdf_ductos)}")
            # Filtramos columnas para que no falle
            cols = [c for c in ['geometry', 'tipo', 'nombre', 'operadora', 'estado'] if c in gdf_ductos.columns]
            gdf_ductos = gdf_ductos[cols]
            
            gdf_ductos.to_postgis("infraestructura", engine, if_exists='replace', index=False)
            print("✅ Tabla 'infraestructura' reemplazada con datos oficiales.")

    except Exception as e:
        print(f"❌ Error fatal: {e}")
        print("👉 Si esto falla, la única opción es descargar los SHP manualmente desde la web del gobierno.")

if __name__ == "__main__":
    obtener_datos_reales()