from owslib.wfs import WebFeatureService
import geopandas as gpd
from sqlalchemy import create_engine, text
import pandas as pd
import warnings

# Ignoramos alertas de versiones de WFS
warnings.filterwarnings("ignore")

# 👇 TU CONEXIÓN DE NEON (Pegala aquí)
db_url = "postgresql://neondb_owner:npg_nxamLK5P6thM@ep-royal-snow-a488eu3z-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

# URL OFICIAL DE WFS (Extraída de la doc que pasaste)
# Usualmente es esta para el SIG de Energía:
WFS_URL = "https://sig.energia.gob.ar/geoserver/wfs"

def sincronizar_datos_oficiales():
    print("🛰️ Conectando con IDE Energía (Secretaría de Energía)...")
    
    if "tu_usuario" in db_url:
        print("❌ ERROR: Falta tu conexión en db_url")
        return

    try:
        # 1. CONEXIÓN AL SERVIDOR GUBERNAMENTAL
        wfs = WebFeatureService(url=WFS_URL, version='1.1.0')
        print(f"✅ Conexión establecida. Título: {wfs.identification.title}")
        
        # 2. LISTAR CAPAS DISPONIBLES
        # Buscamos las que nos interesan
        capas_disponibles = list(wfs.contents)
        print(f"🔎 Analizando {len(capas_disponibles)} capas disponibles...")
        
        # Diccionario de búsqueda (Palabra clave : Nombre probable en servidor)
        objetivos = {
            "pozos": None,
            "ductos": None,
            "areas": None
        }

        for capa in capas_disponibles:
            nombre_lower = capa.lower()
            if "pozo" in nombre_lower and "exploracion" not in nombre_lower: # Queremos explotación
                objetivos["pozos"] = capa
            elif "ducto" in nombre_lower and "existente" in nombre_lower:
                objetivos["ductos"] = capa
            elif "concesion" in nombre_lower and "explotacion" in nombre_lower:
                objetivos["areas"] = capa

        print(f"🎯 Capas detectadas: {objetivos}")

        # 3. DESCARGAR Y SUBIR A NEON
        engine = create_engine(db_url)
        
        # --- PROCESAR DUCTOS ---
        if objetivos["ductos"]:
            print(f"⬇️ Descargando Infraestructura Real ({objetivos['ductos']})...")
            # Descargamos solo Neuquén (bbox aproximado) para no tardar años
            bbox_vm = (-71.0, -40.0, -67.0, -36.0) 
            
            try:
                response = wfs.getfeature(typename=objetivos["ductos"], bbox=bbox_vm, outputFormat='application/json')
                gdf_ductos = gpd.read_file(response)
                
                # Subir a DB
                if not gdf_ductos.empty:
                    print("⬆️ Subiendo ductos a Neon...")
                    # Filtramos columnas complejas para evitar errores
                    gdf_ductos = gdf_ductos[['geometry', 'tipo', 'nombre', 'operadora', 'estado']]
                    gdf_ductos.to_postgis("infraestructura", engine, if_exists='replace', index=False)
                    print("✅ Ductos actualizados.")
            except Exception as e:
                print(f"⚠️ Error descargando ductos (puede ser muy pesado): {e}")

        # --- PROCESAR POZOS (Georeferenciar Padrón) ---
        # Esto es lo más valioso: Cruzar tu tabla 'padron' con las coordenadas reales
        if objetivos["pozos"]:
            print(f"⬇️ Descargando Coordenadas de Pozos ({objetivos['pozos']})...")
            try:
                response = wfs.getfeature(typename=objetivos["pozos"], bbox=bbox_vm, outputFormat='application/json')
                gdf_pozos = gpd.read_file(response)
                
                if not gdf_pozos.empty:
                    print(f"📍 Inyectando {len(gdf_pozos)} coordenadas reales a tu padrón...")
                    # Subimos a una tabla temporal para cruzar
                    gdf_pozos = gdf_pozos[['geometry', 'id_pozo', 'sigla']] # Ajustar nombres según venga
                    gdf_pozos.to_postgis("temp_pozos_gis", engine, if_exists='replace', index=False)
                    
                    with engine.connect() as conn:
                        # Cruzamos por SIGLA (que suele ser el ID común)
                        conn.execute(text("""
                            UPDATE padron p
                            SET latitud = ST_Y(t.geometry),
                                longitud = ST_X(t.geometry),
                                geom = t.geometry
                            FROM temp_pozos_gis t
                            WHERE p.sigla = t.sigla;
                        """))
                        conn.commit()
                        print("✅ ¡Padrón georeferenciado con datos del Gobierno!")
            except Exception as e:
                print(f"⚠️ Error cruzando pozos: {e}")

    except Exception as e:
        print(f"❌ Error de conexión con IDE Energía: {e}")
        print("💡 Consejo: Los servidores del gobierno a veces se caen. Si falla, usá el 'rescue_system.py' para simular y presentar.")

if __name__ == "__main__":
    sincronizar_datos_oficiales()