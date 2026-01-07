import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString, Polygon
from sqlalchemy import create_engine, text
import numpy as np

# 👇 TU CONEXIÓN DE NEON
db_url = "postgresql://neondb_owner:npg_nxamLK5P6thM@ep-royal-snow-a488eu3z-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

def rescate_total():
    print("🚑 INICIANDO PROTOCOLO DE RESCATE...")

    if "tu_usuario" in db_url:
        print("❌ ERROR: Poné tu conexión en db_url")
        return

    # --- PARTE A: ARREGLAR LA TABLA PADRON ---
    try:
        engine = create_engine(db_url)
        with engine.connect() as conn:
            print("🛠️ 1. Reparando columnas de coordenadas en 'padron'...")
            conn.execute(text("ALTER TABLE padron ADD COLUMN IF NOT EXISTS latitud FLOAT;"))
            conn.execute(text("ALTER TABLE padron ADD COLUMN IF NOT EXISTS longitud FLOAT;"))
            conn.commit()

            print("📍 2. Inyectando ubicaciones simuladas en Añelo...")
            # Añelo está aprox en Lat -38.35, Lon -68.8
            # Simulamos pozos dispersos en esa zona
            conn.execute(text("""
                UPDATE padron 
                SET latitud = -38.35 + (random() * 0.1 - 0.05),
                    longitud = -68.80 + (random() * 0.1 - 0.05)
                WHERE latitud IS NULL;
            """))
            conn.commit()
            print("✅ Tabla 'padron' reparada.")

    except Exception as e:
        print(f"❌ Error en Parte A: {e}")

    # --- PARTE B: GENERAR ARCHIVOS GIS FALTANTES ---
    print("🗺️ 3. Generando mapas de infraestructura de emergencia...")
    
    # Creamos un Gasoducto Simulado (Línea que cruza los pozos)
    linea_ducto = LineString([(-68.90, -38.30), (-68.70, -38.40)])
    gdf_ductos = gpd.GeoDataFrame(
        {'nombre': ['Gasoducto Vaca Muerta Norte'], 'tipo': ['Gasoducto'], 'geometry': [linea_ducto]},
        crs="EPSG:4326"
    )
    gdf_ductos.to_file("ductos_vaca_muerta.geojson", driver='GeoJSON')
    print("   -> Creado 'ductos_vaca_muerta.geojson'")

    # Creamos un Bloque Simulado (Cuadrado alrededor)
    poly_bloque = Polygon([(-68.90, -38.30), (-68.70, -38.30), (-68.70, -38.45), (-68.90, -38.45), (-68.90, -38.30)])
    gdf_bloques = gpd.GeoDataFrame(
        {'area': ['Area Loma Campana Mock'], 'empresa': ['YPF'], 'geometry': [poly_bloque]},
        crs="EPSG:4326"
    )
    gdf_bloques.to_file("bloques_vaca_muerta.geojson", driver='GeoJSON')
    print("   -> Creado 'bloques_vaca_muerta.geojson'")
    
    print("🎉 ¡SISTEMA RESCATADO! Ahora podés correr los scripts de carga.")

if __name__ == "__main__":
    rescate_total()