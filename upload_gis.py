import geopandas as gpd
from sqlalchemy import create_engine, text
import os

# 👇 TU CONEXIÓN (Pegala igual que en los scripts anteriores)
db_url = "postgresql://neondb_owner:npg_nxamLK5P6thM@ep-royal-snow-a488eu3z-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

def subir_datos_espaciales():
    print("🚀 Iniciando carga de datos GIS a Neon...")
    
    if "tu_usuario" in db_url:
        print("❌ ERROR: Te olvidaste de poner tu link de conexión real.")
        return

    try:
        # 1. Conexión y Activación de PostGIS
        engine = create_engine(db_url)
        with engine.connect() as conn:
            print("🔧 Activando extensión PostGIS en la base de datos...")
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
            conn.commit()
            print("✅ PostGIS activo.")

        # 2. Subir Capa de DUCTOS
        archivo_ductos = "ductos_vaca_muerta.geojson"
        if os.path.exists(archivo_ductos):
            print(f"📦 Leyendo {archivo_ductos}...")
            gdf_ductos = gpd.read_file(archivo_ductos)
            
            # Limpieza: A veces las columnas de fecha o object dan error, las convertimos a string
            # Filtramos solo columnas útiles para no llenar la DB de basura
            cols_utiles = ['tipo', 'nombre', 'operadora', 'estado', 'geometry']
            # Nos aseguramos de que existan antes de filtrar
            cols_a_subir = [c for c in cols_utiles if c in gdf_ductos.columns]
            
            gdf_ductos = gdf_ductos[cols_a_subir]

            print("⬆️ Subiendo tabla 'infraestructura'...")
            gdf_ductos.to_postgis("infraestructura", engine, if_exists='replace', index=False)
            print("✅ Ductos cargados correctamente.")
        else:
            print(f"⚠️ No encontré el archivo {archivo_ductos}. Saltando...")

        # 3. Subir Capa de BLOQUES (Concesiones)
        archivo_bloques = "bloques_vaca_muerta.geojson"
        if os.path.exists(archivo_bloques):
            print(f"📦 Leyendo {archivo_bloques}...")
            gdf_bloques = gpd.read_file(archivo_bloques)
            
            # Limpieza básica
            cols_utiles_bloq = ['area', 'empresa', 'yacimiento', 'geometry']
            cols_a_subir_bloq = [c for c in cols_utiles_bloq if c in gdf_bloques.columns]
            
            if cols_a_subir_bloq:
                gdf_bloques = gdf_bloques[cols_a_subir_bloq]

            print("⬆️ Subiendo tabla 'bloques'...")
            gdf_bloques.to_postgis("bloques", engine, if_exists='replace', index=False)
            print("✅ Bloques cargados correctamente.")
        else:
            print(f"⚠️ No encontré el archivo {archivo_bloques}.")

        print("🎉 ¡Fase 2 Completada! Tu base de datos ahora es Espacial.")

    except Exception as e:
        print(f"❌ Error crítico: {e}")

if __name__ == "__main__":
    subir_datos_espaciales()