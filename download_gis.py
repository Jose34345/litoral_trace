import geopandas as gpd
import pandas as pd
import os

# URLs oficiales del GeoServer de Energía (WFS)
# Estas son las direcciones "traseras" del mapa que viste en la web
URL_WFS_BASE = "https://sig.energia.gob.ar/geoserver/ws_hidrocarburos/ows"

def descargar_capa(nombre_capa, archivo_salida):
    print(f"🌍 Descargando capa: {nombre_capa}...")
    
    params = {
        "service": "WFS",
        "version": "1.0.0",
        "request": "GetFeature",
        "typeName": f"ws_hidrocarburos:{nombre_capa}",
        "outputFormat": "application/json"
    }
    
    # Descargamos usando Geopandas directamente
    try:
        url = f"{URL_WFS_BASE}?service=WFS&version=1.0.0&request=GetFeature&typeName=ws_hidrocarburos:{nombre_capa}&outputFormat=application/json"
        gdf = gpd.read_file(url)
        
        # Filtramos solo Neuquén para no hacer pesado el sistema
        if 'provincia' in gdf.columns:
            gdf = gdf[gdf['provincia'] == 'NEUQUEN']
        
        # Guardamos en un archivo local (GeoJSON) para no depender del servidor del gobierno
        gdf.to_file(archivo_salida, driver='GeoJSON')
        print(f"✅ {nombre_capa} guardada en {archivo_salida} ({len(gdf)} registros)")
        return gdf
    except Exception as e:
        print(f"❌ Error descargando {nombre_capa}: {e}")
        return None

if __name__ == "__main__":
    # 1. Capa de Ductos (Para calcular logística)
    descargar_capa("ductos_y_plantas", "ductos_vaca_muerta.geojson")
    
    # 2. Capa de Concesiones (Para ver las áreas de las empresas)
    descargar_capa("concesiones_explotacion", "bloques_vaca_muerta.geojson")
    
    # 3. Capa de Venteos (Para tu sección ESG)
    descargar_capa("venteo_gas", "venteos_reales.geojson")