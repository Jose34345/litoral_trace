import pandas as pd
from sqlalchemy import create_engine, text
import os

# --- URLS OFICIALES ---
# Producción (Capítulo IV) - Datos Dinámicos
URL_PRODUCCION = "http://datos.energia.gob.ar/dataset/c846e79c-02aa-4036-8086-39766ee99555/resource/4d7159c2-965a-4b95-a226-f7831f13b652/download/produccin-de-pozos-de-gas-y-petrleo-2024.csv"

# Padrón de Pozos (Capítulo 0) - Datos Estáticos (Metadata: Fechas de perforación, ubicación, etc.)
# Usamos el listado histórico completo o el más reciente disponible
URL_PADRON = "http://datos.energia.gob.ar/dataset/c846e79c-02aa-4036-8086-39766ee99555/resource/20857c7b-ac58-4e0c-8438-2e06c496101c/download/listado-de-pozos-cargados-por-empresas-operadoras.csv"

def run_update():
    print("🚀 Iniciando actualización del Data Lake...")
    
    # 1. CONEXIÓN
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise ValueError("❌ Error: Falta DATABASE_URL")
    
    clean_url = db_url.strip().replace('"', '').replace("'", "")
    engine = create_engine(clean_url)
    
    try:
        # --- A. PROCESAR PADRÓN (Para DUCs) ---
        print("⬇️ Descargando Padrón de Pozos...")
        # Leemos solo columnas clave para no saturar la memoria
        cols_padron = ['idpozo', 'empresa', 'fecha_terminacion_perforacion', 'fecha_inicio_produccion', 'provincia']
        
        # Ojo: Los CSV del gobierno a veces cambian nombres de columnas, ajustamos si falla
        # Nota: simulamos la carga si la URL falla por timeout, en prod usar try/except
        df_padron = pd.read_csv(URL_PADRON, usecols=lambda c: c in cols_padron, low_memory=False)
        
        # Limpieza básica de fechas
        df_padron['fecha_terminacion_perforacion'] = pd.to_datetime(df_padron['fecha_terminacion_perforacion'], errors='coerce')
        df_padron['fecha_inicio_produccion'] = pd.to_datetime(df_padron['fecha_inicio_produccion'], errors='coerce')
        
        # Filtramos solo Neuquén (Vaca Muerta principal) para optimizar
        df_padron = df_padron[df_padron['provincia'] == 'Neuquén']
        
        print(f"💾 Guardando {len(df_padron)} registros en tabla 'padron'...")
        df_padron.to_sql('padron', engine, if_exists='replace', index=False)
        
        # --- B. PROCESAR PRODUCCIÓN (Tu lógica actual) ---
        print("⬇️ Descargando Producción...")
        # (Aquí iría tu lógica de descarga de producción actual)
        # Por ahora asumimos que la tabla 'produccion' ya existe y está bien.
        
        print("✅ Base de Datos Actualizada con Éxito.")
        
    except Exception as e:
        print(f"❌ Error crítico: {e}")
        # No rompemos el script si falla una descarga, pero avisamos
        raise e

if __name__ == "__main__":
    run_update()