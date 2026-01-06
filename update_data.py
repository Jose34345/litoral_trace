import pandas as pd
from sqlalchemy import create_engine
import os

# --- URLS ---
# ⚠️ IMPORTANTE: Pegá acá el link nuevo que encontraste
URL_PADRON = "http://datos.energia.gob.ar/dataset/c846e79c-02aa-4036-8086-39766ee99555/resource/cbfa4d79-ffb3-4096-bab5-eb0dde9a8385/download/listado-de-pozos-cargados-por-empresas-operadoras.csv" 

def run_update():
    print("🚀 Iniciando actualización INTELIGENTE...")
    
    # 1. CONEXIÓN (Modo Manual para tu prueba local)
    # db_url = os.environ.get("DATABASE_URL")
    # 👇 DESCOMENTÁ ESTO Y PONÉ TU CLAVE PARA LA PRUEBA LOCAL
    db_url = "postgresql://neondb_owner:npg_nxamLK5P6thM@ep-royal-snow-a488eu3z-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
    
    if not db_url:
        raise ValueError("Falta la conexión a la Base de Datos")
        
    engine = create_engine(db_url)
    
    try:
        # --- A. PROCESAR PADRÓN ---
        print("⬇️ Descargando Padrón (sin filtros)...")
        
        # Leemos TODO el archivo para ver qué columnas trajo realmente
        df_padron = pd.read_csv(URL_PADRON, low_memory=False)
        
        # Normalizamos nombres de columnas a minúsculas para evitar errores por mayúsculas
        df_padron.columns = [c.lower().strip() for c in df_padron.columns]
        
        print(f"👀 Columnas detectadas: {list(df_padron.columns)}")
        
        # DICCIONARIO DE SINÓNIMOS
        # Si el gobierno cambia el nombre, nosotros lo arreglamos acá
        correcciones = {
            'fecha_fin_perforacion': 'fecha_terminacion_perforacion',
            'fecha_fin_perf': 'fecha_terminacion_perforacion',
            'fec_term_perf': 'fecha_terminacion_perforacion',
            'fecha_inicio_prod': 'fecha_inicio_produccion',
            'fec_ini_prod': 'fecha_inicio_produccion'
        }
        
        # Aplicamos correcciones
        df_padron.rename(columns=correcciones, inplace=True)
        
        # Verificamos si ahora sí tenemos las columnas críticas
        if 'fecha_terminacion_perforacion' not in df_padron.columns:
            raise KeyError(f"❌ Sigue faltando la fecha de terminación. Columnas disponibles: {df_padron.columns}")

        # Filtramos solo lo que necesitamos y solo Neuquén
        cols_finales = ['idpozo', 'empresa', 'fecha_terminacion_perforacion', 'fecha_inicio_produccion', 'provincia']
        
        # Nos aseguramos de que existan antes de filtrar
        cols_existentes = [c for c in cols_finales if c in df_padron.columns]
        df_padron = df_padron[cols_existentes]
        
        if 'provincia' in df_padron.columns:
            df_padron = df_padron[df_padron['provincia'] == 'Neuquén']
        
        # Convertimos fechas
        print("🔄 Procesando fechas...")
        df_padron['fecha_terminacion_perforacion'] = pd.to_datetime(df_padron['fecha_terminacion_perforacion'], errors='coerce')
        df_padron['fecha_inicio_produccion'] = pd.to_datetime(df_padron['fecha_inicio_produccion'], errors='coerce')
        
        print(f"💾 Guardando {len(df_padron)} registros en tabla 'padron'...")
        df_padron.to_sql('padron', engine, if_exists='replace', index=False)
        
        print("✅ ÉXITO TOTAL. Tabla creada.")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise e

if __name__ == "__main__":
    run_update()