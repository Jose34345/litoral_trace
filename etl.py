import pandas as pd
from sqlalchemy import create_engine

def cargar_datos_a_sql():
    print("🚀 Iniciando proceso ETL (Extracción, Transformación y Carga)...")
    
    # 1. CONEXIÓN A LA BASE DE DATOS
    # Formato: postgresql://usuario:password@host:puerto/nombre_db
    # REEMPLAZA 'TU_CONTRASEÑA_AQUÍ' CON TU CLAVE REAL
    engine = create_engine('postgresql://neondb_owner:npg_nxamLK5P6thM@ep-royal-snow-a488eu3z-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require')

    try:
        # 2. LEER EL CSV (EXTRACCIÓN)
        print("📂 Leyendo archivo CSV...")
        df = pd.read_csv(
            "produccion_vaca_muerta.csv",
            sep=',',            # Usamos coma porque vimos que era el correcto
            encoding='utf-8',   # O 'latin-1' si te da error de caracteres raros
            low_memory=False
        )

        # 3. LIMPIEZA Y SELECCIÓN (TRANSFORMACIÓN)
        print("🧹 Limpiando y seleccionando columnas clave...")
        
        # Normalizamos nombres de columnas del CSV a minúsculas
        df.columns = df.columns.str.strip().str.lower()
        
        # Seleccionamos solo las columnas que definimos en la tabla SQL
        # Mapeo: Columna_SQL = Columna_CSV
        columnas_a_cargar = [
            'idpozo', 'anio', 'mes', 'empresa', 'sigla', 
            'prod_pet', 'prod_gas', 'prod_agua', 
            'formacion', 'cuenca', 'provincia', 
            'tipo_de_recurso', 'fecha_data'
        ]
        
        # Filtramos el DataFrame para tener solo esas columnas
        # Usamos intersection para evitar errores si falta alguna no vital
        cols_existentes = [c for c in columnas_a_cargar if c in df.columns]
        df_final = df[cols_existentes].copy()

        # Aseguramos que fecha_data sea fecha real para SQL
        if 'fecha_data' in df_final.columns:
            df_final['fecha_data'] = pd.to_datetime(df_final['fecha_data'])

        # 4. CARGA A POSTGRESQL (LOAD)
        print(f"floppy_disk Guardando {len(df_final)} registros en la Base de Datos...")
        
        # if_exists='append': Agrega los datos a la tabla que ya creaste
        # chunksize=10000: Sube de a 10 mil filas para no ahogar la memoria
        df_final.to_sql(
            'produccion', 
            engine, 
            if_exists='append', 
            index=False, 
            chunksize=10000
        )
        
        print("✅ ¡ÉXITO! Datos cargados correctamente en PostgreSQL.")

    except Exception as e:
        print(f"❌ Error en el proceso: {e}")

if __name__ == "__main__":
    cargar_datos_a_sql()