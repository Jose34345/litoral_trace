import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import os

# 👇 IMPORTANTE: PEGÁ TU CONEXIÓN DE NEON AQUÍ (la que empieza con postgresql://...)
db_url = "postgresql://neondb_owner:npg_nxamLK5P6thM@ep-royal-snow-a488eu3z-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

def generar_datos_falsos():
    print("⚠️ MODO EMERGENCIA: Generando datos simulados de DUCs...")
    
    if "tu_usuario" in db_url:
        print("❌ ERROR: Te olvidaste de poner tu link de conexión real en la variable db_url")
        return

    try:
        engine = create_engine(db_url)

        # 1. Lista de empresas reales
        empresas = ['YPF. S.A.', 'PAN AMERICAN ENERGY', 'VISTA', 'SHELL', 'TECPETROL', 'PLUSPETROL', 'TOTAL']
        
        data = []
        
        # 2. Generamos 500 pozos "inventados"
        print("🎲 Fabricando 500 pozos virtuales...")
        for i in range(500):
            empresa = np.random.choice(empresas, p=[0.4, 0.15, 0.15, 0.1, 0.1, 0.05, 0.05])
            
            # Fechas aleatorias del último año
            fecha_fin_perf = pd.Timestamp('2024-01-01') + pd.to_timedelta(np.random.randint(0, 365), unit='D')
            
            # Lógica DUC: El 30% de los pozos NO tiene fecha de inicio (son DUCs)
            es_duc = np.random.rand() < 0.3
            
            if es_duc:
                fecha_ini_prod = None # Es un DUC
            else:
                # Si produce, arrancó entre 30 y 90 días después
                fecha_ini_prod = fecha_fin_perf + pd.to_timedelta(np.random.randint(30, 90), unit='D')
                
            data.append({
                'idpozo': 10000 + i,
                'empresa': empresa,
                'fecha_terminacion_perforacion': fecha_fin_perf,
                'fecha_inicio_produccion': fecha_ini_prod,
                'provincia': 'Neuquén'
            })
            
        df_mock = pd.DataFrame(data)
        
        # 3. Guardar en la base de datos
        print(f"💾 Guardando en tabla 'padron'...")
        df_mock.to_sql('padron', engine, if_exists='replace', index=False)
        print("✅ ¡Listo! Base de datos reparada con datos de prueba.")
        
    except Exception as e:
        print(f"❌ Error conectando a la base: {e}")

if __name__ == "__main__":
    generar_datos_falsos()