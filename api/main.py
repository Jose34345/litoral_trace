from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from sqlalchemy import create_engine, text
import os

# --- CONFIGURACIÓN ---
app = FastAPI(
    title="Vaca Muerta Intelligence API",
    version="2.2.0",
    description="Backend oficial con soporte GIS y Financiero"
)

# 1. CONFIGURACIÓN CORS (CRÍTICO: Sin esto, el Dashboard no conecta)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permite conexiones desde cualquier lugar (Streamlit Cloud, Local)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Función para obtener la conexión
def get_db_engine():
    # Prioridad 1: Variable de Entorno (Render/Nube)
    db_url = os.environ.get("DATABASE_URL")
    
    # Prioridad 2: Hardcode de emergencia (Tu string de conexión)
    # Si todo falla, poné tu link real acá abajo entre las comillas
    if not db_url:
        db_url = "postgresql://neondb_owner:npg_nxamLK5P6thM@ep-royal-snow-a488eu3z-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
    
    if db_url:
        clean_url = db_url.strip().replace('"', '').replace("'", "")
        return create_engine(clean_url)
    
    return None

# --- ENDPOINTS ---

@app.get("/")
def home():
    return {"status": "online", "message": "Vaca Muerta Intelligence API Operativa 🟢"}

@app.get("/empresas")
def get_empresas():
    engine = get_db_engine()
    if not engine:
        raise HTTPException(status_code=500, detail="Error de conexión a DB")
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT DISTINCT empresa FROM produccion ORDER BY empresa ASC"))
            empresas = [row[0] for row in result]
        return {"total": len(empresas), "data": empresas}
    except Exception as e:
        print(f"Error Empresas: {e}")
        return {"data": []}

@app.get("/produccion/{empresa}")
def get_produccion_empresa(empresa: str):
    engine = get_db_engine()
    # Query optimizada con ingresos
    query = text("""
        SELECT 
            p.fecha_data, 
            SUM(p.prod_pet) as petroleo, 
            SUM(p.prod_gas) as gas,
            SUM(p.prod_pet * COALESCE(pr.precio_usd, 70)) as revenue_usd 
        FROM produccion p
        LEFT JOIN precios_brent pr ON p.anio = pr.anio AND p.mes = pr.mes
        WHERE p.empresa = :empresa
        GROUP BY p.fecha_data
        ORDER BY p.fecha_data ASC
    """)
    try:
        df = pd.read_sql(query, engine, params={"empresa": empresa})
        if not df.empty:
            df['fecha_data'] = df['fecha_data'].astype(str)
        return df.to_dict(orient="records")
    except Exception as e:
        print(f"Error Produccion: {e}")
        return []

@app.get("/ducs")
def get_ducs_inventory():
    """
    Endpoint Inteligente: Intenta traer datos GIS (Mapa). 
    Si falla (porque no corriste los scripts), trae solo la lista simple.
    """
    engine = get_db_engine()
    
    # INTENTO 1: Query Completa (Con Mapa y Costos)
    query_full = text("""
        SELECT 
            empresa,
            COUNT(*) as ducs,
            AVG(latitud) as latitud,
            AVG(longitud) as longitud,
            AVG(distancia_ducto_km) as distancia_ducto_km,
            AVG(capex_conexion_usd) as capex_conexion_usd
        FROM padron
        WHERE fecha_terminacion_perforacion >= '2023-01-01'
          AND (fecha_inicio_produccion IS NULL OR fecha_inicio_produccion > CURRENT_DATE)
          AND latitud IS NOT NULL
        GROUP BY empresa
        HAVING COUNT(*) > 0
        ORDER BY ducs DESC
        LIMIT 50
    """)
    
    # INTENTO 2: Fallback (Solo conteo, por si falla el mapa)
    query_simple = text("""
        SELECT 
            empresa,
            COUNT(*) as ducs
        FROM padron
        WHERE fecha_terminacion_perforacion >= '2023-01-01'
          AND (fecha_inicio_produccion IS NULL OR fecha_inicio_produccion > CURRENT_DATE)
        GROUP BY empresa
        HAVING COUNT(*) > 0
        ORDER BY count(*) DESC
        LIMIT 10
    """)
    
    try:
        with engine.connect() as conn:
            try:
                # Probamos la query con mapa
                result = conn.execute(query_full)
                data = [dict(row._mapping) for row in result]
                return data
            except Exception as e:
                print(f"⚠️ Aviso GIS: No se encontraron columnas de mapa. Usando modo simple. Error: {e}")
                # Si falla, usamos la simple
                result = conn.execute(query_simple)
                data = [{"empresa": row[0], "ducs": row[1]} for row in result]
                return data
    except Exception as e:
        print(f"❌ Error Crítico DUCs: {e}")
        return []

@app.get("/eficiencia/{empresa}")
def get_eficiencia_empresa(empresa: str):
    engine = get_db_engine()
    query = text("""
        SELECT 
            fecha_data,
            SUM(prod_agua) as agua_m3,
            CASE 
                WHEN SUM(prod_pet) > 0 THEN (SUM(prod_gas) * 1000) / SUM(prod_pet) 
                ELSE 0 
            END as gor_promedio
        FROM produccion
        WHERE empresa = :empresa
        GROUP BY fecha_data
        ORDER BY fecha_data ASC
    """)
    try:
        df = pd.read_sql(query, engine, params={"empresa": empresa})
        if not df.empty:
            df['fecha_data'] = df['fecha_data'].astype(str)
        return df.to_dict(orient="records")
    except Exception as e:
        return []

@app.get("/curvas-tipo/{empresa}")
def get_curvas_tipo(empresa: str):
    engine = get_db_engine()
    query = text("""
        WITH inicio_pozos AS (
            SELECT idpozo, MIN(fecha_data) as fecha_inicio
            FROM produccion WHERE empresa = :empresa GROUP BY idpozo
        ),
        produccion_normalizada AS (
            SELECT 
                p.prod_pet,
                ((EXTRACT(YEAR FROM p.fecha_data::DATE) - EXTRACT(YEAR FROM i.fecha_inicio::DATE)) * 12 + 
                 (EXTRACT(MONTH FROM p.fecha_data::DATE) - EXTRACT(MONTH FROM i.fecha_inicio::DATE))) as mes_n
            FROM produccion p
            JOIN inicio_pozos i ON p.idpozo = i.idpozo
            WHERE p.empresa = :empresa
        )
        SELECT mes_n, AVG(prod_pet) as promedio_petroleo
        FROM produccion_normalizada
        WHERE mes_n BETWEEN 0 AND 24
        GROUP BY mes_n ORDER BY mes_n ASC
    """)
    try:
        df = pd.read_sql(query, engine, params={"empresa": empresa})
        return df.to_dict(orient="records")
    except Exception as e:
        return []

@app.get("/venteo")
def get_venteo_kpi():
    engine = get_db_engine()
    # Usamos prod_gas y gas_venteo
    query = text("""
        SELECT 
            empresa,
            SUM(prod_gas) as total_gas_prod,
            SUM(gas_venteo) as total_gas_venteo
        FROM produccion
        WHERE fecha_data >= '2023-01-01'
        GROUP BY empresa
        HAVING SUM(prod_gas) > 1000000
        ORDER BY total_gas_prod DESC
    """)
    try:
        with engine.connect() as conn:
            result = conn.execute(query)
            data = []
            for row in result:
                prod = row[1] or 0
                venteo = row[2] or 0
                total = prod + venteo
                ratio = (venteo / total * 100) if total > 0 else 0
                data.append({
                    "empresa": row[0],
                    "ratio_venteo": round(ratio, 2),
                    "volumen_venteado": venteo
                })
            data.sort(key=lambda x: x['ratio_venteo'], reverse=True)
        return data[:10]
    except Exception as e:
        return []