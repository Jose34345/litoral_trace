from fastapi import FastAPI, HTTPException
import pandas as pd
from sqlalchemy import create_engine, text
import tomllib  # <--- Librería nativa de Python 3.11+
import os

# --- CONFIGURACIÓN ---
app = FastAPI(
    title="Vaca Muerta Intelligence API",
    version="2.1.0",
    description="Backend oficial conectado a Neon PostgreSQL"
)

# Función para obtener la conexión
def get_db_engine():
    # 1. INTENTO NUBE: Buscar en variables de entorno
    db_url = os.environ.get("DATABASE_URL")
    
    if db_url:
        # 🧹 SANITIZACIÓN DE URGENCIA
        # Esto elimina espacios vacíos y comillas si se colaron en Render
        clean_url = db_url.strip().replace('"', '').replace("'", "")
        return create_engine(clean_url)
    
    # 2. INTENTO LOCAL: Buscar archivo secrets.toml
    try:
        secrets_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".streamlit", "secrets.toml")
        with open(secrets_path, "rb") as f:
            secrets = tomllib.load(f)
        connection_string = secrets["postgres"]["url"]
        return create_engine(connection_string)
    except Exception as e:
        print(f"⚠️ No se encontró configuración local ni de nube: {e}")
        return None

# --- ENDPOINTS ---

@app.get("/")
def home():
    return {"status": "online", "message": "Bienvenido al núcleo de Vaca Muerta Intelligence"}

@app.get("/empresas")
def get_empresas():
    """Devuelve la lista de todas las operadoras disponibles en la base de datos."""
    engine = get_db_engine()
    if not engine:
        raise HTTPException(status_code=500, detail="Error de conexión a Base de Datos")
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT DISTINCT empresa FROM produccion ORDER BY empresa ASC"))
            empresas = [row[0] for row in result]
        return {"total": len(empresas), "data": empresas}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/produccion/{empresa}")
def get_produccion_empresa(empresa: str):
    """Obtiene volumen Y facturación histórica para una empresa."""
    engine = get_db_engine()
    
    query = text("""
        SELECT 
            p.fecha_data, 
            SUM(p.prod_pet) as petroleo, 
            SUM(p.prod_gas) as gas,
            SUM(p.prod_pet * pr.precio_usd) as revenue_usd
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
        return {"error": str(e)}

@app.get("/ducs")
def get_ducs_inventory():
    """
    Calcula el inventario de DUCs (Drilled but Uncompleted).
    Lógica: Pozos con fecha_fin_perforacion NOT NULL y fecha_inicio_produccion NULL.
    """
    engine = get_db_engine()
    
    # Query Inteligente:
    # Filtramos pozos perforados desde 2023 para que sea "inventario reciente" 
    # y no basura de hace 20 años abandonada.
    query = text("""
        SELECT 
            empresa,
            COUNT(*) as duc_count
        FROM padron
        WHERE fecha_terminacion_perforacion >= '2023-01-01'
          AND (fecha_inicio_produccion IS NULL OR fecha_inicio_produccion > CURRENT_DATE)
        GROUP BY empresa
        HAVING COUNT(*) > 0
        ORDER BY duc_count DESC
        LIMIT 10
    """)
    
    try:
        with engine.connect() as conn:
            result = conn.execute(query)
            # Formateamos para el frontend
            data = [{"empresa": row[0], "ducs": row[1]} for row in result]
            
        return data
    except Exception as e:
        # Si la tabla padron no existe aun (porque no corrió el ETL), devolvemos lista vacía
        print(f"Error DUCs: {e}")
        return []

@app.get("/eficiencia/{empresa}")
def get_eficiencia_empresa(empresa: str):
    """Devuelve métricas operativas críticas (Agua y GOR)."""
    engine = get_db_engine()
    
    query = text("""
        SELECT 
            fecha_data,
            SUM(prod_agua) as agua_m3,
            SUM(prod_gas) * 1000 as gas_m3, 
            SUM(prod_pet) as petroleo_m3,
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
        return {"error": str(e)}
    
@app.get("/curvas-tipo/{empresa}")
def get_curvas_tipo(empresa: str):
    """Genera la Curva Tipo (Promedio de pozos) normalizada."""
    engine = get_db_engine()
    
    # 🛡️ QUERY BLINDADA: Agregamos ::DATE para evitar errores de tipo
    query = text("""
        WITH inicio_pozos AS (
            SELECT idpozo, MIN(fecha_data) as fecha_inicio
            FROM produccion
            WHERE empresa = :empresa
            GROUP BY idpozo
        ),
        produccion_normalizada AS (
            SELECT 
                p.idpozo,
                p.prod_pet,
                p.fecha_data,
                -- Calculamos mes relativo asegurando que sean FECHAS (::DATE)
                ((EXTRACT(YEAR FROM p.fecha_data::DATE) - EXTRACT(YEAR FROM i.fecha_inicio::DATE)) * 12 + 
                 (EXTRACT(MONTH FROM p.fecha_data::DATE) - EXTRACT(MONTH FROM i.fecha_inicio::DATE))) as mes_n
            FROM produccion p
            JOIN inicio_pozos i ON p.idpozo = i.idpozo
            WHERE p.empresa = :empresa
        )
        SELECT 
            mes_n,
            AVG(prod_pet) as promedio_petroleo,
            COUNT(DISTINCT idpozo) as cantidad_pozos
        FROM produccion_normalizada
        WHERE mes_n <= 24 
        GROUP BY mes_n
        ORDER BY mes_n ASC;
    """)
    
    try:
        df = pd.read_sql(query, engine, params={"empresa": empresa})
        return df.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}