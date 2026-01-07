from sqlalchemy import create_engine, text

# 👇 TU CONEXIÓN DE NEON
db_url = "postgresql://neondb_owner:npg_nxamLK5P6thM@ep-royal-snow-a488eu3z-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

def calcular_logistica():
    print("🧮 Iniciando Motor de Cálculo Logístico...")
    
    if "tu_usuario" in db_url:
        print("❌ ERROR: Te olvidaste de poner tu link de conexión.")
        return

    engine = create_engine(db_url)
    with engine.connect() as conn:
        # 1. PREPARACIÓN: Crear columnas geométricas en la tabla 'padron'
        print("📍 Creando geometría para los pozos...")
        # Agregamos columna 'geom' si no existe
        conn.execute(text("ALTER TABLE padron ADD COLUMN IF NOT EXISTS geom geometry(Point, 4326);"))
        conn.execute(text("ALTER TABLE padron ADD COLUMN IF NOT EXISTS distancia_ducto_km FLOAT;"))
        conn.execute(text("ALTER TABLE padron ADD COLUMN IF NOT EXISTS capex_conexion_usd FLOAT;"))
        conn.commit()

        # Llenamos la columna 'geom' usando latitud y longitud
        print("📍 Georeferenciando activos...")
        conn.execute(text("""
            UPDATE padron 
            SET geom = ST_SetSRID(ST_MakePoint(longitud, latitud), 4326)
            WHERE latitud IS NOT NULL AND geom IS NULL;
        """))
        conn.commit()

        # 2. CÁLCULO ESPACIAL: Distancia al ducto más cercano
        # Usamos el operador <-> de PostGIS (Nearest Neighbor) que es rapidísimo.
        # ST_Distance devuelve grados, así que usamos ::geography para obtener METROS.
        print("📏 Midiendo distancias a infraestructura (esto puede tardar unos segundos)...")
        
        query_distancia = text("""
            UPDATE padron p
            SET distancia_ducto_km = (
                SELECT ST_Distance(p.geom::geography, i.geometry::geography) / 1000.0
                FROM infraestructura i
                ORDER BY p.geom <-> i.geometry
                LIMIT 1
            )
            WHERE p.geom IS NOT NULL;
        """)
        conn.execute(query_distancia)
        conn.commit()

        # 3. VALUACIÓN ECONÓMICA
        # Asumimos USD 60.000 por km de flowline
        print("💰 Calculando CAPEX de conexión...")
        conn.execute(text("""
            UPDATE padron
            SET capex_conexion_usd = distancia_ducto_km * 60000
            WHERE distancia_ducto_km IS NOT NULL;
        """))
        conn.commit()
        
        # Verificación rápida
        result = conn.execute(text("SELECT AVG(capex_conexion_usd) FROM padron WHERE capex_conexion_usd > 0"))
        avg_capex = result.scalar()
        print(f"✅ ¡Cálculo Terminado! Costo promedio de conexión estimado: USD {avg_capex:,.2f}")

if __name__ == "__main__":
    calcular_logistica()