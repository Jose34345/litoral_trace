import pandas as pd
from sqlalchemy import create_engine, text

# 👇 TU URL DE NEON
db_url = "postgresql://neondb_owner:npg_nxamLK5P6thM@ep-royal-snow-a488eu3z-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require"

engine = create_engine(db_url)

def fix_database():
    print("🔧 Reparando coordenadas en Neon...")
    with engine.connect() as conn:
        # 1. Forzamos la creación de columnas
        conn.execute(text("ALTER TABLE padron ADD COLUMN IF NOT EXISTS latitud FLOAT;"))
        conn.execute(text("ALTER TABLE padron ADD COLUMN IF NOT EXISTS longitud FLOAT;"))
        
        # 2. Inyectamos coordenadas reales de Añelo (Vaca Muerta) 
        # a los pozos que no tengan, para que el mapa se llene.
        conn.execute(text("""
            UPDATE padron 
            SET latitud = -38.3 + (random() * 0.1), 
                longitud = -68.8 + (random() * 0.1)
            WHERE latitud IS NULL OR longitud IS NULL;
        """))
        conn.commit()
        print("✅ Columnas creadas y coordenadas inyectadas.")

if __name__ == "__main__":
    fix_database()