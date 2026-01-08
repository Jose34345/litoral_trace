import sqlalchemy
from sqlalchemy import create_engine, text

# URL de tu base de datos Neon
db_url = "postgresql://neondb_owner:npg_nxamLK5P6thM@ep-royal-snow-a488eu3z-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require"
engine = create_engine(db_url)

def normalize():
    # Diccionario de mapeo: "Nombre Sucio": "Nombre Limpio"
    mapping = {
        "YPF. S.A.": "YPF",
        "YPF S.A.": "YPF",
        "VISTA ENERGY ARGENTINA SAU": "VISTA",
        "VISTA ENERGY": "VISTA",
        "PAN AMERICAN ENERGY SL": "PAE",
        "PAN AMERICAN ENERGY": "PAE",
        "SHELL ARGENTINA": "SHELL",
        "TOTAL AUSTRAL": "TOTAL",
        "TOTAL ENERGIES": "TOTAL",
        "TECPETROL S.A.": "TECPETROL",
        "PLUSPETROL S.A.": "PLUSPETROL"
    }

    print("🧼 Iniciando normalización de nombres...")
    
    with engine.connect() as conn:
        for old_name, new_name in mapping.items():
            # Actualizar en tabla produccion
            conn.execute(text(
                "UPDATE produccion SET empresa = :new WHERE empresa = :old"
            ), {"new": new_name, "old": old_name})
            
            # Actualizar en tabla padron
            conn.execute(text(
                "UPDATE padron SET empresa = :new WHERE empresa = :old"
            ), {"new": new_name, "old": old_name})
            
        conn.commit()
    print("✅ ¡Base de datos normalizada con éxito!")

if __name__ == "__main__":
    normalize()