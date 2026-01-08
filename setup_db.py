import psycopg2

def crear_base_de_datos():
    # DATOS DE CONEXIÓN (Ajusta la contraseña)
    config = {
        "dbname": "postgres",
        "user": "postgres",
        "password": "postgresql://neondb_owner:npg_nxamLK5P6thM@ep-royal-snow-a488eu3z-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require", # <--- PONÉ TU CLAVE
        "host": "localhost",
        "port": "5432"
    }

    try:
        # 1. Conectar a PostgreSQL
        conn = psycopg2.connect(**config)
        cur = conn.cursor()
        
        # 2. Definir la tabla basada en tus columnas reales
        # Usamos nombres profesionales y tipos de datos correctos
        sql_create_table = """
        CREATE TABLE IF NOT EXISTS produccion (
            id SERIAL PRIMARY KEY,
            idpozo INT,
            anio INT,
            mes INT,
            empresa VARCHAR(255),
            sigla VARCHAR(50),
            prod_pet FLOAT,
            prod_gas FLOAT,
            prod_agua FLOAT,
            formacion VARCHAR(100),
            cuenca VARCHAR(100),
            provincia VARCHAR(100),
            tipo_de_recurso VARCHAR(100),
            fecha_data DATE
        );
        """
        
        cur.execute(sql_create_table)
        conn.commit()
        
        print("✅ ¡Tabla 'produccion' creada exitosamente en PostgreSQL!")
        
        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    crear_base_de_datos()