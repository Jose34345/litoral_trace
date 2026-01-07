import os
from sqlalchemy import create_engine, text, inspect

# 👇 PEGÁ TU CONEXIÓN DE NEON AQUÍ
db_url = "postgresql://neondb_owner:npg_nxamLK5P6thM@ep-royal-snow-a488eu3z-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

def reparar_venteo_inteligente():
    print("🕵️‍♂️ Iniciando diagnóstico de tabla 'produccion'...")
    
    if "tu_usuario" in db_url:
        print("❌ ERROR: Falta tu cadena de conexión en la variable db_url")
        return

    try:
        engine = create_engine(db_url)
        inspector = inspect(engine)
        
        # 1. Obtener nombres reales de las columnas
        columns = [col['name'] for col in inspector.get_columns('produccion')]
        print(f"📋 Columnas detectadas: {columns}")
        
        # 2. Buscar cuál es la columna de gas
        columna_gas = None
        posibles_nombres = ['prod_gas', 'gas_prod', 'gas', 'prod_gas_m3', 'gas_m3']
        
        for nombre in posibles_nombres:
            if nombre in columns:
                columna_gas = nombre
                break
        
        if not columna_gas:
            print("❌ No encontré ninguna columna que parezca de gas. ¿Podrás enviarme la lista de columnas que se imprimió arriba?")
            return

        print(f"✅ Columna de gas encontrada: '{columna_gas}'")

        with engine.connect() as conn:
            # 3. Asegurar que la columna destino (gas_venteo) existe
            print("🛠️ Verificando columna 'gas_venteo'...")
            conn.execute(text("ALTER TABLE produccion ADD COLUMN IF NOT EXISTS gas_venteo FLOAT;"))
            conn.commit()
            
            # 4. Inyectar datos usando el nombre correcto de la columna
            print(f"💉 Inyectando datos simulados usando '{columna_gas}'...")
            
            # Usamos f-string para insertar el nombre de columna detectado
            query_fill = text(f"""
                UPDATE produccion 
                SET gas_venteo = {columna_gas} * (random() * 0.04 + 0.005) 
                WHERE gas_venteo IS NULL OR gas_venteo = 0;
            """)
            
            conn.execute(query_fill)
            conn.commit()
            
            print("✨ ¡Reparación completada con éxito!")
            print("🔄 Recargá tu Dashboard (F5) para ver los colores.")

    except Exception as e:
        print(f"❌ Error inesperado: {e}")

if __name__ == "__main__":
    reparar_venteo_inteligente()