import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine

def actualizar_precios_brent():
    print("💰 Consultando mercados internacionales (Yahoo Finance)...")
    
    # 1. Descargar datos históricos del Brent (Símbolo: BZ=F)
    brent = yf.Ticker("BZ=F")
    # Traemos 5 años para cubrir todo tu historial
    hist = brent.history(period="5y")
    
    # 2. Resamplear a Promedio Mensual
    # 'MS' significa "Month Start" (Primer día del mes)
    precios_mensuales = hist['Close'].resample('MS').mean().reset_index()
    
    # 3. Preparar DataFrame para SQL
    df_sql = pd.DataFrame()
    df_sql['anio'] = precios_mensuales['Date'].dt.year
    df_sql['mes'] = precios_mensuales['Date'].dt.month
    df_sql['precio_usd'] = precios_mensuales['Close'].round(2)
    
    print(f"📉 Se calcularon {len(df_sql)} meses de precios promedio.")

    # 4. Guardar en PostgreSQL
    # REEMPLAZA CON TU CONTRASEÑA
    engine = create_engine('postgresql://neondb_owner:npg_nxamLK5P6thM@ep-royal-snow-a488eu3z-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require')
    
    try:
        df_sql.to_sql('precios_brent', engine, if_exists='replace', index=False)
        print("✅ ¡Tabla 'precios_brent' actualizada con éxito!")
        print(df_sql.tail(3)) # Muestra los últimos 3 meses para verificar
    except Exception as e:
        print(f"❌ Error guardando precios: {e}")

if __name__ == "__main__":
    actualizar_precios_brent()