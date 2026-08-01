#!/bin/bash
# Script de Despliegue Automático en Producción — Litoral Trace (FastAPI)
set -e

echo "=========================================================="
echo "🚀 Iniciando Despliegue de Producción: litoraltrace.com"
echo "=========================================================="

APP_DIR="/opt/litoral_trace"

if [ ! -d "$APP_DIR" ]; then
    echo "📁 Creando directorio de aplicación en $APP_DIR..."
    mkdir -p "$APP_DIR"
fi

cd "$APP_DIR"

echo "🛑 Deteniendo contenedores de producción anteriores..."
docker-compose -f docker-compose.prod.yml down || true

echo "🔨 Reconstruyendo e iniciando contenedores FastAPI + PostGIS + Nginx..."
docker-compose -f docker-compose.prod.yml up -d --build

echo "⏳ Esperando inicio saludable de la base de datos PostgreSQL/PostGIS..."
sleep 5

echo "🏥 Verificando salud de la API FastAPI..."
curl -s -f http://localhost:8000/health || (echo "❌ Error: El servicio FastAPI no respondió en http://localhost:8000/health" && exit 1)

echo "=========================================================="
echo "✅ Despliegue Exitoso. La plataforma B2B está activa en:"
echo "🌐 https://litoraltrace.com"
echo "🌐 https://litoraltrace.com/docs (OpenAPI Documentation)"
echo "=========================================================="
