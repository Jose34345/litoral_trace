# Manual de Despliegue en Producción (Deployment Runbook) — litoraltrace.com

Guía completa para actualizar el servidor VPS en la nube y transmitir la nueva plataforma FastAPI B2B para **Litoral Trace**.

---

## 1. Acceso al Servidor VPS vía SSH
Abre la terminal en tu computadora y conéctate a tu servidor VPS en la nube:
```bash
ssh root@IP_DE_TU_SERVIDOR
```

---

## 2. Transferencia y Extracción del Nuevo Paquete
Desde tu servidor VPS, descarga e instala la versión más reciente:
```bash
cd /opt
rm -rf /opt/litoral_trace_old
mv /opt/litoral_trace /opt/litoral_trace_old 2>/dev/null || true

# Descargar e descomprimir el nuevo paquete
curl -L -o litoral_trace_fastapi.zip "https://drive.google.com/uc?export=download&id=1Nlwg9tK87I52-tCeHAWLOfVQzoy52XtR"
unzip litoral_trace_fastapi.zip -d /opt/
cd /opt/litoral_trace
```

---

## 3. Configuración de Credenciales de Producción (`.env` / `.streamlit/secrets.toml`)
Asegúrate de tener configuradas las credenciales del servidor:
```bash
mkdir -p .streamlit
cat << 'SECRETS' > .streamlit/secrets.toml
DB_URL = "postgresql://litoral_user:litoral_secure_pass@db:5432/litoral_trace_db"

[gcp_service_account]
type = "service_account"
project_id = "litoral-trace-prod"
private_key_id = "PROD_KEY_ID"
private_key = "-----BEGIN PRIVATE KEY-----\n..."
client_email = "earthengine-sa@litoral-trace-prod.iam.gserviceaccount.com"
SECRETS
```

---

## 4. Ejecución del Script de Despliegue Automático
```bash
chmod +x deploy_production.sh
./deploy_production.sh
```

---

## 5. Configuración del Certificado SSL con Let's Encrypt (HTTPS)
Si es la primera vez que configuras SSL para `litoraltrace.com`:
```bash
sudo apt update && sudo apt install -y certbot
sudo certbot certonly --standalone -d litoraltrace.com -d www.litoraltrace.com

# Copiar certificados a Nginx
mkdir -p ./nginx/certs
sudo cp /etc/letsencrypt/live/litoraltrace.com/fullchain.pem ./nginx/certs/fullchain.pem
sudo cp /etc/letsencrypt/live/litoraltrace.com/privkey.pem ./nginx/certs/privkey.pem

# Reiniciar Nginx
docker-compose -f docker-compose.prod.yml restart proxy
```

---

## 6. Verificación de Funcionamiento en Producción
Abre tu navegador e ingresa a:
- **Página Pública B2B**: `https://litoraltrace.com`
- **Dashboard**: `https://litoraltrace.com/dashboard`
- **Bóveda de Archivos**: `https://litoraltrace.com/vault`
- **Documentación OpenAPI**: `https://litoraltrace.com/docs`
