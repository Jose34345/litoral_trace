# Manual de Despliegue en Produccion (Deployment Runbook) - litoraltrace.com

Guia completa para actualizar el servidor VPS en la nube y transmitir la nueva plataforma FastAPI B2B para **Litoral Trace**.

---

## 1. Acceso al Servidor VPS via SSH
Abre la terminal en tu computadora y conectate a tu servidor VPS en la nube:
```bash
ssh root@IP_DE_TU_SERVIDOR
```

---

## 2. Transferencia y Extraccion del Nuevo Paquete
Desde tu servidor VPS, descarga e instala la version mas reciente:
```bash
cd /opt
rm -rf /opt/litoral_trace_old
mv /opt/litoral_trace /opt/litoral_trace_old 2>/dev/null || true

# Descargar y descomprimir el nuevo paquete
curl -L -o litoral_trace_fastapi.zip "https://drive.google.com/uc?export=download&id=1Nlwg9tK87I52-tCeHAWLOfVQzoy52XtR"
unzip litoral_trace_fastapi.zip -d /opt/
cd /opt/litoral_trace
```

---

## 3. Configuracion de Credenciales de Produccion (`.env` / `.streamlit/secrets.toml`)
Asegurate de tener configuradas las credenciales del servidor:
```bash
mkdir -p .streamlit
cat << 'SECRETS' > .streamlit/secrets.toml
DATABASE_URL = "<set-runtime-database-url>"
MIGRATION_DATABASE_URL = "<set-migration-owner-database-url>"
# Legacy alias para compatibilidad temporal:
# DB_URL = "<optional-legacy-alias>"
# POSTGRES_URL = "<optional-legacy-alias>"

[gcp_service_account]
type = "service_account"
project_id = "<set-production-gcp-project-id>"
private_key_id = "<set-production-gcp-private-key-id>"
private_key = "<load-from-secret-manager-or-escaped-env>"
client_email = "<set-production-service-account-email>"
SECRETS
```

> IMPORTANTE: FastAPI/runtime debe usar `DATABASE_URL`.
> Alembic / migraciones deben usar `MIGRATION_DATABASE_URL`.
> Las credenciales runtime no deben ser owner del schema ni tener `BYPASSRLS`.
> `init_db.py` NO debe utilizarse en produccion; Alembic es el unico mecanismo oficial para crear o migrar el esquema.
> SQLite solo es un fallback de desarrollo/local y NO debe usarse en produccion.

---

## 4. Ejecucion del Script de Despliegue Automatico
```bash
chmod +x deploy_production.sh
./deploy_production.sh
```

---

## 5. Configuracion del Certificado SSL con Let's Encrypt (HTTPS)
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

## 6. Verificacion de Funcionamiento en Produccion
Abre tu navegador e ingresa a:
- **Pagina Publica B2B**: `https://litoraltrace.com`
- **Dashboard**: `https://litoraltrace.com/dashboard`
- **Boveda de Archivos**: `https://litoraltrace.com/vault`
- **Documentacion OpenAPI**: `https://litoraltrace.com/docs`
