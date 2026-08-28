# Assurance v1 — runbook de preparación del primer piloto

Este runbook cubre la preparación técnica de un piloto sin modificar producción y sin exigir integración ERP, migración maestra ni cambio de proceso operativo al cliente.

## 1. Staging exclusivo

El piloto debe ejecutarse en un entorno `staging` dedicado. Use `docker-compose.assurance-pilot-staging.yml` y `.env.assurance-pilot-staging` creado a partir del ejemplo versionado.

Criterios obligatorios antes de habilitar a un cliente:

- base PostgreSQL distinta de producción y principal runtime `NOBYPASSRLS`;
- principal de migración separado y usado sólo por el servicio one-shot `migrate`;
- bucket S3 privado distinto del Vault de producción;
- clave JWT, hostname/origen CORS y secretos exclusivos del piloto;
- un único `ASSURANCE_PILOT_ORGANIZATION_ID` autorizado por la configuración del piloto;
- `ENVIRONMENT=staging`, `LT_ASSURANCE_PILOT_MODE=1` y feature flags Assurance habilitados;
- EUDR remoto y workers satelitales deshabilitados salvo validación separada;
- aplicación publicada detrás de un proxy TLS; el Compose sólo enlaza el puerto a `127.0.0.1`.

Antes de arrancar compare explícitamente host/base/bucket/hostname contra producción. Si cualquiera coincide, aborte el despliegue.

Preparación:

1. copie `pilot/assurance-pilot.example.json` a `pilot/assurance-pilot.json` y reemplace únicamente referencias operativas del cliente piloto;
2. copie `.env.assurance-pilot-staging.example` a `.env.assurance-pilot-staging` y cargue credenciales desde el gestor de secretos;
3. valide la composición con `docker compose --env-file .env.assurance-pilot-staging -f docker-compose.assurance-pilot-staging.yml config`;
4. migre con el perfil one-shot `migration`;
5. inicie `app` y exija `/ready` HTTP 200 antes de entregar acceso.

La existencia de estos archivos no demuestra por sí sola que exista un staging persistente: la casilla de staging sólo debe cerrarse cuando el entorno externo dedicado esté realmente aprovisionado y verificado.

## 2. Permisos y aislamiento documental

El usuario piloto entra por autenticación normal y conserva RBAC. Evidence Vault y las tablas Assurance usan `organization_id`; los gates PostgreSQL con `FORCE RLS` deben permanecer verdes antes de cada entrega. No use usuarios propietarios de tablas, `BYPASSRLS`, superusuarios ni credenciales de migración en el proceso web.

Prueba mínima antes de entregar acceso: crear datos equivalentes en tenant A y tenant B con el runtime role y demostrar que A no puede leer/modificar documentos, extracciones, proveedores, discrepancias ni excepciones de B.

## 3. Logs sin datos sensibles

El staging usa `pilot/logging.json` y `SensitiveDataLogFilter`. Además se deshabilita el access log de Uvicorn para no persistir query strings ni referencias operativas en URLs.

No registrar cuerpos de request, bytes/documentos, texto OCR, valores extraídos, CUIT, correo, teléfono, dirección, cookies, Authorization, JWT, claves, contraseñas ni secretos. Los logs operativos deben usar códigos, conteos, estados, ids técnicos no sensibles y clases de error sanitizadas. Sentry queda deshabilitado en el perfil inicial; si se habilita después debe aplicar el mismo criterio de saneamiento.

## 4. Replay histórico sin ERP

No se crea otro importador. El replay usa la entrada universal existente de Assurance y los mismos PDF/XLSX/XLS/CSV que la empresa ya produce o archiva.

Procedimiento:

1. seleccionar una operación histórica cerrada y copiar sus archivos a una carpeta de trabajo;
2. anonimizar cuando corresponda según la sección 6;
3. cargar por la pantalla única `Agregar documentos/datos de operación`; para lotes grandes dividir en tandas de hasta 20 archivos;
4. esperar extracción y revisar sólo campos marcados `NEEDS_REVIEW`;
5. verificar vínculos de proveedor/lote/despacho, discrepancias, Preflight y Market-Ready Matrix;
6. conservar la carpeta fuente sólo fuera de Git y fuera del staging si contiene originales no anonimizados.

No se requiere API del ERP, acceso a la base del cliente, agente local ni sincronización bidireccional para iniciar el piloto.

## 5. Formatos de entrega aceptados

El cliente puede entregar PDF, XLSX, XLS y CSV. Los originales se guardan primero en Evidence Vault, se validan, se deduplican por SHA-256 y recién después se procesan. Un formato fuera de esa lista se rechaza en vez de degradarse silenciosamente.

## 6. Procedimiento de anonimización

Para operaciones históricas que no deban conservar identidades reales:

1. trabaje sobre copias, nunca sobre los originales;
2. cree un mapa local de seudónimos consistente para cliente, proveedores, lotes, pedidos y despachos;
3. sustituya nombres, correos, teléfonos, domicilios, cuentas bancarias, identificadores personales y CUIT por valores sintéticos consistentes; si se necesita probar matching por CUIT use un CUIT sintético válido y el mismo valor en todos los documentos relacionados;
4. preserve únicamente relaciones, cantidades, unidades, fechas relativas, producto y mercado que sean necesarias para reproducir el workflow;
5. en PDF no use una caja visual que deje texto recuperable debajo: genere una copia realmente redaccionada/flattened y compruebe la capa de texto antes de subir;
6. no cargue al staging el mapa real↔seudónimo y no lo guarde en Git, Vault ni logs;
7. ejecute una revisión humana final buscando nombres reales, correos, CUIT, teléfonos, direcciones, tokens y secretos en todos los archivos anonimizados;
8. documente quién hizo la revisión, fecha y conjunto de archivos, sin incluir los valores sensibles.

Si una validación requiere documentos legales reales, obtenga autorización del cliente y use el staging privado; no declare el set como anonimizado.

## 7. Configuración mínima cliente / mercado / reglas

`LT_ASSURANCE_PILOT_CONFIG_PATH` apunta a un JSON montado read-only. La configuración es tenant-scoped y define combinaciones exactas de `customer_reference + market + product`, documentos requeridos y si la operación exige evaluación fitosanitaria/EUDR.

Preflight combina esas reglas con el request: los documentos configurados no pueden eliminarse desde el cliente HTTP y un requisito `true` no puede degradarse a `NOT_APPLICABLE`. Si el piloto está habilitado y una combinación completa no existe en la configuración, el request falla cerrado para impedir un READY sin reglas.

El archivo real del cliente es local/deployment config y está ignorado por Git. El ejemplo versionado contiene datos ficticios. La configuración se considera “real” para conciliación de documentos requeridos sólo después de validarla con la empresa; hasta entonces no debe usarse para cerrar la deuda de reglas reales del checklist.

## 8. No exigir cambio de proceso para iniciar

Contrato del primer piloto: la empresa continúa generando sus documentos y planillas actuales. Litoral Trace recibe copias, extrae y reutiliza datos, muestra únicamente excepciones/revisión necesaria y calcula la preparación operacional. El piloto no exige cambiar ERP, numeración, responsables internos, circuito de aprobación ni formato documental antes de comenzar.

Cambios de proceso se proponen sólo después de medir el replay histórico y una operación viva, y únicamente si reducen una fricción demostrada.
