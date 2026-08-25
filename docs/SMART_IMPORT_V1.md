# Smart Import V1 — Data Intake Engine

## Objetivo

Permitir que Litoral Trace interprete workbooks XLSX empresariales razonablemente estructurados sin exigir que el cliente adapte previamente sus archivos a `Plantilla_LitoralTrace`.

Smart Import es una capa previa al pipeline batch existente. No reemplaza la validación semántica, idempotencia, aislamiento por tenant, auditoría ni persistencia atómica actuales.

## Primer ciclo implementado

- Normalización determinística de encabezados: case, acentos, puntuación y abreviaturas comunes.
- Diccionario amplio de aliases para los 8 campos canónicos de lotes.
- Fuzzy matching sin dependencia externa.
- Inferencia de contenido como evidencia secundaria, nunca como autoridad única.
- Umbrales más estrictos para campos de trazabilidad de mayor riesgo.
- Detección de columnas extra sin rechazar el workbook.
- Descubrimiento de hojas distintas a `Plantilla_LitoralTrace`.
- Detección de encabezados dentro de las primeras 25 filas.
- Hasta 20 hojas y 256 columnas inspeccionables por workbook en la capa de discovery.
- Selección del mejor dataset por hoja y ranking global.
- Identificación explícita de campos obligatorios faltantes.
- Canonicalización side-effect-free de un mapping confirmado hacia `BatchWorkbook`.
- Reutilización directa del validador semántico batch existente después de canonicalizar.
- Compatibilidad con la plantilla oficial como caso simple.

## Principios de seguridad

1. El XLSX pasa primero por el preflight ZIP/XML endurecido existente.
2. Discovery no persiste datos.
3. Un campo ausente nunca se inventa.
4. Un mapping ambiguo queda `MANUAL` o `IGNORED`.
5. Campos críticos requieren mayor confianza para auto-map.
6. Dos columnas fuente no pueden auto-mapear silenciosamente al mismo destino canónico.
7. Las columnas extra se ignoran en la proyección canónica; no invalidan el workbook.
8. Fórmulas o errores Excel en columnas mapeadas fallan cerrado.
9. La validación final sigue pasando por el esquema canónico de Litoral Trace antes de cualquier escritura en PostgreSQL.
10. La canonicalización V1 conserva temporalmente el límite actual de 500 filas porque el validador/persistencia existentes todavía son atómicos. Discovery puede inspeccionar workbooks más amplios; chunk/jobs se implementará en un gate posterior.

## Estados de mapping

- `AUTO`: suficientemente seguro para preselección automática.
- `CONFIRM`: sugerencia fuerte que debe mostrarse al usuario.
- `MANUAL`: candidato ambiguo; requiere decisión humana.
- `IGNORED`: no tiene correspondencia suficientemente segura con el esquema objetivo.

## Lo que todavía no hace este primer ciclo

- UI browser de preview/mapping/confirmación.
- Persistencia de perfiles de mapping por empresa.
- Fingerprint y schema drift/versionado por tenant.
- Importación grande por chunks/jobs.
- Joins entre hojas.
- Varias tablas independientes dentro de una misma hoja.
- CSV/ERP/API.
- LLM/OCR.

## Próximo gate

Exponer el discovery y la canonicalización mediante un preview browser-safe: hoja sugerida, fila de encabezado, mappings, confidence, campos faltantes y muestra de filas. Después, persistir perfiles por `organization_id` con fingerprint y detección de schema drift. El procesamiento de más de 500 filas debe implementarse como trabajo explícito por chunks sin debilitar la semántica fail-closed.
