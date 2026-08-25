# Smart Import V1 — Data Intake Engine

## Objetivo

Permitir que Litoral Trace interprete workbooks XLSX empresariales razonablemente estructurados sin exigir que el cliente adapte previamente sus archivos a `Plantilla_LitoralTrace`.

Smart Import es una capa previa al pipeline batch existente. No reemplaza la validación semántica, idempotencia, aislamiento por tenant, auditoría ni persistencia atómica actuales.

## Ciclo 1 — motor de interpretación

Implementado:

- normalización determinística de encabezados: case, acentos, puntuación y abreviaturas comunes;
- diccionario amplio de aliases para los 8 campos canónicos de lotes;
- fuzzy matching sin dependencia externa;
- inferencia de contenido como evidencia secundaria, nunca como autoridad única;
- umbrales más estrictos para campos de trazabilidad de mayor riesgo;
- columnas extra sin rechazo automático;
- descubrimiento de hojas distintas a `Plantilla_LitoralTrace`;
- detección de encabezados dentro de las primeras 25 filas;
- hasta 20 hojas y 256 columnas inspeccionables en discovery;
- ranking de datasets;
- campos obligatorios faltantes explícitos;
- canonicalización side-effect-free a `BatchWorkbook`;
- reutilización del validador batch existente.

## Ciclo 2 — browser mapping + memoria por empresa

Implementado en la rama Smart Import:

- fallback automático desde el parser estricto al motor Smart Import cuando el problema es de estructura/esquema y no de seguridad del contenedor;
- preview browser con hoja sugerida, fila de encabezado, score del dataset, columnas observadas y mapping canónico;
- selector manual source → canonical para los 8 campos requeridos;
- muestras de valores sólo para facilitar la revisión visual del mapping;
- columnas no utilizadas mostradas como ignoradas y excluidas de la canonicalización;
- importación no estándar bloqueada hasta recibir confirmación explícita del mapping;
- binding de la confirmación a `sheet_name + header_row`, fingerprint sensible al orden y SHA-256 exacto del workbook re-subido;
- rechazo si una misma columna se intenta reutilizar para dos campos obligatorios;
- perfil tenant-scoped opcional para recordar el formato;
- el perfil guarda sólo encabezados normalizados + mapping, nunca valores de negocio ni bytes del workbook;
- fingerprint SHA-256 del esquema de encabezados;
- resolución del mapping recordado por nombre normalizado de encabezado, no por posición física de columna;
- `EXACT`: misma firma de encabezados;
- `COMPATIBLE_DRIFT`: cambió la firma (por ejemplo, aparecieron columnas extra) pero siguen existiendo de forma unívoca todas las columnas usadas por el mapping;
- `BLOCKED_DRIFT`: falta o se vuelve ambigua alguna columna necesaria; el perfil no se aplica automáticamente;
- perfiles versionados, con contador de uso y metadatos de creador/último editor;
- tabla protegida con RLS forzado por `organization_id` y privilegios mínimos del runtime;
- nombres de perfiles endurecidos contra caracteres Unicode de control, formato, bidi y separadores invisibles que puedan alterar su representación visual;
- creación y actualización de perfiles auditadas dentro de la misma transacción que la mutación del perfil;
- si la auditoría del perfil falla, la mutación también hace rollback;
- el payload de auditoría conserva sólo metadatos estructurales/versionado y no duplica valores del Excel ni encabezados fuente.

## Flujo browser

```text
Excel del cliente
    ↓
preflight XLSX seguro
    ↓
parser oficial exacto ──si coincide──→ validador canónico
    │
    └─si sólo difiere la estructura──→ Smart Import discovery
                                         ↓
                                   hoja + header
                                         ↓
                                   mapping sugerido
                                         ↓
                                  preview humano
                                         ↓
                                confirmación explícita
                                         ↓
                                  canonicalización
                                         ↓
                               validador batch existente
                                         ↓
                              persistencia atómica existente
```

## Principios de seguridad

1. El XLSX pasa primero por el preflight ZIP/XML endurecido existente.
2. Un error de seguridad del contenedor no activa un bypass Smart Import.
3. Discovery no persiste lotes.
4. Un campo ausente nunca se inventa.
5. Un mapping ambiguo queda sujeto a revisión humana.
6. Dos columnas fuente no pueden auto-mapear silenciosamente al mismo destino canónico.
7. La confirmación recibida desde el browser no se confía por sí sola: el workbook se analiza nuevamente y se verifica hoja, header, fingerprint, índices y SHA-256 del archivo.
8. Fórmulas o errores Excel en columnas mapeadas fallan cerrado; una fórmula en una columna ignorada no contamina el dataset canónico.
9. Los perfiles recordados se aíslan por tenant con RLS y no contienen datos de negocio del archivo.
10. La validación semántica, idempotencia y persistencia atómica existentes siguen siendo la última autoridad.
11. La importación canónica browser mantiene temporalmente el límite de 500 filas; no se aumenta una constante para simular escalabilidad.
12. Smart Import comparte el límite endurecido de 10 MB del parser batch principal.
13. Encabezados, muestras de texto y rangos de filas están acotados antes de operaciones de matching o iteraciones costosas.
14. Un rango XLSX patológico por filas/formato residual falla antes del escaneo largo y devuelve una instrucción de corrección comprensible para el usuario.
15. Los nombres recordados no aceptan controles Unicode, formatos invisibles ni overrides/isolates bidi.
16. CREATE/UPDATE de perfiles y su evento de auditoría forman una sola transacción lógica.

## Estados de mapping del motor

- `AUTO`: suficientemente seguro para preselección automática.
- `CONFIRM`: sugerencia fuerte que debe mostrarse al usuario.
- `MANUAL`: candidato ambiguo; requiere decisión humana.
- `IGNORED`: no tiene correspondencia suficientemente segura con el esquema objetivo.

Una selección realizada por el usuario puede aparecer en el preview como `USER` cuando contradice o reemplaza la sugerencia original del matcher.

## Compatibilidad y límites

La plantilla oficial continúa siendo el fast path y conserva el comportamiento histórico.

Smart Import V1/V1.1 no intenta ser un ETL genérico. Todavía quedan fuera:

- importación grande por jobs/chunks;
- joins entre hojas;
- varias tablas independientes dentro de una misma hoja;
- CSV;
- conectores ERP/API;
- transformaciones arbitrarias;
- LLM/OCR.

## Política de GitHub Actions para repositorio privado

Para preservar el cupo mensual de minutos, la rama mantiene una política de CI de bajo consumo:

- los gates especializados PostgreSQL, EUDR, UX, browser E2E y release son `workflow_dispatch` manual;
- el CI ordinario de PR conserva un único job automático de Python;
- frontend build y production Docker build quedan disponibles dentro del workflow principal, pero sólo mediante dispatch manual;
- commits de documentación/comercial quedan fuera del CI automático por `paths-ignore`;
- durante desarrollo iterativo se utilizan mensajes compatibles con skip de Actions y se reserva una única ejecución automática para el head estabilizado;
- el backup lógico PostgreSQL mantiene una sola ejecución diaria en vez de dos;
- `tests/test_actions_budget_policy.py` protege esta política contra regresiones accidentales.

Esta reducción afecta frecuencia de ejecución, no elimina los gates críticos: se ejecutan explícitamente en hitos de integración o antes de un merge/release que los requiera.

## Gate de cierre recomendado

Antes de mergear Smart Import, el objetivo es obtener una ejecución verde del CI Python sobre el head estabilizado y, como segundo gate deliberado, ejecutar una vez el gate PostgreSQL non-superuser/migración para verificar `029_add_smart_import_profiles`, FORCE RLS y permisos efectivos.

Después del merge no se debe construir automáticamente `Large Intake Jobs`. Ese trabajo sólo pasa a prioridad si un piloto real demuestra que el límite actual de 500 filas bloquea el onboarding o si el mismo patrón aparece de manera repetida en varios prospectos/clientes. Hasta entonces, la prioridad de producto sigue siendo reducir fricción del Cliente #1, no ampliar el ETL por anticipación.
