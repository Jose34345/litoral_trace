# P0 calidad documental — issue #473

## Estado y alcance

Base de implementación: `8b909fa4488e0d0017ce5c855c0e7aa1622ccc10`, rama `fix/p0-document-quality-473`, worktree dedicada. DI-01, DI-03 y limpieza #472 ya estaban integradas. Sólo se utilizaron los tres fixtures sintéticos versionados de `tests/fixtures/us_lacey_quality_output/`. No se modificaron sus bytes.

La observación de nueve pendientes, 25 respaldados y tres confirmados no demuestra por sí misma nueve errores de extracción. Este cambio recupera observaciones literales y conserva las decisiones de autoridad existentes. No afirma haber corregido o reprocesado una operación LIVE.

## Diagnóstico y antes/después

| Campo / ruta | Antes reproducido | Después comprobado localmente | Decisión de autoridad |
| --- | --- | --- | --- |
| Consignee Name, Engine 2 | DI-03 ya extraía `Atlantic Home Goods Distribution Inc.` separado. No se reprodujo aquí la concatenación. | Conserva exactamente el nombre y los anclajes de fuente. | La extracción no confirma al consignatario. |
| Consignee Address, Engine 2 | `CONFLICT` entre dirección de línea visual truncada y dirección completa de la misma celda. | Una dirección completa, obtenida de la misma fila física comprobada por página, etiqueta, texto literal y coordenadas; originales visuales intactos. | No se resuelven por prefijo direcciones de otras filas/páginas o sin coordenadas. |
| Description, Engine 2 | `MISSING`: faltaba la etiqueta explícita `Description of Merchandise` tanto en extracción como en admisión. | Recupera el resumen explícito y las tres descripciones comerciales por fila; no interpreta la cabecera de la tabla como mercancía. | El read model existente conserva las filas locales y compone su presentación; no se modifica publicación canónica. |
| Consignee Name, proyección legacy | Publicaba como propuesta la celda completa `nombre | domicilio`. | Vista inmutable de nombre y domicilio separados, cada uno vinculado al ID y locator de la observación original. | No hay aceptación humana automática. |
| Consignee Address, proyección legacy | Sin candidato desde esa celda; tampoco reconocía el encabezado posesivo del formulario. | Dirección separada desde el delimitador explícito; también reconoce el campo explícito de dirección en formulario/Field-Value. | Si no puede separar inequívocamente, conserva el original y no inventa domicilio. |
| Description, proyección legacy | No enrutaba el encabezado del formulario ni la fila explícita `Field/Value` del XLSX. | Recupera la descripción explícita del formulario de página 2 y de `Shipment Data!B19`. Las tres descripciones de tabla comercial siguen siendo observaciones de línea, no tres valores del escalar. | El worksheet continúa siendo derivado; la hoja BOM no se transforma en descripción. |

### Evidencia literal y trazabilidad

1. **Consignatario, PDF 01, página 1.** Fila `p1-t1-r2`: `Consignee: Atlantic Home Goods Distribution Inc. | 825 Portside Logistics Pkwy., Pooler, GA 31322, United States`. La línea visual `p1-l9` acaba en `31322,`; `p1-l10` contiene `United States`. La normalización existente `party_core` / `party_address` separa los dos componentes. El defecto de Engine 2 era tratar la línea visual parcial como otra dirección independiente. Se conserva la geometría de la fila key/value y se usa su valor completo sólo si contiene físicamente la línea y coincide su prefijo literal y etiqueta. El candidato apunta a la fila completa, página y bbox; ambas líneas visuales siguen en el layout original.
2. **Nombre/domicilio legacy.** El parser Assurance ya conservaba completa la misma celda. El proyector mapeaba `raw.table.1.Consignee` al nombre sin separar su contenido. `documentary_review_rows` produce vistas de esa observación sin mutar las filas persistidas, mantiene ID, página, confianza y locator y separa sólo un delimitador `|` explícito cuyos componentes coinciden sin pérdida con los parsers existentes. No hay vinculación por similitud de empresa.
3. **Descripción resumen, PDF 01.** Página 1, `p1-l33`: `Description of Merchandise: Sawn wood boards, wooden shipping crates, and solid-wood dining chairs.` Página 2, `p2-t1-r9`, repite ese valor en el worksheet. La etiqueta se reconoce en Engine 2; la ruta legacy reconoce sólo el campo escalar explícito del formulario disperso, sin convertir una tabla densa en descripción de embarque.
4. **Descripciones comerciales por producto, PDF 01, página 1.** `p1-t2-r1-c2`: `Sawn eucalyptus boards, kiln-dried`; fila 2: `Wooden shipping crates, assembled`; fila 3: `Solid-wood dining chairs`. Los SKU explícitos de esas mismas filas son `EUC-BRD-01`, `PINE-CRT-02` y `TEAK-CHR-03`. La ruta determinística probada conserva identidades locales `17:logical-001:p1-t2:row:1..3`; no se inventa una identidad canónica `sku:`. En el read model las descripciones conservan documento, página, texto y line_key. La composición de presentación ya existente usa esas tres descripciones explícitas, no inferencia de materiales.
5. **XLSX 03.** `Shipment Data!B13` = nombre, `B14` = domicilio y `B19` = resumen explícito anterior; sus etiquetas están en la columna A de la misma fila. `Plant Lines!C2:C4` y `Packing List!C2:C4` contienen las tres descripciones comerciales. La normalización legacy permite solamente los campos explícitos `Field/Value` afectados; no sintetiza un resumen a partir de `BOM!B2:B9`, no agrega filas botánicas y no promueve el workbook a fuente original verificada.
6. **PDF 02.** Contiene evidencia botánica, no los campos de consignatario o resumen comercial investigados. No se lo usa para completarlos. Su clasificación y los candidatos de especies/país permanecen fuera de este cambio.

## Diferencia importante entre rutas reales

El batch de Engine 2 procesa PDF; el XLSX del paquete completo produce la abstención `PARTIAL` existente. `worker.py` omite publicación canónica cuando no existe un snapshot completo. Por eso corregir sólo Engine 2 no arreglaba la ruta de pantalla que seguía utilizando proyección legacy.

No se agregó soporte XLSX a Engine 2, no se omitió ese gate y no se publican snapshots parciales. La integración legacy consta de un import y una llamada al normalizador enfocado antes de la selección de candidatos. El gate PostgreSQL Golden extendido prueba los tres valores exactos después de procesar los tres archivos con el worker real y verifica que no aparezcan decisiones humanas automáticas.

## Autoridad y pendientes legítimos

`classify_document("01_Commercial_Invoice_Entry_Worksheet.pdf", "COMMERCIAL_INVOICE")` sigue devolviendo `DERIVED_PREPARATION`. El nombre de rol no convierte un worksheet en original. La elegibilidad rápida y la validación transaccional de revisión no se modificaron. Recuperar el texto permite inspeccionarlo; no autoriza una confirmación basada únicamente en el worksheet.

El workbook contiene campos explícitos pero tampoco adquiere autoridad original por llamarse packing list. Cuando no exista fuente original suficientemente autorizada, el campo debe continuar pendiente de evidencia/revisión. No se reducen excepciones desactivando una validación. Los conflictos entre direcciones independientes permanecen visibles.

## Cambios

- `lacey_engine/documentary_review_rows.py`: vistas literales para los campos afectados en formas key/value y filas explícitas Field/Value.
- `lacey_engine/party_source.py`: correspondencia conservadora entre línea visual parcial y celda completa de la misma fuente física.
- `lacey_engine/layout_parser.py`: bbox de filas PDF key/value; sin cambios OCR.
- `lacey_engine/pipeline.py`, `admission.py`: etiqueta explícita de mercancía y uso de la fuente física completa para la parte.
- `us_lacey/projection.py`: integración del normalizador, sin reglas de autoridad nuevas.
- Tests nuevos documentales/legacy, gate Golden all3 y prueba PostgreSQL existente de identidad de versión.

`ENGINE_VERSION` pasa de `lacey-engine-2.6.3` a `lacey-engine-2.6.4`. La versión participa tanto en la identidad inmutable de extracción como en la huella del conjunto de fuentes. Se comprueba que ambas cambian y que no se reutiliza la extracción previa. No se modifica ningún módulo de caché ni se reescribe historia o documentos ya revisados.

## Validación ejecutada por el implementador

- Rojo Engine 2 antes de la corrección: **6 fallos, 5 pases**; dirección truncada, etiqueta omitida e integración de valores.
- Rojo legacy antes de normalizar: **2 fallos, 2 pases** sobre los tres archivos reales del fixture; concatenación postal y descripción ausente.
- Rojo del contrato de versión: **1 fallo** con la versión anterior.
- Focales con nuevos casos, DI-03, Pack 1, Pack 2, masa neta 288 kg, controles estructurales y autoridad: **136 pases**.
- Suite ampliada: **536 pases, 4 omitidos, 24.20 s**. Incluye `tests/lacey_engine`, tests de proyección, `canonical_publication_support`, `engine2_supported_suggestions`, `review_source_authority` y nuevos casos legacy. Las omisiones son gates/fixtures opcionales, no conversiones artificiales a verde.

Todas estas ejecuciones usaron el runner offline, entorno sin URLs/credenciales, plugins automáticos desactivados, dotenv desactivado y PostgreSQL desactivado. No hubo llamadas externas de IA ni documentos privados. No se ejecutó merge o deploy.

La coordinación ejecuta separadamente PostgreSQL/RLS, worker/E2E y CI completo. Sus resultados definitivos deben adjuntarse al PR; este informe no presenta esos gates como ejecutados personalmente por este implementador. La aserción histórica de versión en el test de persistencia se actualizó por coordinación para comparar 2.6.3 con 2.6.4, manteniendo la comprobación de dos extracciones.

## GO / NO-GO

**GO local de implementación y regresión documental. NO-GO de merge hasta completar gates PostgreSQL/CI y revisión independiente.** El flujo completo conserva los gates de autoridad, source set, revisión, exportación y auditoría. No se afirma una reducción cuantitativa de decisiones humanas, una resolución de pendientes regulatorios ni una mejora LIVE sin su verificación correspondiente.

### Estado de coordinación comunicado posteriormente

Coordinación informó CI completo con **2783 pases, 366 omisiones y cero fallos**, y worker/E2E con **10 pases**, incluido Golden all3. El primer gate PostgreSQL tuvo 63 pases y una aserción de versión anterior; coordinación actualizó esa comprobación al contrato 2.6.3 → 2.6.4. Falta adjuntar el resultado de su rerun y la revisión independiente final. Son resultados comunicados por coordinación, no ejecuciones de este implementador.

## Cierre de validaci?n local coordinada ? 2026-10-30

Este cierre sustituye el pendiente local del apartado anterior. Suite general: **2783 passed, 366 skipped, 0 failures/errors** (runner offline con conftest general y SQLite temporal). PostgreSQL real aislado: **64 passed, 0 skipped** en gates y **10 passed, 0 skipped** en worker/E2E, incluido el golden que ingiere los tres fixtures. El primer fallo PostgreSQL era una expectativa de versi?n anterior; la prueba conserva dos extracciones y verifica ahora 2.6.3 ? 2.6.4, sin cambiar m?dulos de cach?.

Revisi?n independiente de c?digo/autoridad y 75 pruebas: sin Critical/Important abiertos. Sintaxis de 982 archivos Python, el TOML rastreado, `git diff --check` y Alembic: correctos; ?nico head `079_us_lacey_assurance_v2_claim_snapshots`. Los 36 fixtures rastreados conservan su blob Git original; los PDF/XLSX no se modificaron. No hay migraciones, dependencias, datos privados, ni cambios de configuraci?n productiva.

Las 366 omisiones de la suite general incluyen los gates que requieren PostgreSQL habilitado; ese resultado no sustituye los 74 pases PostgreSQL independientes. No se introdujeron skips para conseguir verde. Logs/JUnit, comandos y manifiesto de integridad quedan fuera de Git en el directorio temporal del sprint. No se midieron decisiones humanas ni tiempos de usuarios reales. **GO local; integraci?n pendiente de CI remoto y revisi?n del PR.**
