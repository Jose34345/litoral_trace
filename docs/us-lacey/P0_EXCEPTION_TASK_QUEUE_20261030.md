# P0 UX Exception Work Queue - evidencia (2026-10-30)

Estado: GO local focal, pendiente de gates completos/remotos y QA visual del coordinador. Sin merge ni despliegue. Rama `feat/p0-exception-task-queue`; base `8b909fa4488e0d0017ce5c855c0e7aa1622ccc10`.

## Arquitectura y alcance

`exception_task_queue.py` es una proyeccion de presentacion: consume campos ya proyectados y devuelve grupos/tareas; no escribe, decide readiness ni establece autoridad documental. Grupos: Verify existing evidence; Obtain missing or original documents; Complete missing information; Required before filing. Los limites de agrupacion incluyen scope/linea, entidad conocida y documento exacto. Los conflictos permanecen individuales. No se infieren equivalencias por nombres, HTS o proximidad.

El nuevo fragmento reutiliza `action_field`, endpoints de review por campo, CSRF por campo, candidate_id y modal de valor verificado. La accion pareada ya existente de masa mantiene IDs/valores/locators, transaccion y auditoria separada. No hay un batch nuevo. Los grupos con varios campos se expanden desde un unico control principal; sus decisiones individuales son secundarias.

PDF.js, fuente autorizada, anclas y modal movil existentes se reutilizan. Cada candidato unico tiene acceso directo al documento/pagina/locator. El ancho de la cola se adapta al inspector existente solo si quedan al menos 320px utiles, considerando el offset real del shell. El inspector movil y su retorno de foco permanecen a cargo del componente existente.

## Cambios

- `src/litoral_trace/us_lacey/exception_task_queue.py`: agrupacion solo visual.
- `src/litoral_trace/templates/us_lacey/fragments/exception_task_queue.html`: grupos y tareas; requisitos regulatorios visibles.
- `src/litoral_trace/templates/us_lacey/fragments/operation_workspace.html`: tarjetas responsivas en pendientes, acciones secundarias progresivas, elegibilidad en todas las alternativas y avance HTMX que abre el grupo de destino.
- `src/litoral_trace/static/css/us_lacey_exception_tasks.css`: estilos locales de cola, sin menu global.
- `src/litoral_trace/web/us_lacey_operational_views.py`: mismos contextos en pagina completa y fragmento HTMX.
- `tests/test_us_lacey_exception_task_queue.py`: nueve contratos de render/handler HTTP/DOM; helpers sinteticos `render_case(A-F)` y `GROUP` para QA visual offline.

## Diagnostico y rojo-verde

Baseline anterior a modificar comportamiento: 30 passed (fast review, contexto original, pre-entry, rutas).

Regresiones nuevas, sobre render productivo: 5 failed y 1 passed. Faltaban categorias/agrupacion y la tabla imponia un ancho minimo de 70rem. Un conflicto con dos candidatos mostraba forms Accept aunque `quick_accept_eligible` lo rechazaba. Se aplico ese mismo guard visual tambien a las alternativas; el escritor no se modifica. El caso HTMX de conteos ya funcionaba y se conserva como control.

Validacion focal final del implementador: **126 passed, 1 warning**, 19.89s. Incluye los nueve tests nuevos y contratos de fast review, Pack2 presentation, articulo original, masa pareada/autoridad, pre-entry, modal Entry Number, rutas/CSRF, regulatory UI, normalizacion, campos reutilizables, Product Intelligence, dossier, exception-first, preview y rendimiento PDF. El warning es una deprecacion anyio de Starlette TestClient. No es un fallo funcional.

Ejecucion: Python 3.11 del venv existente mediante runner offline; PYTHONPATH src+raiz; sin conftest global ni plugins automaticos. Se eliminaron variables URL/secret/key/token/password del proceso y se desactivo dotenv. El runner bloquea red no loopback. No se usaron fuentes privadas, proveedores externos ni nuevos paquetes.

## Resultados reproducibles y metricas

| Caso sintetico | Antes | Despues comprobado |
|---|---|---|
| Importer name/address ausentes | 2 filas independientes | 1 tarea, 2 campos/IDs/CSRF y decisiones individuales conservados |
| Conflicto con 2 candidatos | 2 forms Accept visibles en alternativas | 0 forms Accept; ambas fuentes y estado conflictivo conservados |
| Candidato unico elegible | Confirmacion por candidate_id | Misma decision y candidato; documento/pagina/locator conservados |
| Worksheet sin original | Pendiente documental | Grupo de originales; Prepare evidence request; ningun envio afirmado |
| Entry Number no emitido | Pendiente y exportacion bloqueada | Required before filing; pendiente y exportacion bloqueada |
| POST HTMX autenticado | 2 campos pendientes | 1 pendiente, campo aceptado fuera de cola, un solo workspace |
| Jerarquia inicial | Acciones paralelas por campo | Maximo 1 accion principal por tarea en casos A/B/C/D/GROUP, verificado con parser DOM |

No se midieron ahorro de tiempo, clicks de usuarios reales ni porcentajes de productividad. El numero de decisiones regulatorias no disminuye por agrupar visualmente. Las capturas, overflow a viewport movil, tiempos y resultados del recorrido navegador corresponden al reporte separado del coordinador; el implementador no los presenta como ejecutados por el.

## Seguridad y limites de validacion

No se modifican RLS, escritores, CAS, source sets, auditoria append-only, reglas Lacey, generacion/exportacion ni flags V2. Un fallo CSRF en el handler real no llama al escritor y conserva el campo; una respuesta exitosa devuelve el render autoritativo con conteos actuales. La entrada futura no se marca no requerida. Las solicitudes solo preparan texto para copiar.

PostgreSQL/RLS/source sets/review transaccional, suite completa, CI remoto y QA visual son gates coordinados por la rama principal del sprint y deben adjuntarse antes de merge. Los tests HTTP usan lecturas/servicios DB sinteticos; no sustituyen esos gates de persistencia.

Una restauracion de archivo desde HEAD para corregir encoding fue rechazada por revision automatica por riesgo de descartar cambios locales. No se ejecuto: se corrigieron unicamente cuatro secuencias introducidas en el diff propio, y el diff final de vistas contiene solo el import y los dos hooks previstos. Sin bloqueo pendiente por ese rechazo.

## Cierre de validaci?n local coordinada ? 2026-10-30

Suite general final: **2771 passed, 366 skipped, 0 failures/errors**. PostgreSQL real aislado final: **64 passed, 0 skipped** en gates y **12 passed, 0 skipped** en worker/E2E, incluyendo portal activo. Las tres pruebas antiguas que buscaban literalmente la matriz eliminada se adaptaron para seguir el componente extra?do y verificar sus mismos contratos de valores/acciones/auditor?a; ninguna se borr? ni omiti?.

Se verificaron GET terminal y POST HTMX: el fragmento aut?nomo incluye actualizaciones OOB, mientras la p?gina completa no duplica los IDs de workflow/readiness/species/country. Este defecto de duplicaci?n ya exist?a y se reprodujo antes de corregir la composici?n. Escritores y reglas de revisi?n permanecen intactos. Revisi?n independiente: 15 pases y ning?n bloqueo de autoridad/CSRF; el hallazgo OOB qued? cerrado con regresi?n adicional y suite general final.

Chrome headless offline, viewports 1440 y 390px, fixtures p?blicos sint?ticos y plantillas reales: casos A?H verifican evidencia/retorno, originales insuficientes, entrada pendiente, conflicto verdadero sin Accept, refresco HTMX 2?1 con IDs ?nicos, agrupaci?n importer (1 tarea/2 campos), y dos enlaces de exportaci?n ?nicamente con estado backend COMPLETED simulado. El inspector existente carga la p?gina PDF; no hay segundo visor. Este harness visual usa servicios/render sint?ticos y no se presenta como E2E completo de generaci?n federal: el handler HTTP y PostgreSQL se prueban por separado. Capturas/JSON y comandos permanecen fuera de Git.

Sintaxis de 980 Python, TOML, diff whitespace y ?nico Alembic head correctos; 36 blobs de fixtures intactos. No hay cambios de escritor, RLS, migraciones, cach?s, reglas regulatorias, men? global ni dependencias. Agrupar no reduce el n?mero de decisiones regulatorias. No se afirma ahorro de tiempo de usuarios. **GO local; integraci?n pendiente de CI remoto y revisi?n del PR.**
