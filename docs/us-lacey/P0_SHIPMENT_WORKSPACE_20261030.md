# P0 Shipment Workspace ? verificaci?n 2026-10-30

## Alcance y dependencia

Rama `feat/p0-shipment-workspace`, apilada sobre `feat/p0-exception-task-queue` (PR #476, `95da80a0d7b7c96d587a2dcc0cabde727ba6e3de`). Base can?nica del sprint: `8b909fa4488e0d0017ce5c855c0e7aa1622ccc10`. PR4 contiene ?nicamente el delta workspace frente a PR3; no duplica su implementaci?n. Integraci?n local no destructiva, sin rebase/reset ni modificaciones a worktrees originales.

Navegaci?n principal Review ? Documents ? Package; Pending/Supported/Confirmed son filtros de la misma revisi?n. UUID completo y auditor?a permanecen en detalles/copia; proveedor no resuelto sigue visible con acci?n v?lida. Se extrae una franja de readiness con los valores reales del backend, sin nueva regla. Scores quedan en detalles t?cnicos con advertencia expl?cita; evidencia/documento/p?gina, elegibilidad y revisi?n final se conservan. Aplicabilidad y an?lisis regulatorio se ubican bajo Package; IDs/anclas hist?ricos y formularios evidenciados se mantienen. El paquete bloqueado pliega formatos deshabilitados y conserva los requisitos; los dos downloads s?lo aparecen cuando el estado backend lo autoriza.

## Composici?n HTMX

P?gina completa sin OOB anidado. Fragmentos aut?nomos GET terminal/POST actualizan un workflow y un summary por outerHTML, que contiene las tres tarjetas con IDs ?nicos. El nodo de aplicabilidad existente se conserva antes del swap y se reinserta bajo Package despu?s, manteniendo un escenario no guardado sin presentarlo como decisi?n verificada. No se cambia el escritor, CSRF, auditor?a, RLS, generaciones, reglas o autorizaci?n de exportaci?n.

Dos conflictos iniciales de integraci?n: comentario del controlador y footer OOB. Se mantuvieron hooks/grupos/guards de la cola y el summary ?nico. Un conflicto posterior de CSS compilado se resolvi? regenerando el artefacto desde las fuentes conjuntas, sin escoger una versi?n que descartara cambios. Los commits de ambas ramas se conservan.

## Rojo ? verde y gates locales

- Baseline 30 pases antes de cambiar comportamiento. Nuevos contratos workspace: 6 fallos/1 pase antes de implementar.
- Contratos combinados focales: 101 pases; revisi?n independiente: 37 pases, sin Critical/Important abiertos.
- Suite general inicial: 2769 pases, 13 fallos, 366 omitidos. Los 13 fallos eran selectores est?ticos de navegaci?n anterior/ubicaci?n de componentes/score en columna. Se reprodujeron (13 fallos/13 pases), actualizaron ocho archivos siguiendo componentes reales y reforzaron renders de vac?o/procesamiento/fallo/readiness/V2: 31 pases, sin omisiones. No se borraron pruebas ni se silenciaron contratos regulatorios.
- **Suite general final: 2787 passed, 366 skipped, 0 failures/errors.** Los skips de servicios opcionales son expl?citos, no se cuentan como pases.
- **PostgreSQL aislado: 64 gates + 12 worker/portal E2E passed; 0 failures y 0 skipped**, incluyendo RLS/source sets, canonical, stale generations y golden Pack 1/Pack 2. El c?digo de persistencia no cambi?.
- 981 archivos Python compilados en memoria, TOML v?lido, Jinja mediante contratos de render, JS syntax correcto, 7 pruebas Node del inspector aprobadas, diff check y Alembic head ?nico `079_us_lacey_assurance_v2_claim_snapshots` correctos.
- Build CSS con dependencias ya fijadas, sin nuevos paquetes en manifests: dos ejecuciones id?nticas, SHA256 `6bad071591e2a3f014cefbdc6bc16abcb0d3ffc69d3927805a5bb78bcc9e8ee2`.
- Los 36 fixtures rastreados conservan su blob original; sin cambios a DI-01/DI-03, cach?s, OCR, migraciones, billing, men? global, configuraci?n o modos experimentales. Ning?n PDF privado, dump o secreto en el diff.

## Navegador y medidas reproducibles

Chrome headless offline, viewport desktop 1440?1100 y m?vil 390?844. Ocho escenarios sint?ticos A?H sobre plantillas, estilos, HTMX e inspector existentes: A evidencia; B originales insuficientes/solicitud s?lo preparada; C Entry pendiente/export bloqueado; D dos valores realmente contradictorios y ambas fuentes sin Accept; E contadores 2?1/IDs ?nicos y escenario no guardado conservado; F PDF p?gina 1 y retorno de foco a la tarea; G importer dos campos/una tarea/decisiones separadas; H dos links s?lo en estado COMPLETED simulado. Todos los checks aprobaron. No hubo red externa ni nuevo visor.

| Muestra sint?tica con el mismo harness | Base 8b909fa | Cola PR3 | Workspace + cola |
|---|---:|---:|---:|
| Secciones principales | 5 | 5 | 3 |
| Primera fila/campo, desktop (px desde borde superior) | 641.98 | 676.98 | 609.84 |
| Primera fila/campo, m?vil (px) | 827 | 878 | 773.05 |
| Controles visibles A (button/a/input/select/summary) | 24 | 22 | 26 |
| Scrolls horizontales internos, m?vil | 2 | 2 | 0 |
| Conflicto D ofrece Accept | S? | No | No |
| Tarjeta status duplicada tras HTMX E | S? | No | No |

La altura no equivale al tiempo necesario para resolver una operaci?n. El total de controles visibles no disminuy? en todas las muestras: no se presenta una reducci?n universal de controles ni decisiones. S? se verifican tres secciones, cero scroll horizontal interno en el workspace conjunto y m?ximo una acci?n principal por tarea. El caso G conserva ambos IDs y exige decisiones independientes. El harness visual usa render/servicios sint?ticos; las verificaciones HTTP reales y PostgreSQL se ejecutan separadamente. H no es generaci?n de una declaraci?n federal real. No se midieron clicks/tiempos de humanos, costos o porcentajes de ahorro.

Capturas, JSON de casos, comandos y JUnit se preservan fuera de Git en el directorio temporal del sprint; no contienen documentos privados. **GO local; integraci?n pendiente de gates remotos y revisi?n del PR.** Primero debe integrarse PR3 y luego actualizarse la base de este PR a la rama can?nica. No se hace merge ni deploy en este sprint.
