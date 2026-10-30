# DI-02 — Evidencia de implementación (2026-10-29)

Estado: implementación preparada para revisión. GO local focal; NO-GO de merge hasta gates de PostgreSQL/RLS, CI remoto y revisión independiente a cargo del coordinador. Sin merge ni despliegue.

## Estado inicial y causa raíz

Base real: `8b909fa4488e0d0017ce5c855c0e7aa1622ccc10`, rama aislada `fix/p0-di02-exact-sku-identity`. Worktree inicialmente limpia. DI-01/DI-03 ya integrados en la base verificada por el coordinador.

Ruta trazada:

1. `multi_agent/specialist_runtime.py` transporta valores candidatos y sidecars observados, sin identidad canónica.
2. `specialized_shadow._verify_candidates` verifica el valor contra Engine 2 y valida la identidad en `_verified_row_sidecar`.
3. La validación anterior usaba subcadenas tanto con tabla/fila como con texto independiente. El sidecar `ACT-TRAY-18` sobrevivía sobre la fuente sintética `SKU OTHER-ACT-TRAY-18 Species Pinus taeda`.
4. `bind_line_items` daba prioridad a ese sidecar incorrecto; además `_normalize_token` podía borrar caracteres significativos del SKU.
5. Admission comprobaba evidencia/rol/autoridad/forma de la identidad, pero recibía una identidad falsamente aceptada. Fusion agrupa por esa identidad; no vuelve a comprobar su correspondencia documental.
6. El contrato de source claims verifica su propio anchor y conserva el estado CANDIDATE. Su patrón anterior tampoco consideraba `+` como carácter significativo ni rechazaba labels contradictorios en el mismo anchor.
7. La preparación de publicación especializada y la reconciliación anterior a Canonical ShipmentTruth consumen claves explícitas. La publicación final sigue siendo su frontera existente, sin autorización humana implícita.

## Corrección

`lacey_engine/sku_identity.py` concentra comparación completa, gramática y detección determinista de identificadores explícitos: label SKU o SKU líder de fila. Mayúsculas ASCII y whitespace exterior son normalizaciones de comparación; `- _ . / +`, incluso separadores finales, permanecen significativos. Delimitadores documentados: whitespace, dos puntos, coma, punto y coma y paréntesis/corchetes/llaves. No se elimina puntuación para construir equivalencias. Grafías no admitidas no se truncan a un identificador válido.

El verificador descarta sidecars parciales o contradictorios conservando una tabla/fila válida. Binding usa la identidad completa visible; múltiples identificadores explícitos conservan identidad local. Source claims distingue valor soportado de entidad vinculada. Admission, materialización especializada y reconciliación cuantitativa comparten la gramática para conservar asociaciones legítimas con `+`. La versión de line binding cambia para impedir reutilización de resultados calculados con la regla insegura; no se cambian cache, OCR ni migraciones.

Los valores originales, textos fuente, documento, página, span y tabla/fila se mantienen. Dos SKU explícitos diferentes siguen separados aunque tengan descripción o HTS iguales. No se modificaron source sets, RLS, revisión, reglas regulatorias ni Canonical ShipmentTruth.

## Pruebas ejecutadas y evidencia

Ejecuciones offline con Python local, sin proveedores externos ni bases de producción. Se retiraron variables URL/key/token/secret/password/credential del proceso y se desactivaron dotenv, plugins automáticos y PostgreSQL. El runner bloquea conexiones no locales. Logs detallados sólo en TEMP, con datos sintéticos o fixtures autorizados.

| Etapa | Resultado | Log TEMP |
|---|---|---|
| Baseline binding/Pack3/specialized | 33 passed | `di02-baseline.log` |
| Regresión original + variantes antes de código productivo | 19 failed, 9 passed | `di02-red.log` |
| Contradicciones y source claims antes de corrección | 25 failed, 30 passed | `di02-red-expanded.log` |
| Primera corrección + baseline | 88 passed | `di02-green.log` |
| Gramática downstream y case/sparse antes de ajuste | 3 failed, 32 passed | `di02-downstream-red.log` |
| Seam preciso de reconciliación cuantitativa antes de ajuste | 1 failed, 34 deselected | `di02-canonical-reconciliation-red.log` |
| SKU líder contradictorio antes de corrección | 3 failed, 37 deselected | `di02-leading-contradiction-red.log` |
| Focal final | 96 passed | `di02-final-focused.log` |
| Gate offline final, incluyendo regresión original externa sin modificar | 550 passed, 5 skipped | `di02-final-offline-gate.log` |

El gate final ejecutó `tests/lacey_engine` completo y los archivos `test_us_lacey_identity_memory`, `test_us_lacey_reusable_evidence`, `test_assurance_v2_claim_bridge`, `test_assurance_v2_documentary_claim_binding`, `test_assurance_v2_real_pdf_packs`, `test_assurance_v2_pack2_candidate_triage`, `test_assurance_v2_p0_p1_real_pack_contracts`, `test_golden_fixture_pack_2`, más el test original `test_sku_suffix_is_not_exact_source_identity` de la auditoría externa. No se activó Assurance V2 en la aplicación.

Cobertura: exacto/case/whitespace; SKU contenido/prefijo/sufijo; separadores; proveedores distintos; HTS y descripciones iguales; multi-documento y orden inverso; falta de SKU/ambigüedad local; conservación de procedencia; candidatos soportados sin entidad verificada; revisión/hash/versión posteriores; reutilización autorizada y rechazo de historia conflictiva/expirada. Incluye Pack1/Pack2 reales, golden, masa neta 288 kg, line binding, fusion, canonical y adversariales existentes.

Los 5 skips son explícitos: tres casos `test_gate1_document_engine.py` requieren `import_info_wood_brokerage_real.pdf`, fixture privada ausente; dos casos PostgreSQL de `test_golden_fixture_pack_2.py` requieren la base aislada. No se contaron como aprobados.

## Pendientes de release

El coordinador ejecuta pytest global, PostgreSQL/RLS/source sets/CAS/reuse E2E y gates remotos, además de revisión independiente. Este implementador no hizo commit, push, PR, merge ni despliegue. No se agregaron dependencias, secretos o documentos privados.


## Revision independiente - cierres adicionales 2026-10-30

La revision detecto tres rutas concretas adicionales dentro de la identidad SKU:

1. `_row_identity_text.casefold()` convertia una fuente Unicode a un alias ASCII antes del helper. Con fuentes sinteticas `ACT-` + U+00DF/U+017F/U+212A + `-18`, los sidecars ASCII SS/S/K sobrevivian tanto con fila como sin ella. El matching general de citas puede conservar su casefold; la identidad se valida ahora sobre el texto original de Engine 2.
2. `SKU No:`, `SKU No.:`, `SKU Number:` y `SKU ID:` se capturaban como SKU NO/NO./NUMBER/ID. La gramatica consume el modificador completo y su puntuacion; no puede retroceder y fabricar una identidad a partir del propio modificador. Tambien se conserva `SKU #:`. Dos productos distintos con cada label permanecen separados hasta fusion.
3. Una cita de IA recortada dentro de `OTHER-ACT-TRAY-18` podia verificar literalmente la subcadena `ACT-TRAY-18` y volver a admitir la identidad incorrecta. La verificacion especializada comprueba ahora el SKU citado contra el ancla original del mismo documento/pagina/fila. Una cita recortada no conserva `evidence_verified`; admission la rechaza sin reescribir el texto ni las coordenadas. Las citas parciales legitimas sin SKU conservan el valor verificado cuando el ancla y su identidad son coherentes. Varias anclas incompatibles no pueden autorizar una identidad por orden.

Rojo reproducido antes de producto: **11 failed, 41 deselected** para Unicode y labels; despues **112 passed** en exact SKU/source claims/line binding/quantitative binding/shadow. Segundo rojo: **5 failed, 1 passed, 52 deselected** para citas adulteradas y control parcial legitimo. Verde final focal: **118 passed**, 4.11s, sin warnings. La ejecucion inicial del primer verde uso un path de test inexistente y no corrio pruebas; se corrigio la ruta sin alterar expectativas.

Los cambios de esta revision se limitan a `sku_identity.py`, el seam de verificacion en `specialized_shadow.py`, pruebas sinteticas y este contrato. No cambian cache/OCR, fuente original, canonical, DB ni regulacion. Los gates completos y PostgreSQL anteriores deben repetirse sobre este estado final antes del dictamen de merge.


Cierre de abstencion final: un label explicito con token no admitido (por ejemplo `ACT-` + U+00DF + `-18` o `ACT#18`) no puede ignorarse para usar un alias literal posterior `Legacy ACT-SS-18`/`ACT-18`. El veto compartido en `has_exact_source_identity` y `derive_line_item_key` conserva identidad local ROW/FP y candidato documental sin fabricar una clave SKU. Source claims permanece UNBOUND en ese caso. La deteccion negativa lee el token completo unicamente para abstener; no amplia la gramatica positiva. Se conservan los SKU legitimos que comienzan `SKU-1`, distinguiendo el keyword de label de un prefijo del producto.

Ultimo rojo: **4 failed, 4 passed, 58 deselected**; los cuatro controles positivos demuestran que el SKU explicito admitido sigue prevaleciendo sobre un alias Legacy diferente. En la primera integracion del veto, siete tests existentes de shadow detectaron una confusion del keyword `SKU` con el identificador legitimo `SKU-1`; se corrigio el limite del keyword, sin cambiar expectativas. Verde focal posterior: **126 passed**, 3.62s. El gate offline de los tres cierres anteriores habia dado **567 passed, 5 skipped**, 39.94s; se repite a continuacion sobre el veto final. Una ejecucion anterior tuvo error de coleccion por no incluir tests/ en PYTHONPATH; no fue un fallo de producto y la repeticion con ambas carpetas de fixtures produjo el resultado citado.

Gate offline definitivo posterior al veto compartido y Source Claims: **575 passed, 5 skipped**, 32.90s, exit 0; `git diff --check` exit 0. Mismos archivos del gate ampliado detallado arriba, sin el test externo adicional de la auditoria (su reproducer esta cubierto por el test P0 interno). Los skips siguen siendo tres casos que requieren el PDF privado ausente y dos casos PostgreSQL de golden Pack 2. Dictamen de revision sobre las rutas inspeccionadas: sin hallazgos Critical/Important abiertos conocidos tras los cierres; GO local, pendiente de gates globales/PG/remotos actualizados por el coordinador. Runtime y tests congelados; sin commit/push propio.


## Segundo pase independiente: identidad local y sidecar explicito

La revision read-only independiente encontro dos Important adicionales y entrego seis reproducers externos sin modificar el runtime: `ACT#18` y `ACT@18` quedaban con el mismo FP al borrar puntuacion, y un sidecar ya verificado `ACT.TRAY.18`/`ABC123` caia a ROW porque se exigia separador.

Se agregaron seis contratos internos antes de producto: **3 failed, 3 passed, 66 deselected**. La correccion queda en `line_binding.py`: el fallback FP de un SKU explicitamente no admitido conserva puntuacion y Unicode al calcular su identidad local, normalizando solo ASCII case y whitespace; la fila ROW existente no cambia. Para sidecars explicitos ya verificados no se exige separador, manteniendo la gramatica admitida y los otros controles lexicales. La inferencia heuristica de SKU lider conserva su restriccion anterior; no se amplian fuentes de autoridad ni reglas de inferencia.

Verde: **138 passed**, 3.97s, incluyendo 132 contratos focales internos y los seis reproducers externos originales. Se conservan fuente, cita, documento y evidencia; dos productos locales distintos sobreviven por separado a fusion. No se autorizan SKU no admitidos y no cambia canonical/OCR/cache/DB.

Segundo pase, gate ampliado definitivo: **587 passed, 5 skipped**, 40.39s, exit 0, incluyendo los seis reproducers independientes originales. `git diff --check` exit 0. El revisor independiente repitio los 138 focales y confirmo ambos Important cerrados, sin Critical/Important abiertos en las invariantes inspeccionadas. Codigo y tests congelados; quedan los gates globales/PostgreSQL/remotos de coordinacion antes de merge. Sin commits propios.

## Cierre de gates coordinados ? 2026-10-30

Sobre el c?digo congelado posterior a ambos pases de revisi?n: suite general offline **2836 passed, 366 skipped, 0 failures/errors**; PostgreSQL real aislado **70 passed, 0 skipped** (incluye source sets, canonical, RLS, identidad/reutilizaci?n) y worker/E2E **10 passed, 0 skipped**, con Pack 1 y Pack 2. La suite general incluye los tres contratos del PDF privado mediante su ruta local original; no se a?adi? ni copi? a Git. Las 366 omisiones corresponden al entorno general sin servicios opcionales habilitados; no se cuentan como pases ni se ocultaron con nuevos skips.

Revisi?n independiente final: **138 passed** incluidos los seis reproducers externos intactos; ambos Important cerrados, sin Critical/Important abiertos conocidos. Sintaxis de 980 archivos Python, TOML, `git diff --check`, 36 blobs de fixtures intactos y Alembic con ?nico head `079_us_lacey_assurance_v2_claim_snapshots` verificados. No se modifican DI-01, DI-03, cach?s, OCR, migraciones, reglas regulatorias ni configuraci?n productiva. Logs, JUnit y manifiestos permanecen fuera de Git.

**GO local de DI-02; integraci?n pendiente de CI/PostgreSQL remotos y revisi?n del PR.** Estos resultados del caso adversarial no constituyen tasas globales de precisi?n ni una medici?n de ahorro de tiempo humano.


## Hallazgos remotos PR 475 sobre 8102ea9

Se reprodujeron dos defectos del pase remoto sin reset/rebase ni perdida de cambios. Una descripcion comercial identica hacia que `_reconcile_to_unique_sku` revirtiera el veto de un SKU explicitamente no admitido y promoviera su identidad local al SKU de otro documento. El veto se mantiene ahora tambien durante esa reconciliacion; no se agregan reglas de correspondencia por descripcion. En los tests de cadena completa, ROW conserva su identidad local admitida y ambas entidades sobreviven en fusion; FP conserva su identidad local pero admission lo bloquea por LINE_BINDING_AMBIGUOUS, sin convertirlo en SKU.

El segundo caso usaba whitespace Unicode alrededor de un SKU ASCII completo. La cita con espacio ASCII estaba validada contra la fuente, pero `re.ASCII` restringia los delimitadores whitespace en los helpers. Solo el texto temporal de comparacion normaliza ahora whitespace con split/join; los regex conservan su alfabeto ASCII, las fuentes y citas quedan literales, y whitespace dentro del identificador nunca se concatena para fabricar un SKU.

Rojo previo al runtime: **12 failed, 1 passed, 72 deselected**. Cubre reconciliacion con/sin fila y orden inverso, NBSP y NNBSP antes/despues del SKU, y el control negativo de whitespace interno. Primer cierre P1: **4 passed, 81 deselected**. Verde conjunto focal: **145 passed**, 4.55s. Se aplica el skill receiving-code-review y se verifican los hallazgos antes de corregirlos. Una aprobacion de herramienta interrumpida por cuota no ejecuto el ajuste de whitespace; tras indicacion de continuar se inspecciono el diff existente y se completo unicamente ese ajuste.

Runtime y tests congelados para nueva revision independiente. Sin commits propios, sin merge ni despliegue; gates globales/PostgreSQL/remotos a cargo del coordinador.


### Cita parcial: transporte negativo desde el ancla original

La primera correccion del pase remoto conservaba el veto cuando la cita incluia el label SKU, pero la revision independiente demostro el mismo P1 con una cita parcial valida `Description Solid wood tray`: el label no admitido solo existia en el ancla original. El resultado focal previo de 145 pruebas y el gate de 600 passed/5 skipped no cerraban ese caso y quedan supersedidos para el dictamen final.

Se agrega `CandidateEnvelope.source_sku_identity_blocked: bool = False`, metadata negativa compatible, derivada exclusivamente del ancla Engine 2 original verificada. No es una configuracion, flag de activacion ni autoridad positiva. El verificador mantiene evidencia valida, cita literal y procedencia; el veto impide que derive/reconcile concedan una identidad SKU por una descripcion coincidente, aunque la cita no incluya el label. ROW sigue local y admisible cuando esta probado; FP sigue bloqueado por admission como identidad ambigua.

El serializer incluye el booleano y el codec de rehidratacion lo conserva. Un valor mal tipado causa cache miss; no se convierte texto a booleano ni se pierde la senal por normalizacion. El default False conserva constructores anteriores. `LINE_BINDING_VERSION` pasa a `lacey_document_local_line_binding_v3_source_sku_abstention`, consumido por los fences existentes de engine version y computation fingerprint: los artefactos previos v2 no se reutilizan para la nueva semantica. No se modifico el modulo de cache ni su arquitectura.

Auditoria de consumidores: specialist_runtime construye envelopes con el default y no permite al proveedor otorgar autoridad mediante esta metadata; las transformaciones de binding/fusion usan envelopes inmutables/replace; specialized_shadow es el serializer y codec existentes; source claims y proyeccion siguen consumiendo las identidades source-local/evidencia ya verificadas. No se agregan escritores, nuevas claves SKU, nuevas reglas regulatorias ni una procedencia paralela.

Rojo adicional: **6 failed, 4 passed, 92 deselected**, incluyendo cuatro variantes de cita parcial con/sin ROW y orden inverso, default compatible/version fence y codec. El fixture de codec se ajusto para declarar COMMERCIAL INVOICE en su fuente sintetica y activar el router real; se confirmo su rojo por metadata ausente antes del cambio de producto. Verde posterior: **162 passed**, 3.82s, incluyendo los once reproducers externos intactos del pase remoto. Se prueba roundtrip serializer/cache, rebinding del envelope rehidratado, rechazo de tipo incorrecto y que el engine fingerprint v3 difiere de v2. Runtime y tests congelados para repetir gates finales.

Gate ampliado final del pase remoto: **617 passed, 5 skipped**, 33.64s, exit 0, con ambos archivos externos de revision intactos; `git diff --check` exit 0. El primer pase ampliado habia dado 616 passed y un fallo en la lista exacta de campos de CandidateEnvelope: se actualizo ese contrato declarado para el booleano autorizado, agregando checks de default False, inmutabilidad y ausencia del campo en AICandidate, sin quitar validaciones. Revision independiente final: **168 passed**, 1.90s, P1/P2 cerrados y sin Critical/Important abiertos en las invariantes inspeccionadas. Runtime y tests finales congelados; sin commits propios. Los cinco skips siguen siendo tres fixtures privados ausentes y dos casos PostgreSQL Golden Pack 2. Gates globales/PG/remotos finales pendientes del coordinador antes de merge.

### Coordinaci?n: gates del transporte negativo v3

El PostgreSQL aislado posterior a la propagaci?n desde el ancla original aprob? **70 gates + 10 worker/E2E**, cero fallos y cero omisiones. Se revalidaron sintaxis de 980 Python, TOML, Alembic ?nico y 36 fixtures intactos. La revisi?n independiente final (168 pases) incluye los reproducers externos sin modificar, y el gate ampliado final del implementador dio 617 pases/5 omisiones.

La suite general y CI remoto se est?n repitiendo sobre el contrato final, incluido el test de la lista expl?cita de campos de CandidateEnvelope (default False, inmutabilidad y AICandidate intacto). No se presenta el gate anterior como aprobaci?n de esta sem?ntica nueva. **NO-GO para merge hasta adjuntar el resultado general y los checks remotos del SHA actualizado.**
