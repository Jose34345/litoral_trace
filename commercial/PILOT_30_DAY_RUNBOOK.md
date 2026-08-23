# Litoral Trace — Piloto comercial de 30 días

## Objetivo

Convertir un prospecto forestal del NEA en un usuario activo de Litoral Trace con una operación verificable de punta a punta y métricas que permitan decidir continuidad paga al día 30.

Este runbook no depende del smoke remoto EUDR ACCEPTANCE y no habilita LIVE EUDR. El piloto debe usar documentos, lotes y operaciones reales del cliente cuando exista autorización para hacerlo.

## Día 0 — Preparación comercial

Responsables sugeridos del cliente: dueño/gerencia + comercio exterior/calidad + un usuario operativo.

Reunir antes de la sesión inicial:
- razón social, CUIT y contacto administrador;
- 1–5 lotes/rodales representativos con ubicación;
- 1 proveedor/productor real;
- documentos de origen disponibles;
- un flujo real de recepción y transformación;
- un despacho internacional reciente o representativo;
- referencias ARCA/SIM disponibles;
- evaluación fitosanitaria del mercado de destino;
- si el destino es UE, datos necesarios para preparar el candidato EUDR sin inventar EORI, HS, especies o conclusiones de riesgo.

## Día 1 — Activación

1. Superadmin crea organización, licencia piloto y usuario administrador.
2. Cliente inicia sesión y abre **Preparar piloto**.
3. Carga lotes por la vía más rápida disponible: carga masiva o captura existente.
4. Sube al menos una evidencia al Vault.
5. Verificar que el estado pasa de `NOT_STARTED` a `IN_PROGRESS`.

Criterio de éxito del día 1: el cliente puede ingresar sin asistencia de desarrollo y ve faltantes concretos con enlaces accionables.

## Días 2–7 — Cadena de custodia

1. Registrar una recepción vinculada a lote de origen y llevarla a `POSTED`.
2. Registrar una transformación real o representativa y llevarla a `POSTED`.
3. Revisar genealogía y balance de cantidades.
4. Corregir identificadores/documentos faltantes detectados durante el flujo.

Criterio de éxito: Litoral Trace reconstruye la relación lote → recepción → transformación sin intervención en base de datos.

## Días 8–14 — Despacho y expediente

1. Crear un despacho internacional con material trazado.
2. Llevar el despacho a `DISPATCHED`.
3. Completar expediente Corrientes/ARCA/SIM según perfil forestal aplicable.
4. Vincular evidencias Vault al despacho.
5. Completar la evaluación fitosanitaria: `NOT_REQUIRED`, `PAPER` o `EPHYTO`, siempre con fundamento/evidencia cuando corresponda.

Criterio de éxito: expediente exportador y fitosanitario muestran `READY` o faltantes concretos y verificables.

## Días 15–21 — EUDR y Control de Salida

Para destinos UE:
1. completar el candidato DDS local con operador UE, EORI cuando corresponda, HS, producto, cantidad, país/fechas de producción, especies cuando aplique y evaluación de riesgo real;
2. alcanzar `CONFORMANCE_READY`;
3. no afirmar cumplimiento legal por ese estado;
4. no ejecutar LIVE.

Para todos los destinos:
1. abrir Control de Salida;
2. revisar trazabilidad, evidencia, expediente y faltantes;
3. generar/revisar dossier documental existente cuando el flujo lo permita.

Criterio de éxito: el despacho puede explicarse a un comprador/auditor sin reconstruir manualmente la historia en Excel, Drive y WhatsApp.

## Días 22–27 — Uso operativo real

El cliente repite el flujo con una segunda operación o actualiza la primera con nuevos documentos.

Medir:
- tiempo de alta de lote/proveedor;
- tiempo de reconstrucción de origen antes/después;
- cantidad de documentos encontrados sin búsqueda manual;
- faltantes detectados antes del despacho;
- tiempo necesario para preparar expediente de comprador/importador;
- usuarios activos y frecuencia de uso;
- errores operativos o pasos que todavía requieren soporte de Litoral Trace.

No convertir estas métricas en claims comerciales sin evidencia del propio piloto.

## Días 28–30 — Cierre y conversión

Reunión de cierre con tres preguntas:
1. ¿Qué tarea que antes requería Excel/Drive/WhatsApp quedó objetivamente más rápida o controlada?
2. ¿Qué riesgo documental/operativo fue detectado antes de que llegara a exportación?
3. ¿Qué volumen mensual de lotes, proveedores, documentos y despachos debería manejar Litoral Trace si continúa?

Entregables:
- captura/registro del estado **Preparar piloto**;
- un despacho con Control de Salida revisado;
- dossier/evidencia disponible;
- lista de gaps observados durante el piloto;
- propuesta comercial correspondiente al volumen real.

## Criterio `PILOT_READY`

La plataforma lo calcula automáticamente y exige siete hitos:
1. organización/usuario/licencia activos;
2. al menos un lote de origen;
3. al menos un documento Vault disponible;
4. al menos una recepción `POSTED`;
5. al menos una transformación `POSTED`;
6. al menos un despacho internacional `DISPATCHED` con material;
7. expediente exportador + fitosanitario `READY` y, si el destino es UE, candidato EUDR `CONFORMANCE_READY`.

El smoke remoto EUDR ACCEPTANCE no es requisito para `PILOT_READY`. Sigue siendo un gate técnico separado antes de cualquier futura integración LIVE.

## Trigger posterior al primer cliente pago

Una vez confirmado el primer cliente pago, ejecutar el issue de infraestructura #67 antes de ampliar uso comercial: worker persistente, secretos correspondientes y extensión del restore history de Neon según el runbook vigente.
