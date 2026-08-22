# UX10-D — Operaciones de cadena de custodia

## Objetivo

Hacer operable desde la interfaz web la cadena de custodia industrial que P1A–P1C ya modelan y validan, sin crear un segundo motor de inventario o genealogía.

## Contrato de arquitectura

UX10-D separa dos estados deliberadamente:

1. **Borrador recuperable**: la UI persiste la intención de recepción, proceso o despacho como `DRAFT`.
2. **Transición contable**: únicamente `TraceabilityLedgerService.post_event()` y `dispatch_shipment()` pueden modificar el ledger derivado.

La UI no calcula ni persiste `current_stock`. El saldo se deriva de salidas `POSTED` menos entradas `POSTED` menos ítems de despachos `DISPATCHED`.

## Flujos iniciales

- recepción desde parcela/rodal registrada → lote `RAW_MATERIAL`;
- transformación, mezcla, división o reempaque → lotes `INTERMEDIATE` o `FINISHED_GOOD`;
- preparación de despacho → `DRAFT`;
- despacho irreversible → `DISPATCHED` mediante P1B.

`ADJUSTMENT` queda fuera del flujo inicial de UI porque requiere una política operativa/auditora más específica.

## Seguridad y permisos

- `traceability:operate`: crear y contabilizar recepciones/procesos, preparar despachos;
- `traceability:dispatch`: ejecutar el acto de despacho;
- `superadmin`, `admin` y `manager`: ambos permisos;
- `auditor` y `cliente`: sólo lectura, sin operaciones.

Todos los identificadores de lote, evento, despacho y origen se resuelven dentro del `organization_id` autenticado. Las escrituras POST usan CSRF browser-bound.

## Fail-closed

El ledger existente conserva la autoridad para rechazar:

- stock insuficiente;
- doble consumo concurrente;
- lotes inactivos;
- operación anterior a la producción;
- recepción sin parcela de origen;
- unidades inconsistentes;
- conversiones M3/KG/TON sin perfil documental explícito;
- eventos o despachos en estado incompatible.

Cuando una contabilización falla, el `DRAFT` no se transforma en evidencia cerrada y permanece disponible para revisión.

## Caso de aceptación Corrientes

- recepción A: 100 M3;
- recepción B: 80 M3;
- transformación: 70 M3 A + 30 M3 B → 65 M3 terminado;
- merma: 35 M3;
- rendimiento: 65%;
- saldos luego del proceso: A 30 M3, B 50 M3, terminado 65 M3;
- despacho: 60 M3;
- saldo terminado final: 5 M3.

La atribución de origen posterior sigue siendo responsabilidad de P1C y, ante mezcla homogénea, utiliza la convención explícita `PROPORTIONAL_INPUT_ALLOCATION`; UX10-D no afirma identificación física de fibras.

## Tiempo operacional

Los controles HTML `datetime-local` del primer flujo regional se interpretan en `America/Argentina/Cordoba` y se normalizan a UTC antes de persistir. Este supuesto se muestra al usuario y no se usa para inferir horarios en APIs externas.
