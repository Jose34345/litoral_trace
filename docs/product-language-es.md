# Lenguaje de producto — Litoral Trace

Este documento define el vocabulario visible que debe utilizar la interfaz de Litoral Trace.

## Descriptor principal

**Litoral Trace — Trazabilidad de origen y cadena de custodia**

La propuesta debe explicarse desde la operación: origen y proveedor → recepción → lote industrial → transformación → producto terminado → despacho/venta → genealogía → dossier de origen.

EUDR, análisis geoespacial, integridad y auditabilidad son capacidades y contextos de uso; no reemplazan la descripción principal del producto.

## Vocabulario canónico

| Evitar como etiqueta principal | Usar en la interfaz |
| --- | --- |
| Dashboard | Inicio / Resumen operativo |
| Imports | Carga masiva / Importación masiva de lotes |
| Vault | Documentos y evidencias |
| Regional Intelligence | Contexto regional de origen |
| Compliance Intelligence | Trazabilidad de origen |
| Compliance Workspace | Espacio de trazabilidad / cadena de custodia |
| Buyer-facing dossier | Dossier para el comprador / expediente del despacho |
| Source lot | Parcela o lote de origen |
| Shipment | Despacho |
| Lineage | Genealogía / trazabilidad de origen |
| Input / Output | Entrada / Salida |
| Allocation | Atribución de origen |
| Public ID | Identificador técnico, sólo cuando sea necesario |
| Job | Tarea, cuando sea visible para el usuario |
| Tenant | Organización |
| RBAC | Permisos por rol |
| Server-side | Validación en Litoral Trace / validación previa |

## Términos técnicos permitidos

Pueden permanecer cuando aportan precisión o interoperabilidad y están contextualizados: **EUDR, NDVI, PDF, JSON, GeoJSON, SHA-256 y MIME**.

Los nombres internos de enums, códigos de error, claves de API y estados de persistencia no deben ser la etiqueta principal de una pantalla. Cuando sean útiles para auditoría deben aparecer como información técnica secundaria.

## Reglas de comunicación

1. **Español primero.** Toda superficie comercial y operativa debe ser comprensible en español sin depender de términos ingleses.
2. **Explicar el valor antes que la implementación.** No presentar tenant, RBAC, idempotencia, arquitectura server-side o nombres de fases internas como propuesta de valor.
3. **No inventar datos.** Ningún consumo, cuota, vencimiento, cliente, CUIT, contacto o métrica con apariencia productiva puede ser ficticio salvo que la pantalla esté marcada explícitamente como demostración.
4. **No sobreafirmar cumplimiento.** Litoral Trace documenta origen, cadena de custodia y evidencia. No debe presentarse como certificadora ni afirmar integración o aceptación oficial EUDR/TRACES sin validación específica.
5. **Geografía no equivale a riesgo.** El contexto regional ayuda a organizar la evidencia; no es una conclusión de riesgo para una operación, proveedor, parcela o despacho.
6. **Auditabilidad visible, complejidad secundaria.** SHA-256, identificadores y formatos técnicos pueden estar disponibles en detalles avanzados sin dominar la experiencia cotidiana.

## Estados visibles

Los estados internos deben traducirse en presentación. Ejemplos:

- `ACTIVE` → Activo
- `CLOSED` → Cerrado
- `VOID` → Anulado
- `DRAFT` → Borrador
- `POSTED` → Registrado
- `available` → Disponible
- `pending_upload` → Pendiente
- `upload_failed` → Carga fallida
- `delete_pending` → Eliminación pendiente
- `delete_failed` → Eliminación fallida
- `completed` → Completada

Los valores internos pueden mantenerse sin cambios en base de datos y API cuando formen parte de contratos existentes; la localización corresponde a la capa de presentación.
