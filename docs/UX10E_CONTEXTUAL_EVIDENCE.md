# UX10-E · Evidencia contextual de cadena de custodia

## Objetivo

La evidencia documental debe verse y gestionarse dentro del eslabón que respalda, sin convertir la bóveda en un segundo sistema de trazabilidad ni obligar al operador a perder contexto.

## Principios de experiencia

1. **Un único archivo, múltiples vínculos legítimos.** Vault conserva el objeto privado y su SHA-256. UX10-E sólo registra qué eslabón respalda.
2. **Contexto primero.** El usuario elige origen, movimiento, lote industrial o despacho y ve únicamente la evidencia de ese eslabón.
3. **Carga directa.** Un usuario autorizado puede subir y vincular un documento sin abandonar el flujo.
4. **Reutilización.** Un documento ya disponible en Vault puede vincularse sin duplicarse.
5. **Integridad sin ruido.** SHA-256, MIME y tamaño permanecen visibles bajo detalles avanzados, no como información primaria.
6. **Historial append-only.** Desvincular no elimina el archivo ni la relación histórica; registra `unlinked_at`.
7. **Cobertura factual.** El porcentaje representa eslabones con al menos una evidencia registrada. No equivale a certificación ni a cumplimiento normativo automático.
8. **Navegación corta.** `/operations`, `/evidence`, `/traceability` y `/vault` deben estar conectados mediante acciones visibles; el operador no debe volver al menú lateral para completar el ciclo documental.

## Huella Documental Litoral Trace

La firma de UX10-E es la **Huella Documental Litoral Trace**: una lectura verificable del respaldo que acompaña a la cadena `Origen → Movimiento → Lote industrial → Despacho`.

No es un score regulatorio. La huella responde una pregunta operacional y auditable: **qué evidencia está vinculada a qué eslabón y cuál es el hash del contenido que lo respalda**.

Cuando se genera un dossier de despacho:

- P1C sigue siendo la fuente única de genealogía;
- UX10-E selecciona sólo los vínculos documentales pertenecientes a ese grafo;
- el manifiesto buyer-facing incorpora referencias públicas, metadatos relevantes y SHA-256;
- las notas operativas internas y los IDs internos de base de datos no salen al comprador;
- el SHA-256 del manifiesto cambia si cambia la evidencia documental;
- el PDF muestra una sección `HUELLA DOCUMENTAL LITORAL TRACE` después del camino industrial;
- los archivos privados de Vault **no** se incrustan automáticamente dentro del ZIP.

Así, genealogía y cobertura documental permanecen como dimensiones distintas: una cadena puede estar genealógicamente cerrada y todavía mostrar una brecha documental visible.

## Sujetos

- `SOURCE_LOTE`: parcela o rodal de origen.
- `TRACEABILITY_EVENT`: recepción, transformación, mezcla, división o reempaque.
- `TRACEABILITY_BATCH`: lote industrial.
- `SHIPMENT`: despacho o venta.

## Tipos documentales contextuales

- autorización de origen;
- guía forestal;
- remito;
- factura / documento comercial;
- certificado;
- documento de transporte;
- evidencia geoespacial;
- declaración de proveedor;
- otra evidencia.

La lista describe la función del documento dentro de la cadena. No pretende definir por sí sola requisitos regulatorios universales.

## Seguridad

- lectura: `LOTE_READ` + `VAULT_READ`;
- gestión de vínculos: `TRACEABILITY_EVIDENCE`;
- carga de nuevos archivos: además `VAULT_UPLOAD`;
- todas las escrituras browser usan CSRF;
- FKs compuestas y RLS impiden vínculos cross-tenant;
- runtime no recibe privilegio DELETE sobre `traceability_evidence_links`.
