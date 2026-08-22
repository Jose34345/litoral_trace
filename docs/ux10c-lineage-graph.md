# UX10-C — Grafo visual de genealogía

## Objetivo

Hacer legible la genealogía de un despacho en una sola vista, usando exclusivamente la información presentada por el flujo P1C/P1D existente.

## Regla de arquitectura

El grafo es una representación server-rendered. No reconstruye genealogía en JavaScript, no consulta una API paralela y no persiste una segunda proyección del dominio.

## Lectura visual

1. **Origen atribuido:** parcelas, proveedores, cantidades, participaciones y georreferencia.
2. **Cadena industrial documentada:** eventos reales con entradas, salidas, pérdida y rendimiento.
3. **Lotes comerciales:** volumen despachado, volumen atribuido, volumen no resuelto y contribuciones por origen.
4. **Despacho y expediente:** referencia final y acceso a los artefactos P1E.

## Mezclas

Cuando el payload declara `PROPORTIONAL_INPUT_ALLOCATION`, la interfaz lo explica como una convención contable de trazabilidad para entradas homogéneas. No se presenta como identificación física de fibras individuales.

## Fail-closed

Si `complete=false`, el grafo muestra una brecha explícita, el volumen no resuelto y las incidencias conocidas. No debe dibujarse ni describirse una cadena cerrada cuando falta evidencia.

## Alcance regulatorio

El grafo documenta origen y cadena de custodia. No afirma certificación automática EUDR, aprobación TRACES NT ni reemplazo de la debida diligencia del operador.
