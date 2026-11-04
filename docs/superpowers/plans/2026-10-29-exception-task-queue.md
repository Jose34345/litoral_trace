# P0 Exception Work Queue — plan y diseño

Objetivo: mostrar trabajo documental por acción sin cambiar autoridad, reglas ni escritores.
Especificación: solicitud ENTREGA 3 proporcionada por el usuario (2026-10-29).
Arquitectura: un modelo de presentación agrupa los campos ya proyectados por fase/intervención, entidad/línea y documento exacto. Un fragmento reutiliza las decisiones por campo y el inspector PDF.js existente. Se descartaron agrupaciones por similitud y nuevos escritores/batch porque perderían identidad o autoridad.

- [x] Baseline: contratos actuales de revisión, contexto original, pre-entry y rutas.
- [x] Rojo: render real para grupos, original insuficiente, entrada futura, conflicto y re-render HTMX.
- [x] Implementar módulo de presentación exception_task_queue.py y hooks GET/HTMX.
- [x] Reemplazar tabla de pendientes por tarjetas y acciones secundarias progresivas; conservar revisión pareada autorizada.
- [x] Verificar contratos de decisiones, CSRF, identidad, auditoría, exportación y visor existente.
- [ ] Revisar diff y métricas reproducibles. E2E móvil sólo si hay navegador/runner local disponible.

Restricciones: ningún cambio V2/flags, RLS, persistencia, regulación o exportación; sin dependencias nuevas. Identidad y decisiones por campo, conflictos visibles, ninguna confirmación cuando quick_accept_eligible la prohíbe. Solicitudes sólo texto para copiar. La entrada futura continúa bloqueando filing según readiness existente.
