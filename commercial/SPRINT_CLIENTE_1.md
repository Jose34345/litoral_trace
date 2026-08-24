# Litoral Trace — Sprint comercial Cliente #1

## Objetivo

Conseguir el primer cliente pago de Litoral Trace mediante una venta founder-led y un piloto de 30 días que demuestre valor sobre una operación forestal real.

Este sprint no tiene como objetivo agregar nuevas funciones grandes. El producto actual debe venderse primero. Sólo se corrigen bugs o fricciones pequeñas que impidan una demo o un piloto.

## Hipótesis comercial a validar

Litoral Trace crea valor cuando una empresa forestoindustrial mediana compra materia prima de múltiples proveedores, transforma esa materia prima y luego necesita reconstruir con rapidez el origen, la cadena de custodia y la evidencia documental de un despacho concreto.

El mensaje principal no es “software EUDR”. Es:

> De la parcela al embarque: demostrá de dónde salió cada producto y entregá a tu comprador o auditor toda la evidencia que lo respalda.

Para empresas con exposición UE:

> Cuando tu comprador necesite EUDR, los polígonos, especies, proveedores y documentos ya están conectados a la operación.

## ICP de este sprint

Prioridad geográfica: Corrientes.

Tipo de empresa:
- aserradero/remanufacturador mediano;
- aproximadamente 20–80 proveedores o combinación relevante de materia prima propia + terceros;
- transformación industrial real;
- exportación o provisión habitual a exportadores;
- suficiente complejidad para que Excel/Drive/WhatsApp/ERP no resuelvan fácilmente la genealogía completa;
- suficientemente mediana para que la compra pueda decidirla dueño/gerencia sin procurement enterprise prolongado.

Evitar como cliente #1:
- microaserraderos sin exportación ni complejidad;
- majors con sistemas internos maduros y procurement largo;
- empresas sin materia prima externa ni necesidad de reconstrucción de origen;
- prospectos que sólo quieren “ver una demo” sin aportar una operación real.

## Lista inicial de prospectos

Orden de ataque recomendado basado en fit + comprabilidad del análisis de mercado de agosto de 2026:

1. NORFOR SA — Corrientes — Gerencia / Comercio Exterior / Calidad.
2. Forestal Las Marías SA — Corrientes — Gerencia / Calidad.
3. TRIPAYN SRL — Corrientes — Dueño / Gerencia / Comercio Exterior.
4. Pampa Wood — Corrientes — Dirección / Comercio Exterior.
5. G3 Maderas SRL — Corrientes — Dueño / Gerencia.
6. Aserradero Puerta de Misiones SA — Misiones — Gerencia / Comercio Exterior.
7. Foresto Industrial Langer — Misiones — Dirección / Calidad.
8. COAMA Sud América SA — Misiones — Calidad / Comercio Exterior.
9. Laharrague Chodorge SA — Misiones — Calidad / Comercio Exterior.
10. Forestal Guaraní SA — Misiones — Gerencia.
11. MAZTER Ind. Maderil SA — Misiones — Calidad.
12. AGUER Maderas SRL — Misiones — Dirección.
13. Carbonex SA — Chaco — Gerencia / Comercio Exterior.
14. Wachnowski Forestal SRL — Chaco — Gerencia.
15. Argecosol SRL — Chaco — Gerencia.
16. Nardelli Exportación SA — Chaco — Dirección.
17. MADERTRAT — Corrientes — Titular.
18. MH Maderas SRL — Misiones — Dirección.
19. GMF Latinoamericana SA — Corrientes — Operaciones / Calidad.
20. T. Hnos SRL — Corrientes — Dueño.

## Fases del sprint

### Fase 0 — Preparación, 1–2 días

Entregables:
- demo de 20 minutos ensayada con un único caso;
- propuesta de piloto de USD 249;
- ficha de una página con problema, alcance y límites;
- planilla/CRM simple con prospectos y estados;
- guion de discovery;
- cuenta demo limpia o tenant piloto listo.

No mostrar veinte módulos. La demo recorre:

parcela → recepción → transformación → despacho → genealogía → evidencia → Control de Salida.

EUDR se muestra al final sólo si el prospecto tiene exposición UE. NDVI y transporte API no son parte de la demo inicial salvo pregunta específica.

### Fase 1 — 20 contactos iniciales, días 1–10

Objetivo mínimo:
- 20 contactos calificados;
- 10 respuestas/conversaciones;
- 5 entrevistas de discovery;
- 3 demos con contexto real;
- 1 propuesta de piloto paga.

Canales:
- contacto directo a dueño/gerente cuando la empresa es mediana;
- Comercio Exterior;
- Calidad/FSC/PEFC;
- Forestal/Compras;
- Administración de exportaciones;
- introducciones vía APICOFOM, AMAC, AFoA, consultores forestales y despachantes.

Mensaje inicial sugerido:

> Estoy trabajando con empresas forestales que necesitan reconstruir de qué lotes y proveedores salió cada despacho. Quiero tomar una operación real de ustedes y ver si hoy podemos armar origen → recepción → transformación → despacho + documentos en menos tiempo que con el proceso actual.

Si hay exposición UE:

> Si además venden a Europa, dejamos preparados polígonos y datos para entregar al comprador; no reemplaza su asesor ni promete cumplimiento legal.

### Fase 2 — Discovery

No preguntar “¿te interesa un software de trazabilidad?”.

Preguntas obligatorias:
1. Mostrame un embarque de hace tres meses: ¿pueden llegar desde la factura/despacho hasta los lotes originales?
2. ¿Cuántas personas hay que llamar para reconstruirlo?
3. ¿Qué datos les está pidiendo hoy el comprador, auditor o certificador?
4. ¿Cómo reciben hoy polígonos y documentos de sus proveedores?
5. ¿Qué es lo último que suele faltar cuando quieren cerrar un expediente?

Preguntas de cuantificación:
- proveedores activos;
- lotes/rodales por mes;
- recepciones por mes;
- transformaciones relevantes;
- despachos internacionales por mes;
- cantidad de personas involucradas;
- tiempo aproximado para reconstruir un despacho;
- herramientas usadas actualmente;
- auditorías/certificaciones;
- destinos de exportación;
- existencia de pedidos de geolocalización/EUDR por compradores.

Criterio para ofrecer piloto:
- existe una operación real que se puede reconstruir;
- participan al menos dos áreas/personas;
- hay materia prima de varios orígenes o transformación;
- existe un dolor observable de documentación/reconstrucción/control;
- el prospecto acepta entregar datos reales o anonimizados y asignar un responsable interno.

### Fase 3 — Demo, 20 minutos

Minutos 0–3: problema y operación objetivo.

Minutos 3–6: lote/parcela y origen.

Minutos 6–10: recepción + transformación.

Minutos 10–13: despacho y genealogía inversa.

Minutos 13–16: Evidence Vault y documentos vinculados al eslabón correcto.

Minutos 16–19: Control de Salida mostrando faltantes concretos.

Minuto 19–20: si aplica UE, mostrar que los mismos datos alimentan el candidato EUDR local; aclarar que ACCEPTANCE no es una DDS legal y LIVE no está habilitado.

Cerrar con:

> Quiero hacerlo con una operación real de ustedes durante 30 días y medir cuánto tiempo y retrabajo ahorra.

### Fase 4 — Oferta de piloto

Precio recomendado: USD 249 una vez.

Alcance:
- 30 días;
- 1 empresa;
- 3–4 usuarios;
- hasta 10 proveedores;
- hasta 20 lotes/parcelas;
- una operación completa, opcional segunda operación;
- hasta aproximadamente 100 documentos;
- import inicial Excel;
- onboarding founder-led;
- sin integración custom durante el piloto;
- 100% del piloto aplicable al primer período comercial si convierte.

KPIs:
- origen identificable: 100% o gaps explícitos;
- recepciones conectadas: >95%;
- inputs/outputs de transformación vinculados: >95%;
- documentos requeridos: >90% o faltantes identificados;
- tiempo de reconstrucción: objetivo reducción >=50%;
- detectar al menos un faltante antes del cierre sería evidencia fuerte;
- al menos 2 usuarios activos;
- responsable interno valora utilidad >=8/10;
- decisión de continuidad antes del día 30.

### Fase 5 — Conversión

Precio objetivo post-piloto inicial: USD 399/mes para el ICP medio, ajustando por proveedores, operaciones e integraciones.

No cobrar principalmente por cantidad de usuarios. La unidad de valor es:
- proveedores activos;
- operaciones/expedientes;
- complejidad de transformación;
- integraciones y soporte.

Entregable de cierre:
- before/after de tiempo;
- cantidad de archivos/personas involucradas antes;
- cantidad de faltantes detectados;
- tiempo de reconstrucción con LT;
- un despacho reconstruido desde producto hasta parcelas;
- propuesta de continuidad.

## Cadencia diaria del fundador

Durante 20 días hábiles:
- 5 nuevos contactos/día;
- 3 follow-ups/día;
- máximo 2 demos/día;
- registrar siempre respuesta, dolor, proveedor count, exportación, decisión y próxima fecha;
- toda feature solicitada se registra como evidencia, no se promete durante la llamada.

## Estados del pipeline

1. IDENTIFICADO
2. CONTACTADO
3. RESPONDIO
4. DISCOVERY
5. CALIFICADO
6. DEMO
7. PILOTO_PROPUESTO
8. PILOTO_PAGO
9. PILOTO_ACTIVO
10. CONVERTIDO
11. PERDIDO
12. NURTURE

Motivos de pérdida obligatorios:
- sin dolor;
- sin presupuesto;
- ERP/certificador suficiente;
- sin exportación/complejidad;
- necesita integración antes de comprar;
- timing;
- competidor;
- no responde;
- otro.

## Reglas de producto durante el sprint

No abrir un gran sprint nuevo por una sola solicitud.

Se permite:
- corregir bugs;
- mejorar texto/UX que bloquee el piloto;
- pequeños imports/mappings;
- documentación;
- soporte de onboarding.

Requiere evidencia de al menos 3 prospectos/clientes para priorizar:
- portal de proveedores;
- nueva integración oficial;
- ERP específico;
- OCR;
- app móvil;
- workflow de aprobación.

## Criterio de éxito del sprint

Éxito fuerte:
- >=1 piloto pago USD 249 dentro de 90 días.

Éxito parcial:
- 4–5 pilotos propuestos y evidencia repetida de un mismo dolor, aunque el cierre se demore.

Señal de revisión de tesis:
- después de ~40 conversaciones calificadas y 4–5 pilotos propuestos, nadie acepta pagar USD 249.

En ese caso no se construye más: se revisa ICP, propuesta de valor y problema principal.
