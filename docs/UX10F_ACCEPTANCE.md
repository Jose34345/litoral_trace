# UX10-F · Contrato de aceptación

El Control de Salida forma parte de la navegación autenticada para todos los roles con lectura de lotes/trazabilidad.

La presencia de `/release-control` no concede capacidades de escritura: Cliente y Auditor pueden evaluar y leer el estado operativo, mientras las acciones que modifican cadena de custodia o evidencia continúan protegidas por sus permisos específicos.

La aceptación final exige sobre un único SHA:

- ruta cold-start `/release-control`;
- estados Listo / Requiere atención / Bloqueado;
- reconciliación por unidad sin conversión implícita;
- Huella Documental separada de genealogía;
- expediente verificable sólo cuando existe SHA-256;
- navegación server-side derivada de RBAC;
- frontend reproducible;
- suite Python completa;
- build e infraestructura de producción verdes.

El candidato final debe conservar además el pulido visual: iconografía propia del Control de salida, botones de descarga visibles sólo con huella verificable y estados comunicados mediante texto + icono + color.
