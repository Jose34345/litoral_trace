# Guía de Demostración Comercial en Vivo — Litoral Trace B2B

Esta guía define un demo comercial controlado para exportadores, industrias forestales, despachantes, consultoras y otros actores que necesitan organizar trazabilidad y evidencia para procesos de debida diligencia.

Litoral Trace debe presentarse como **infraestructura de trazabilidad, evidencia documental y análisis geoespacial**. No se presenta como certificadora, autoridad regulatoria ni garantía automática de cumplimiento EUDR.

---

## 1. Preparación previa

1. Usar un tenant de demostración o piloto con datos ficticios o datos reales expresamente autorizados.
2. Utilizar credenciales provisionadas por el canal seguro correspondiente. **Nunca incluir usuarios o contraseñas de demo en el repositorio, documentación pública, tickets o mensajes comerciales.**
3. Verificar antes de la reunión:
   - login y logout;
   - dashboard;
   - importación XLSX;
   - lotes y geolocalización;
   - Satellite Engine si se va a demostrar;
   - Vault y evidencia vinculada.
4. Si el prospecto aporta una planilla propia, conservar su archivo original y usarlo solamente con autorización dentro del alcance acordado.

---

## 2. Guion de demostración — 10 a 15 minutos

### Paso 1 — Problema operativo

Abrir con una pregunta concreta:

> Si mañana un comprador les pide reconstruir el origen de una mercadería —proveedor, lote, geolocalización y documentación—, ¿cuánto tardarían hoy en reunir todo?

Explicar que Litoral Trace busca reducir la dispersión entre Excel, carpetas, correo, mensajería y herramientas GIS, manteniendo relaciones verificables entre origen y evidencia.

### Paso 2 — Importación y trazabilidad

1. Ir a **Importaciones** (`/imports`).
2. Cargar una planilla representativa.
3. Mostrar validaciones y errores de datos sin ocultarlos ni corregir silenciosamente información de negocio.
4. Completar una importación válida y volver al dashboard.

### Paso 3 — Lote y geolocalización

1. En **Dashboard** (`/dashboard`), mostrar que los KPIs, la tabla y el mapa se alimentan de los lotes realmente visibles para el tenant autenticado.
2. Seleccionar un lote.
3. Revisar identificador, productor, producto, superficie y coordenadas.
4. Aclarar que la organización cliente sigue siendo la fuente autoritativa de los datos de origen suministrados.

### Paso 4 — Evidencia satelital, cuando aplique

1. Con un rol habilitado, seleccionar el lote.
2. Configurar rango temporal y nubosidad máxima.
3. Ejecutar el Satellite Engine.
4. Mostrar estado durable del job y, si finaliza durante el demo, las observaciones disponibles.

Mensaje obligatorio: **NDVI y otros resultados geoespaciales son evidencia analítica de apoyo. No constituyen por sí solos prueba legal de ausencia de deforestación, aprobación regulatoria ni certificación EUDR.**

Si el procesamiento no termina dentro del tiempo de la reunión, mostrar el estado del job y continuar con el resto del flujo; no inventar resultados ni usar capturas presentadas como si fueran el resultado de esa ejecución.

### Paso 5 — Vault y evidencia

1. Ir a **Vault / Evidencias** (`/vault`).
2. Mostrar documentos del tenant y sus relaciones con el flujo correspondiente.
3. Explicar control de acceso, integridad y auditoría sin prometer retención, SLA o disponibilidad que no estén contractualmente definidos y operativamente activos.

### Paso 6 — Cierre

Volver a la pregunta inicial:

> La propuesta es pasar de “creemos que podemos reconstruir el origen” a “tenemos lotes, geolocalización, documentos y evidencia relacionados dentro de un flujo auditable”.

Cerrar preguntando cómo manejan hoy ese proceso y qué parte les genera más trabajo o riesgo operativo.

---

## 3. Qué no decir

Evitar expresiones como:

- “certifica EUDR”;
- “100% compliant”;
- “garantiza que no hubo deforestación”;
- “certificado oficial EUDR”;
- “DDS aprobado por la UE”;
- “esto reemplaza al asesor legal, certificador, operador o importador”;
- “funciona offline” o cualquier capacidad no demostrada y aceptada en la versión actual.

Los JSON, reportes, análisis y expedientes generados por Litoral Trace son **artefactos de soporte al proceso de debida diligencia**, no decisiones regulatorias.

---

## 4. Objeciones frecuentes

### “Ya usamos Excel y Drive”

Respuesta sugerida: el valor no es reemplazar cada herramienta por sí misma, sino mantener la relación entre lote, origen, geolocalización, documentos, análisis y auditoría dentro de un mismo flujo tenant-scoped.

### “¿Esto me asegura cumplimiento EUDR?”

Respuesta sugerida: no. Litoral Trace ayuda a organizar trazabilidad y evidencia y a detectar inconsistencias técnicas u operativas. La responsabilidad legal y la decisión de cumplimiento siguen correspondiendo a los actores obligados y sus asesores cuando aplique.

### “¿Podemos probarlo con nuestros datos?”

Respuesta sugerida: sí, mediante un piloto controlado con alcance, responsables, fuentes de datos y criterios de éxito acordados previamente.
