# Manual de usuario — Litoral Trace

**Versión:** agosto de 2026  
**Objetivo:** enseñar a una persona sin experiencia previa a utilizar Litoral Trace de principio a fin sobre una operación forestal real.

---

## 1. Qué es Litoral Trace y para qué sirve

Litoral Trace permite reconstruir y demostrar la historia de un producto desde su origen hasta un despacho.

La secuencia conceptual es:

**parcela/rodal → recepción → transformación → lote industrial → despacho → documentos → controles**

La plataforma no reemplaza a ARCA, SENASA, organismos forestales provinciales, FSC/PEFC ni al EUDR Information System. Organiza la información, conecta los pasos de la operación y muestra qué falta.

Un estado `READY`, `LISTO` o `CONFORMANCE_READY` significa que los requisitos configurados en Litoral Trace están completos. No equivale por sí solo a una certificación ni a una autorización oficial.

---

## 2. El orden correcto de uso

Para una operación nueva, utilizar siempre este orden:

1. Iniciar sesión y verificar la organización.
2. Abrir **Preparar piloto** para ver el estado inicial.
3. Crear/cargar lotes o rodales de origen.
4. Subir documentos al Vault.
5. Vincular cada documento al eslabón correcto.
6. Registrar y contabilizar la recepción.
7. Registrar y contabilizar la transformación, mezcla, división o reempaque.
8. Crear el despacho con producto trazado.
9. Confirmar/despachar la salida.
10. Revisar la genealogía en **Trazabilidad**.
11. Completar el **Expediente exportador** si el despacho es internacional.
12. Completar el **Expediente fitosanitario** si corresponde.
13. Completar el candidato EUDR si el destino está alcanzado y se dispone de datos reales.
14. Ejecutar **Control de salida**.
15. Volver a **Preparar piloto** y comprobar si la operación alcanzó `PILOT_READY`.

No conviene empezar por EUDR, SENASA o el expediente exportador antes de tener una cadena de custodia válida. Esos controles dependen del despacho y, en el caso EUDR, también de la genealogía y las parcelas.

---

## 3. Inicio de sesión y permisos

### 3.1 Ingresar

1. Abrir Litoral Trace.
2. Seleccionar **Iniciar sesión**.
3. Ingresar usuario y contraseña entregados por el administrador.
4. Al ingresar correctamente se abre **Inicio** (`/dashboard`).

### 3.2 Si aparece “formulario expirado”

El formulario de acceso está protegido. Si permaneció abierto demasiado tiempo o la sesión de seguridad venció:

1. recargar la página de login;
2. volver a escribir usuario y contraseña;
3. no reutilizar una pestaña muy antigua.

### 3.3 Si aparece “usuario o contraseña incorrectos”

- verificar mayúsculas/teclado;
- no insistir muchas veces con una contraseña dudosa;
- pedir al administrador que confirme el usuario y que la cuenta/organización estén activas.

### 3.4 Menú según rol

No todos ven las mismas opciones.

- **Admin/Manager:** operación y preparación del piloto.
- **Operador:** funciones operativas autorizadas.
- **Auditor/consulta:** lectura, sin modificar información sensible.
- **Superadmin:** administración de la plataforma; no es el rol normal del cliente.

Si una opción no aparece, puede ser simplemente una restricción de rol y no un error.

---

## 4. Preparar piloto: el tablero que indica por dónde seguir

Abrir **Preparar piloto** (`/pilot-readiness`).

La pantalla calcula automáticamente siete hitos:

1. Organización y acceso.
2. Origen y lotes.
3. Evidencia documental.
4. Recepción trazable.
5. Transformación/cadena de custodia.
6. Despacho internacional.
7. Expediente y Control de Salida.

Estados posibles:

- `NOT_STARTED`: todavía no se inició una operación.
- `IN_PROGRESS`: hay datos pero faltan hitos.
- `PILOT_READY`: existe al menos un despacho que demuestra una cadena completa y supera los controles aplicables.

Las tildes no se marcan manualmente. Si un paso dice pendiente, hay que completar el dato real en el módulo correspondiente.

---

## 5. Cargar lotes o rodales de origen

La forma más rápida para un cliente que ya trabaja con Excel es **Carga masiva** (`/imports`).

### 5.1 Formato del Excel

El archivo debe ser `.xlsx` y contener una hoja llamada exactamente:

`Plantilla_LitoralTrace`

Columnas requeridas, en este orden:

1. `Identificador_Lote`
2. `ID_Proveedor`
3. `Producto_Forestal`
4. `Hectareas`
5. `Latitud`
6. `Longitud`
7. `Volumen_Ingresado_Ton`
8. `Volumen_Exportar_Ton`

Ejemplo conceptual:

- Identificador: `Rodal_Norte_01`
- Proveedor: `CUIT-30123456789`
- Producto: `Madera Aserrada (Eucalipto)`
- Hectáreas: `120`
- Latitud: `-27.50`
- Longitud: `-58.90`

Límites actuales:

- máximo 500 filas de datos por archivo;
- sólo `.xlsx`;
- máximo 10 MB para la carga masiva;
- no macros, objetos embebidos ni vínculos externos;
- no agregar columnas adicionales a la plantilla canónica.

### 5.2 Procedimiento recomendado

1. Abrir **Carga masiva**.
2. Seleccionar el archivo.
3. Ejecutar primero la validación/preview.
4. Leer todos los errores de fila.
5. Corregir el Excel original.
6. Volver a validar.
7. Importar sólo cuando todas las filas sean válidas.

### 5.3 Si la importación falla

**“Planilla contiene filas con errores”**  
No se guarda ningún lote. Corregir las filas indicadas y volver a subir el archivo.

**“Ya existen identificadores de lote”**  
El mismo identificador ya existe dentro de la organización. No cambiarlo al azar: verificar si se está intentando duplicar un rodal existente o si el nuevo lote necesita un identificador distinto.

**“Clave de idempotencia en conflicto”**  
La misma operación de importación se reutilizó con un archivo diferente. Volver a la pantalla y comenzar una nueva carga.

**“Servicio no disponible”**  
No asumir que hay que repetir varias veces. Conservar el mismo archivo y reintentar cuando el servicio vuelva. La importación está diseñada para evitar duplicados accidentales.

### 5.4 Geometría para EUDR

Para operaciones con potencial EUDR:

- una parcela con polígono válido es preferible siempre;
- si la parcela supera 4 ha, Litoral Trace exige polígono para conformance EUDR;
- una parcela de hasta 4 ha puede utilizar un punto válido como fallback del perfil local;
- no inventar polígonos para conseguir una tilde verde.

---

## 6. Documentos y evidencias: Vault

Abrir **Evidencias** y/o **Documentos y evidencias / Vault**.

El Vault es el depósito de archivos. Subir un archivo al Vault **no significa todavía que esté relacionado con una operación**. Después hay que vincularlo al lote, movimiento, lote industrial o despacho correspondiente.

### 6.1 Flujo correcto

1. Pulsar **Subir documento**.
2. Elegir el tipo documental general del Vault.
3. Seleccionar el archivo.
4. Esperar a que el estado quede `Disponible`.
5. Ir a **Evidencias**.
6. Seleccionar el eslabón de trazabilidad correspondiente.
7. Elegir el documento ya guardado.
8. Elegir el tipo de evidencia.
9. Completar número, emisor, fecha o vigencia cuando sea útil.
10. Confirmar el vínculo.

### 6.2 Tipos de evidencia contextual disponibles

- `ORIGIN_AUTHORIZATION`: autorización/permiso de origen.
- `FOREST_GUIDE`: guía forestal.
- `FRUIT_GUIDE`: Guía de Frutos.
- `REMITO`: remito.
- `INVOICE`: factura.
- `CERTIFICATE`: certificado general.
- `PHYTOSANITARY_CERTIFICATE`: certificado fitosanitario.
- `EPHYTO_XML`: XML ePhyto.
- `TRANSPORT`: vale/documento de transporte.
- `GEOSPATIAL`: archivo/evidencia geoespacial.
- `SUPPLIER_DECLARATION`: declaración del proveedor.
- `OTHER`: otra evidencia.

### 6.3 A qué se puede vincular un documento

- `SOURCE_LOTE`: parcela/rodal de origen.
- `TRACEABILITY_EVENT`: recepción, transformación, mezcla, etc.
- `TRACEABILITY_BATCH`: lote industrial.
- `SHIPMENT`: despacho.

Un mismo archivo puede estar almacenado una sola vez y relacionarse con distintos eslabones cuando tenga sentido.

### 6.4 Regla crítica para los controles de exportación

Los módulos de expediente exportador y fitosanitario buscan determinadas evidencias **vinculadas directamente al despacho**.

Por eso, si una Guía de Frutos o certificado fitosanitario es requisito del despacho, no alcanza con tenerlo sólo guardado en Vault o vinculado al lote de origen. Debe existir el vínculo documental al `SHIPMENT` correspondiente.

### 6.5 Formatos

El backend de almacenamiento soporta actualmente:

- PDF;
- JSON;
- XML;
- XLSX.

El tamaño estándar máximo configurado es 25 MiB por archivo.

**Limitación visible actual:** el modal web de Vault muestra PDF/JSON/XLSX y su selector de archivo no expone XML. El servicio de almacenamiento sí contempla XML y el módulo fitosanitario exige XML para `EPHYTO_XML`. Si un piloto necesita ePhyto, no renombrar un XML como PDF/XLSX: usar el camino técnico habilitado o corregir el selector web antes de ese piloto.

### 6.6 Si una carga falla

- comprobar tamaño;
- comprobar extensión/formato real;
- no cambiar sólo la extensión del archivo;
- volver a intentar una vez si el servicio tuvo un error temporal;
- si sigue en `upload_failed`, conservar el original y pedir soporte.

La plataforma verifica integridad SHA-256. Si la descarga o integridad no coincide, tratar el documento como no confiable hasta resolverlo.

---

## 7. Orden recomendado de documentos

No existe una obligación técnica de subir todos los archivos en una única secuencia, pero para evitar confusión operativa usar este orden.

### Etapa A — Origen, antes de la recepción

1. Geometría/polígono o evidencia geoespacial.
2. Autorización/permiso de origen cuando corresponda.
3. Declaración del proveedor si existe.
4. Documentos forestales del origen.

### Etapa B — Movimiento hacia planta

Para bosque cultivado en Corrientes:

1. Factura o Remito oficial.
2. Guía de Frutos.

Para bosque nativo:

1. Guía de Productos Forestales Nativos.
2. Vale de Transporte.

### Etapa C — Recepción y transformación

1. Remito/recepción asociado al movimiento.
2. Documentación de control interno de planta si existe.
3. Certificados o evidencia específica del lote industrial cuando corresponda.

### Etapa D — Despacho/exportación

1. Factura E / referencia de la factura.
2. Guía/remito/vale aplicable al despacho.
3. Destinación SIM y subrégimen.
4. Otros documentos comerciales/aduaneros que la empresa quiera conservar.

### Etapa E — Fitosanitario

1. Referencia oficial de requisitos del destino.
2. Fecha de evaluación.
3. Referencia CERT-POV si hay trámite.
4. Número de certificado.
5. Certificado fitosanitario PDF si el modo es `PAPER`.
6. Referencia ePhyto + XML ePhyto si el modo es `EPHYTO`.

### Etapa F — EUDR, sólo si aplica

No “subir un EUDR” primero. Antes deben existir:

- despacho;
- genealogía completa;
- parcelas/geometrías válidas;
- identidad real del operador europeo;
- producto/HS/cantidad;
- especies cuando sea madera;
- evaluación de riesgo real.

---

## 8. Registrar una recepción

Abrir **Operaciones**.

### Campos

1. **Parcela / rodal de origen:** elegir un origen ya cargado.
2. **Código de recepción:** ejemplo `REC-2026-001`.
3. **Lote recibido:** ejemplo `MP-PINO-001`.
4. **Producto:** puede informarse de forma explícita.
5. **Cantidad.**
6. **Unidad:** m³, kg o t.
7. **Fecha y hora.**
8. **Instalación/playa.**

### Borrador vs contabilizado

**Guardar borrador:** no modifica existencias. Usarlo cuando todavía falta revisar información.

**Guardar y contabilizar:** crea la existencia disponible y la incorpora al ledger/cadena de custodia.

Regla práctica: ante una operación real, guardar primero borrador si hay duda; contabilizar sólo después de revisar origen, cantidad, unidad y fecha.

### Fallos habituales

**No hay origen seleccionable**  
Volver a Carga masiva y crear el lote/rodal.

**Cantidad inválida**  
Debe ser positiva y numérica.

**Fecha inválida/faltante**  
Completar fecha y hora. El flujo NEA interpreta hora local Argentina y la normaliza internamente.

**Conflicto al contabilizar**  
No recrear la operación inmediatamente. El borrador queda disponible. Revisar si el evento/lote ya fue contabilizado o si hubo una operación concurrente.

---

## 9. Registrar transformación, mezcla, división o reempaque

En **Operaciones**, usar **Registrar proceso industrial**.

### 9.1 Elegir tipo

- `TRANSFORMATION`: transforma insumo en otro producto.
- `MIX`: mezcla dos o más lotes.
- `SPLIT`: divide un lote en varias salidas.
- `REPACK`: reempaque sin representar una transformación material principal.

### 9.2 Entradas

Elegir lotes que tengan saldo disponible y cantidad a consumir.

Para `MIX`, utilizar al menos dos entradas cuando la operación realmente es una mezcla.

### 9.3 Salidas

Para cada salida completar:

- código de lote nuevo;
- producto;
- etapa (`INTERMEDIATE` o `FINISHED_GOOD`);
- unidad;
- cantidad.

### 9.4 Regla de cantidades

Litoral Trace no aplica densidades ni conversiones ocultas.

Si la entrada está en m³ y se intenta crear una salida en kg/t sin un perfil de conversión documentado, la contabilización se rechaza.

Tampoco se debe crear más cantidad de salida que la disponible/ingresada bajo la misma unidad.

### 9.5 Borrador vs contabilizar

- **Guardar borrador:** permite revisar entradas/salidas sin consumir stock.
- **Guardar y contabilizar:** consume entradas y crea las salidas industriales.

Si la contabilización falla, el borrador permanece para revisión.

### Fallos habituales

**Lote sin saldo**  
Elegir otro lote o corregir una operación previa; no editar cifras para forzar disponibilidad.

**Unidad incompatible**  
Mantener la misma unidad o documentar/implementar el perfil de conversión correspondiente antes de contabilizar.

**Salida mayor que entrada**  
Corregir cantidades o rendimiento. No contabilizar hasta que el balance sea coherente.

**Secuencia temporal inválida**  
Una transformación no puede ocurrir lógicamente antes de la recepción que genera su stock.

---

## 10. Crear y confirmar un despacho

Una vez que existe producto industrial disponible:

1. abrir **Operaciones**;
2. crear un despacho;
3. informar un código único, por ejemplo `EXP-2026-001`;
4. completar referencia de venta si existe;
5. completar comprador/referencia del comprador;
6. informar país de destino;
7. seleccionar el/los lotes industriales;
8. informar cantidades;
9. guardar como borrador para revisar;
10. confirmar/despachar cuando la información sea final.

### Estado DRAFT

No consume stock todavía. Es el estado recomendado para preparar la operación.

### Estado DISPATCHED

Consume stock y permite reconstruir formalmente la genealogía del despacho.

No confirmar un despacho sólo para avanzar en el checklist si todavía no representa una operación válida.

### Si falla el despacho

- revisar saldo de cada lote;
- revisar unidad;
- revisar cantidades;
- verificar que el despacho no haya sido confirmado previamente;
- si aparece conflicto, comprobar el estado antes de reintentar para evitar doble consumo.

---

## 11. Revisar Trazabilidad

Abrir **Trazabilidad** después de contabilizar las operaciones y despachar.

Objetivo: comprobar que el despacho puede reconstruirse hacia atrás hasta los lotes de origen.

La cadena esperada es:

`SHIPMENT → lote terminado/intermedio → transformación(es) → lote recibido → recepción → SOURCE_LOTE`

Si la genealogía tiene una brecha, no seguir “completando documentos” como si eso resolviera la cadena. Volver al evento donde se perdió la relación de entrada/salida.

Antes de continuar con expediente/EUDR comprobar:

- origen visible;
- recepción `POSTED`;
- transformación `POSTED`;
- despacho `DISPATCHED`;
- cantidades coherentes;
- sin salto entre un lote y su evento de creación.

---

## 12. Expediente exportador Corrientes

Abrir el expediente mediante el código del despacho.

Estados:

- **LISTO / READY:** todos los requisitos del perfil seleccionado están presentes.
- **BLOQUEADO / BLOCKED:** existe al menos un requisito faltante.

### 12.1 Elegir el perfil correcto

#### Bosque cultivado (`CULTIVATED`)

Litoral Trace exige:

- Factura **o** Remito oficial del traslado, como evidencia vinculada al despacho;
- Guía de Frutos de Corrientes, vinculada al despacho;
- número de Factura E;
- identificador de destinación SIM;
- subrégimen SIM.

CAE y fecha de oficialización pueden registrarse, pero el readiness actual no los usa como requisitos obligatorios.

#### Bosque nativo (`NATIVE`)

Litoral Trace exige:

- Guía de Productos Forestales Nativos, vinculada al despacho;
- Vale de Transporte, vinculado al despacho;
- número de Factura E;
- identificador de destinación SIM;
- subrégimen SIM.

### 12.2 Orden práctico

1. subir documentos al Vault;
2. vincular las guías/remito/vale directamente al despacho con el tipo de evidencia correcto;
3. abrir Expediente exportador;
4. seleccionar `CULTIVATED` o `NATIVE`;
5. completar Factura E;
6. completar destinación SIM;
7. completar subrégimen;
8. guardar expediente;
9. revisar cada requisito rojo.

### 12.3 Importante sobre Factura E

El readiness actual verifica el **número** de Factura E en el expediente. Para un dossier completo es recomendable también almacenar el PDF/documento en Vault y vincularlo, aunque esa copia documental no es el criterio técnico que pone verde `EXPORT_INVOICE_E`.

### Si sigue BLOQUEADO

No adivinar. Leer la lista de requisitos. Cada requisito muestra su fuente: Vault, ARCA, SIM o Expediente.

---

## 13. Expediente fitosanitario

La evaluación comienza normalmente como `UNASSESSED`.

No cambiar a `NOT_REQUIRED` sólo para quitar el bloqueo.

### 13.1 Primero evaluar requisitos del destino

Registrar:

- referencia oficial de requisitos del país de destino;
- fecha en la que esos requisitos fueron evaluados.

### 13.2 Elegir modo

#### `NOT_REQUIRED`

Usar únicamente cuando una fuente/referencia oficial sustenta que no se requiere certificado.

Requisitos:
- referencia oficial;
- fecha de evaluación.

#### `PAPER`

Requisitos:
- referencia oficial;
- fecha de evaluación;
- referencia CERT-POV;
- número de certificado;
- PDF del certificado fitosanitario en Vault;
- vínculo al despacho como `PHYTOSANITARY_CERTIFICATE`.

#### `EPHYTO`

Requisitos:
- referencia oficial;
- fecha de evaluación;
- referencia CERT-POV;
- número de certificado;
- referencia de intercambio ePhyto;
- XML ePhyto en Vault;
- vínculo al despacho como `EPHYTO_XML`.

Para `PHYTOSANITARY_CERTIFICATE`, el archivo debe ser un PDF válido. Para `EPHYTO_XML`, debe ser XML real.

### Si sigue BLOQUEADO

Revisar exactamente el requisito rojo. Ejemplo: tener el número de certificado pero no el PDF no deja `PAPER` listo; tener XML pero no la referencia ePhyto tampoco deja `EPHYTO` listo.

---

## 14. Candidato EUDR, sólo cuando aplica

Este módulo prepara información local y apunta al entorno de prueba ACCEPTANCE. No genera una declaración legal LIVE.

### 14.1 Antes de abrir EUDR

Debe existir:

- despacho válido;
- genealogía completa;
- al menos una parcela de producción atribuida;
- geometría válida de todas las parcelas.

### 14.2 Datos que se deben completar

- actividad EUDR (`IMPORT`, `DOMESTIC`, `EXPORT`) según el actor real;
- commodity (`WOOD` u otro EUDR);
- nombre del operador EUDR real;
- domicilio;
- país ISO alpha-2;
- EORI cuando la actividad sea `IMPORT`;
- HS/CN de 4–10 dígitos;
- nombre comercial;
- descripción;
- masa neta en kg;
- país de producción;
- fecha/rango de producción;
- para madera: nombre común y nombre científico completo;
- DDS previa + número de verificación sólo si realmente se depende de una DDS previa;
- conclusión de riesgo;
- referencia de la evaluación de riesgo;
- fecha de evaluación.

### 14.3 Cuándo llega a `CONFORMANCE_READY`

Sólo si todos los requisitos están completos y:

- genealogía completa;
- todas las parcelas geolocalizadas;
- polígono cuando corresponda;
- operador/producto/cantidad completos;
- especies completas para madera;
- conclusión exactamente `NO_OR_NEGLIGIBLE_RISK`;
- evaluación de riesgo con referencia y fecha;
- perfil técnico vigente.

### 14.4 Lo que nunca se debe inventar

- EORI;
- operador europeo;
- HS/CN;
- especie;
- geometría;
- DDS previa;
- conclusión de riesgo.

Si no existe una evaluación real, dejar `UNASSESSED` aunque eso mantenga el candidato bloqueado.

`CONFORMANCE_READY` no significa “cumple legalmente EUDR”. Significa que el candidato local tiene completos los campos que Litoral Trace valida para el perfil actual.

---

## 15. Control de Salida

Abrir **Control de salida** e ingresar el código del despacho.

El sistema reconstruye:

- genealogía;
- evidencia documental;
- dossier;
- expediente exportador si es internacional;
- fitosanitario si es internacional;
- EUDR si el destino lo requiere.

### Resultado listo

Significa que la operación supera los controles configurados.

### Resultado bloqueado

Significa que existe una causa concreta. No crear documentos falsos ni cambiar estados manualmente. Ir al módulo indicado, completar el requisito y volver a ejecutar el Control de Salida.

### Errores frecuentes

**Despacho no encontrado (404)**  
Verificar el código exacto.

**No se puede evaluar (422)**  
La genealogía o estructura del despacho no permite una evaluación válida. Corregir cadena de custodia.

**Control no disponible / falló (503)**  
Es un problema temporal de dependencia/servicio. No modificar datos para “solucionarlo”. Reintentar cuando la aplicación esté disponible y escalar si persiste.

---

## 16. Cómo alcanzar PILOT_READY

Volver a **Preparar piloto**.

Para `PILOT_READY`, una única operación debe demostrar:

1. usuario/organización válidos;
2. lote de origen;
3. al menos un documento Vault disponible;
4. recepción `POSTED` dentro de la genealogía del despacho;
5. transformación `POSTED` dentro de esa genealogía;
6. despacho internacional `DISPATCHED`;
7. expediente exportador `READY` + fitosanitario `READY` + EUDR `CONFORMANCE_READY` si el destino es EUDR.

No se puede combinar una recepción de una operación, una transformación no relacionada y otro despacho para fabricar el estado. El sistema ancla los hitos a la genealogía de un despacho real.

El smoke remoto EUDR ACCEPTANCE no es requisito para `PILOT_READY`.

---

## 17. Tabla rápida de fallos y respuesta correcta

| Problema | Qué significa | Qué hacer |
|---|---|---|
| Login expirado/CSRF | formulario viejo | recargar e iniciar nuevamente |
| Acceso denegado | rol sin permiso | solicitar rol correcto; no compartir credenciales |
| Excel inválido | estructura/columnas/filas incorrectas | corregir original y revalidar |
| Lote duplicado | identificador ya existe | comprobar si es duplicación real |
| Import 503 | servicio temporalmente no disponible | conservar archivo y reintentar después |
| Vault upload failed | archivo/tamaño/storage | revisar formato y reintentar una vez |
| Evidencia no aparece | archivo no disponible o no vinculado | comprobar estado `available` y vínculo contextual |
| Recepción no contabiliza | origen/cantidad/fecha/conflicto | corregir borrador; no duplicar |
| Proceso no contabiliza | stock/unidad/secuencia | revisar entradas, unidades y fechas |
| Salida > entrada | balance inválido | corregir cantidades/rendimiento |
| Despacho no confirma | stock o conflicto | revisar saldo y estado actual |
| Export case BLOCKED | documento/referencia faltante | completar el requisito rojo según su fuente |
| Phyto UNASSESSED | requisito de destino no evaluado | consultar fuente oficial y registrar referencia/fecha |
| PAPER BLOCKED | falta trámite/número/PDF | completar todos los elementos |
| EPHYTO BLOCKED | falta referencia/XML real | completar ambos; no falsificar formato |
| EUDR BLOCKED | dato, riesgo, geometría o genealogía faltante | corregir la fuente real del requisito |
| Control 404 | código despacho inexistente | verificar código |
| Control 422 | cadena no evaluable | reparar genealogía |
| Control 503 | dependencia caída | esperar/reintentar/escalar |

---

## 18. Reglas que evitan errores graves

1. No contabilizar una operación dudosa sólo para avanzar.
2. No cambiar unidades para hacer coincidir cantidades.
3. No duplicar un lote ya existente.
4. No crear dos veces el mismo despacho por un error de pantalla.
5. No borrar/reemplazar evidencia para ocultar una inconsistencia; conservar trazabilidad.
6. No elegir `NOT_REQUIRED` sin referencia oficial.
7. No inventar datos EUDR.
8. No interpretar NDVI como prueba automática de cumplimiento EUDR.
9. No asumir que `READY` significa autorización oficial.
10. Ante un 503, primero pensar en una falla de servicio, no en un error de los datos.
11. Antes de confirmar una operación, revisar código, fecha, unidad, cantidad y referencia.
12. Para documentos exigidos por un despacho, verificar que estén vinculados al `SHIPMENT`, no sólo almacenados en Vault.

---

## 19. Flujo diario recomendado para una empresa

### Al recibir materia prima

- comprobar/crear origen;
- guardar documentos del proveedor;
- registrar recepción;
- revisar y contabilizar;
- vincular remitos/guías al origen/movimiento según corresponda.

### Al transformar

- elegir lotes de entrada reales;
- crear salidas;
- revisar balance/unidad;
- contabilizar;
- vincular documentación interna relevante.

### Al preparar venta/exportación

- crear despacho DRAFT;
- vincular documentos comerciales;
- completar expediente exportador;
- evaluar fitosanitario;
- completar EUDR sólo si aplica;
- ejecutar Control de Salida;
- corregir faltantes;
- confirmar/despachar cuando la operación sea real y corresponda.

### Después

- revisar trazabilidad;
- conservar dossier/evidencia;
- utilizar el código de despacho como referencia central frente a comprador/auditor.

---

## 20. Checklist antes de pedir soporte

Antes de informar “la plataforma no funciona”, anotar:

- usuario/rol (sin contraseña);
- organización;
- pantalla;
- código de lote/evento/despacho;
- hora aproximada;
- mensaje visible;
- código de error si aparece;
- acción que se intentaba realizar;
- si el registro quedó DRAFT, POSTED o DISPATCHED.

Nunca enviar por email/chat:

- contraseñas;
- Authentication Keys;
- JWT;
- claves AWS/GCP;
- credenciales EUDR;
- códigos MFA.

---

## 21. Resumen de una operación correcta

Una operación bien cargada debe poder responder estas preguntas sin buscar manualmente en carpetas:

1. ¿Qué se despachó?
2. ¿Qué lotes industriales formaron ese despacho?
3. ¿Qué transformación creó esos lotes?
4. ¿Qué materia prima se consumió?
5. ¿Qué recepción ingresó esa materia prima?
6. ¿De qué parcela/rodal y proveedor provino?
7. ¿Qué documentos respaldan cada parte?
8. ¿Qué falta para considerar listo el expediente?
9. ¿Qué información puede entregarse a un comprador/auditor?
10. Si aplica UE, ¿están conectados los datos necesarios para preparar el candidato EUDR?

Si Litoral Trace puede responder esas diez preguntas para un despacho real, la plataforma está siendo utilizada con el propósito para el que fue diseñada.
