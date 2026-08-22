# UX10-F · Control de Salida Litoral Trace

## Propósito

Convertir la salida de un despacho en una decisión empresarial clara y verificable antes de compartir un expediente con un comprador, auditor o contraparte.

UX10-F no crea un score regulatorio. Compone señales factuales que ya existen en Litoral Trace y responde:

- qué está cerrado;
- qué requiere atención;
- qué bloquea la salida;
- qué acción debe realizarse primero;
- cuál es la huella verificable del expediente.

## Impronta Litoral Trace

### Pulso del despacho

Tres estados operativos:

- **Listo**: todos los controles de salida están cerrados.
- **Requiere atención**: no hay bloqueos de trazabilidad, pero existen brechas útiles de resolver antes de compartir.
- **Bloqueado**: uno o más controles esenciales impiden presentar el despacho como expediente cerrado.

### Ruta Litoral Trace

Lectura visual de cuatro etapas:

`Origen → Cadena → Evidencia → Salida`

### Ruta de cierre

Ordena automáticamente primero los bloqueos y después las advertencias. Cada elemento incluye acceso directo al workspace donde puede resolverse.

### Huella verificable

Muestra el SHA-256 del manifest canónico del dossier documental. La huella integra genealogía y referencias documentales buyer-safe; no incluye binarios privados de Vault.

## Controles operativos

1. Estado final del despacho.
2. Genealogía de origen.
3. Reconciliación de volumen por unidad, sin conversiones implícitas entre M3/KG/TON.
4. Geometría de origen: polígono, punto de respaldo o brecha.
5. Huella Documental del recorrido.
6. Contexto comercial útil para comprador.
7. Generación de expediente verificable.

## Semántica

- La reconciliación nunca suma magnitudes de unidades distintas.
- Una geometría puntual puede generar **Atención** sin falsear un polígono.
- Una brecha documental puede generar **Atención** aunque la genealogía esté cerrada.
- Una genealogía incompleta, volumen no resuelto, despacho no final o expediente no generable produce **Bloqueado**.
- El porcentaje visual representa controles operativos cerrados, no cumplimiento regulatorio.
- El Control de Salida no constituye certificación, declaración regulatoria ni conclusión automática de cumplimiento EUDR.

## Interacción

- búsqueda por código comercial de despacho;
- filtros instantáneos Todos / Bloqueos / Atención / Listos;
- detalles expandibles por control;
- accesos directos a Operaciones, Evidencia y Trazabilidad;
- Ruta de cierre priorizada;
- copia del SHA-256 al portapapeles;
- descarga directa de PDF y expediente ZIP cuando están disponibles;
- funcionamiento server-rendered con mejora progresiva mediante JavaScript ligero.

## Comportamiento visual final

- **Control de salida** utiliza iconografía propia de tablero/pulso en la navegación para diferenciarlo de la genealogía.
- Los botones PDF y expediente sólo se muestran cuando existe una huella verificable; un expediente bloqueado nunca ofrece una descarga engañosa.
- Los colores rojo, ámbar y verde expresan exclusivamente estado operacional y siempre se acompañan de texto, icono y explicación para no depender sólo del color.
- La vista conserva su lectura y sus acciones esenciales sin JavaScript; los filtros y la copia de huella son mejoras progresivas.
