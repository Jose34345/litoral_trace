# Landing corporativa estática — Litoral Trace

Este directorio publica **solo la landing corporativa** del sitio existente, sin iniciar FastAPI, PostgreSQL ni Evidence Vault y sin utilizar horas de ejecución de un servicio web Python de Render.

## Fuente

Se parte de la misma rama `p2-enterprise-production` y de sus archivos públicos:

- `src/litoral_trace/templates/public/home.html`
- `src/litoral_trace/templates/public/base_public.html`
- `src/litoral_trace/templates/base.html`
- `src/litoral_trace/static/dist/app.css` y FontAwesome local.

El generador conserva la estructura visual, elimina JS y CSS exclusivo del backend, convierte el login en un enlace a `https://lacey.litoraltrace.com/`, ofrece un menú móvil sin dependencias y mantiene los enlaces de correo.

## Despliegue en Render Static Site

- Repo: `Jose34345/litoral_trace`
- Rama: `ops/corporate-static-landing-20261104` (provisional; no se mergea sobre las ramas de ejecución)
- Build command: `pip install --disable-pip-version-check -q Jinja2 && python corporate_static/build.py`
- Publish directory: `corporate_static/dist`
- Tipo: **Static Site**, no Web Service.

## Validación local

```bash
python -m pip install Jinja2
python -m unittest discover -s tests -p 'test_corporate_static.py' -v
python corporate_static/build.py
python -m http.server 8765 -d corporate_static/dist
```

Comprobar `/`, CSS, fuentes, menú móvil, CTA a Lacey Act y versión responsive.

## Cutover sin caída

1. Comprobar URL provisional HTTPS (`*.onrender.com`) y assets.
2. En Render Static Site > Settings > Custom Domains, **añadir** `litoraltrace.com` y `www.litoraltrace.com` según disponibilidad.
3. Si Render impide asignar el dominio por estar en el Web Service anterior, retirar el custom domain **solo en ese momento**, inmediatamente asignarlo al sitio estático y seguir el procedimiento DNS que muestre Render.
4. Comprobar certificado SSL, DNS, redirecciones y respuesta HTTPS desde diferentes redes. Mantener `lacey.litoraltrace.com` completamente al margen.
5. Dejar el Web Service original accesible por su URL `onrender.com` para rollback. Solo más adelante evaluar su retirada.
6. No habilitar GitHub Actions keep-warm ni UptimeRobot de alta frecuencia para el antiguo Web Service.

El cambio de DNS/dominio no se realiza en el código. Esta rama **por sí sola no modifica** el sitio actual.

## Actualizaciones posteriores

Publicar contenido estático nuevo cambiando únicamente las plantillas públicas, los recursos corporativos o el generador en esta rama. No mezclar cambios comerciales con cambios de backend. La versión de Lacey sigue siendo independiente.
