[![Actualizador Vaca Muerta](https://github.com/Jose34345/vaca-muerta-intel/actions/workflows/mensual.yml/badge.svg)](https://github.com/Jose34345/vaca-muerta-intel/actions/workflows/mensual.yml)

Vaca Muerta Intelligence Platform:
Plataforma de Inteligencia de Negocios y Data Science para la formación Vaca Muerta. Este proyecto implementa una arquitectura moderna de microservicios distribuida para analizar, visualizar y predecir la producción de petróleo y gas en tiempo real.

Arquitectura del Sistema:
El sistema sigue una arquitectura desacoplada y escalable ("Industrial Standard"):

Frontend (BI): Streamlit Cloud (San Francisco) - Dashboard Interactivo.

Backend (API): FastAPI en Render (Ohio) - Lógica de Negocio y Seguridad.

Database: PostgreSQL Serverless en Neon (Virginia) - Almacenamiento Escalable.

Data Engineering: GitHub Actions - Robot ETL para actualización automática mensual.

Hitos Clave del Proyecto (Key Milestones)
1. Ingeniería de Datos & Automatización 
ETL Autónomo: Desarrollo de un pipeline automatizado (update_data.py) que extrae datos oficiales de la Secretaría de Energía.

CI/CD Pipeline: Implementación de GitHub Actions (mensual.yml) para orquestar la ingesta de datos el día 1 de cada mes sin intervención humana.

Database Cloud: Migración de datos locales a una instancia PostgreSQL Serverless (Neon).

2. Backend & Microservicios
API Restful: Desarrollo de una API robusta con FastAPI para servir datos al frontend.

Seguridad: Implementación de variables de entorno y sanitización de inputs para proteger las credenciales de la base de datos.

Endpoints Analíticos: Creación de rutas específicas para cálculos complejos (Curvas Tipo, Eficiencia, Finanzas) delegando la carga computacional al servidor.

3. Analytics & Data Science
Predicción con IA: Modelo de Machine Learning para proyectar la producción a 12 meses.

Ingeniería de Reservorios: Cálculo de métricas técnicas avanzadas:

Type Curves (Curvas Tipo): Normalización de pozos para benchmarking de operadoras (YPF, Vista, PAE).

GOR (Gas-Oil Ratio): Monitoreo de presión y madurez de yacimientos.

Water Management: Análisis de volúmenes de agua para gestión de costos operativos.

Simulación Financiera: Estimación de Revenue (Facturación) basada en escenarios de precios del Brent y volúmenes de producción.

4. Frontend & Visualización
Dashboard Interactivo: Interfaz de usuario construida con Streamlit.

Lazy Loading: Optimización de carga de datos bajo demanda para mejorar la experiencia de usuario.

Visualización Avanzada: Gráficos interactivos con Plotly (Zoom, Pan, Tooltips).

Área,Tecnología,Uso
Lenguaje,Python 3.13,Núcleo del desarrollo
Backend,FastAPI / Uvicorn,API Rest Server
Frontend,Streamlit,Dashboard & UI
Database,PostgreSQL (Neon),Persistencia de datos
ORM,SQLAlchemy,Conexión y Consultas SQL
DevOps,Docker / Render,Despliegue del Backend
Automation,GitHub Actions,Tareas programadas (Cron Jobs)
Data Science,Pandas / Scikit-Learn,Procesamiento y ML

Dashboard Público: https://vaca-muerta-intel-dcsymyjlcarmudqb8jmnyu.streamlit.app/

Desarrollado por Jose David Lezcano

