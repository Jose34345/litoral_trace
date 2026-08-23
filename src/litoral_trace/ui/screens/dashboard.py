"""Pantalla Principal Dashboard Enterprise - Litoral Trace B2B."""
from __future__ import annotations
import pandas as pd
import plotly.express as px
import streamlit as st

from litoral_trace.services.batch import generar_plantilla_excel, procesar_lote_masivo
from litoral_trace.services.compliance import evaluar_compliance_lote, generar_dds_json_traces_nt
from litoral_trace.services.reports import generar_pdf_reporte_bytes
from litoral_trace.ui.components import render_kpi_box
from litoral_trace.ui.navigation import (
    render_logout_button,
    render_nav_buttons,
    render_sidebar_header,
)
from litoral_trace.ui.theme import render_kpi_card

def dashboard_screen() -> None:
    # Sidebar
    with st.sidebar:
        render_sidebar_header()
        render_nav_buttons()
        st.markdown("---")
        st.markdown("#### 📈 Precios de Referencia (FAS)")
        st.info("💡 **Madera Aserrada (Pino)**: USD 220.00 / Ton\n\n💡 **Carbón Vegetal**: USD 350.00 / Ton\n\n💡 **Tanino Quebracho**: USD 1150.00 / Ton")
        render_logout_button()

    # Hero Header B2B
    st.markdown("""
    <div style="margin-bottom: 20px;">
        <h1 style="font-size: 2rem; font-weight: 700; color: #0f172a; margin: 0;">
            Análisis histórico de trazabilidad y balance de masas
        </h1>
        <p style="font-size: 0.95rem; color: #64748b; margin-top: 4px;">
            Pantalla legacy de análisis orientativo. No emite DDS EUDR, no presenta declaraciones ante la UE y no determina cumplimiento regulatorio.
        </p>
    </div>
    """, unsafe_allow_html=True)

    # 4 Tarjetas KPI Enterprise
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        render_kpi_card("Activos Monitoreados", "12 Lotes", "Superficie: 850 ha")
    with k2:
        render_kpi_card("Superficie Analizada", "850 ha", "Vista histórica/demo")
    with k3:
        render_kpi_card("Previews legacy", "8 previews", "No regulatorios")
    with k4:
        render_kpi_card("Resultado demo", "Sin bloqueo demo", "No concluyente EUDR", is_alert=False)

    st.markdown("<br/>", unsafe_allow_html=True)

    # Tabs Principales B2B
    tab_mapa, tab_batch, tab_manual = st.tabs([
        "🗺️ Mapa Geoespacial de Lotes",
        "⚡ Ingreso Masivo & Stress Test (Batch)",
        "🔍 Análisis Manual Legacy"
    ])

    # Tab 1: Mapa Geoespacial
    with tab_mapa:
        st.subheader("Visualización de Lotes y Estado Orientativo")
        
        # DataFrame Demo
        df_lotes = pd.DataFrame([
            {"Lote": "Rodal Norte 01", "Proveedor": "30-11111111-1", "Producto": "Madera Aserrada (Pino)", "Hectareas": 120, "Latitud": -27.45, "Longitud": -58.90, "Estatus": "Verde"},
            {"Lote": "Rodal Sur 02", "Proveedor": "30-22222222-2", "Producto": "Carbón Vegetal", "Hectareas": 85, "Latitud": -26.80, "Longitud": -60.40, "Estatus": "Verde"},
            {"Lote": "Lote Tanino 03", "Proveedor": "30-33333333-3", "Producto": "Extracto de Quebracho (Tanino)", "Hectareas": 210, "Latitud": -27.10, "Longitud": -59.50, "Estatus": "Verde"}
        ])

        fig = px.scatter_mapbox(
            df_lotes,
            lat="Latitud",
            lon="Longitud",
            color="Estatus",
            size="Hectareas",
            hover_name="Lote",
            hover_data=["Proveedor", "Producto", "Hectareas"],
            color_discrete_map={"Verde": "#10b981", "Rojo": "#ef4444", "Pendiente": "#94a3b8"},
            zoom=7,
            height=450
        )
        fig.update_layout(
            mapbox_style="open-street-map",
            margin={"r": 0, "t": 0, "l": 0, "b": 0}
        )
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df_lotes, use_container_width=True, hide_index=True)

    # Tab 2: Procesamiento Batch (DEMO STRESS TEST EN VIVO)
    with tab_batch:
        st.subheader("⚡ Procesamiento Masivo & Stress Test en Vivo")
        st.write("Sube tu matriz de datos en Excel para ejecutar el análisis histórico de biomasa y balance de masas. El resultado es orientativo y no constituye una DDS EUDR.")

        st.download_button(
            label="📥 Descargar Plantilla Excel Oficial",
            data=generar_plantilla_excel(),
            file_name="LitoralTrace_Plantilla_Ingreso.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        archivo_subido = st.file_uploader("Cargar Matriz de Datos (.xlsx)", type=["xlsx"])
        if archivo_subido is not None:
            if st.button("🚀 Ejecutar Stress Test Orientativo", type="primary"):
                try:
                    df_upload = pd.read_excel(archivo_subido)
                    df_resumen, zip_data = procesar_lote_masivo(df_upload)
                    
                    st.success("✅ Análisis histórico completado. Resumen de resultados:")
                    st.dataframe(df_resumen, use_container_width=True, hide_index=True)
                    
                    st.download_button(
                        label="📦 Descargar paquete legacy de auditoría (.ZIP)",
                        data=zip_data,
                        file_name="LitoralTrace_Paquete_Auditoria_Legacy.zip",
                        mime="application/zip",
                        type="primary"
                    )
                except Exception as e:
                    st.error(f"Error procesando archivo: {e}")

    # Tab 3: Auditoría Manual
    with tab_manual:
        st.subheader("🔍 Análisis orientativo lote a lote")
        st.caption("Esta pantalla legacy no emite certificados oficiales ni DDS EUDR.")
        
        c1, c2 = st.columns(2)
        with c1:
            nombre_lote = st.text_input("Identificador del Lote / Rodal", "Rodal Demo 01")
            proveedor_id = st.text_input("CUIT / Guía Forestal", "30-12345678-9")
            producto = st.selectbox("Especie / Producto Forestal", [
                "Madera Aserrada (Pino)",
                "Madera Aserrada (Eucalipto)",
                "Extracto de Quebracho (Tanino)",
                "Rollizo Triturable",
                "Carbón Vegetal"
            ])
            hectareas = st.number_input("Superficie (Hectáreas)", value=100.0)
        
        with c2:
            lat = st.number_input("Latitud Centroide", value=-27.45)
            lon = st.number_input("Longitud Centroide", value=-58.90)
            vol_in = st.number_input("Volumen Ingresado (Ton)", value=500.0)
            vol_out = st.number_input("Volumen Declarado para Exportar (Ton)", value=220.0)

        if st.button("⚖️ Ejecutar análisis orientativo", type="primary"):
            lote_data = {
                "identificador": nombre_lote,
                "productor_id": proveedor_id,
                "producto_forestal": producto,
                "hectareas": hectareas,
                "latitud": lat,
                "longitud": lon,
                "polygon_wkt": f"POLYGON(({lon-0.01} {lat-0.01}, {lon+0.01} {lat-0.01}, {lon+0.01} {lat+0.01}, {lon-0.01} {lat+0.01}, {lon-0.01} {lat-0.01}))"
            }
            
            res = evaluar_compliance_lote(lote_data, vol_in, vol_out)
            
            if res["dictamen"] == "Verde":
                st.success(f"✅ Resultado orientativo: {res['observacion']}")
                
                preview_json = generar_dds_json_traces_nt(lote_data, vol_out)
                pdf_bytes = generar_pdf_reporte_bytes(
                    lote_data, res["dictamen"], res["observacion"], vol_in, vol_out, res["balance_masas"].coeficiente_rendimiento
                )
                
                dl1, dl2 = st.columns(2)
                dl1.download_button("📄 Descargar reporte legacy (PDF)", data=pdf_bytes, file_name=f"REPORTE_LEGACY_{proveedor_id}.pdf", mime="application/pdf")
                dl2.download_button("📑 Descargar preview no regulatorio (JSON)", data=preview_json.encode("utf-8"), file_name=f"PREVIEW_NO_REGULATORIO_{proveedor_id}.json", mime="application/json")
            else:
                st.error(f"❌ Resultado orientativo: {res['observacion']}")
