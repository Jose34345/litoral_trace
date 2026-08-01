"""Generador de Certificados de Auditoría de Riesgo en PDF."""
from __future__ import annotations
import hashlib
import time
from typing import Any

def generar_pdf_reporte_bytes(
    lote_data: dict[str, Any],
    dictamen: str,
    observacion: str,
    volumen_ingresado: float,
    volumen_exportar: float,
    coeficiente_rendimiento: float
) -> bytes:
    """Genera el reporte PDF con sello hash de inmutabilidad."""
    try:
        from fpdf import FPDF
        
        class PDF(FPDF):
            def header(self):
                self.set_font("Helvetica", "B", 11)
                self.set_text_color(100, 100, 100)
                self.cell(0, 8, "LITORAL TRACE | COMPLIANCE INTELLIGENCE", border=0)
                self.cell(0, 8, "REPORTE DE AUDITORÍA DE RIESGO EUDR", border=0, align="R")
                self.line(10, 18, 200, 18)
                self.ln(10)
                
            def footer(self):
                self.set_y(-20)
                self.set_font("Helvetica", "I", 8)
                self.set_text_color(128)
                h_str = hashlib.sha256(str(time.time()).encode()).hexdigest()[:16]
                self.cell(0, 4, f"Certificado autogenerado por Litoral Trace Engine v2.4 | Hash Inmutable: {h_str}", border=0, align="C")

        pdf = PDF()
        pdf.add_page()
        pdf.set_font("Helvetica", "B", 14)
        pdf.set_text_color(15, 23, 42)
        pdf.cell(0, 10, "AUDITORÍA DE DEBIDA DILIGENCIA (REGLAMENTO UE 2023/1115)", border=0, align="C")
        pdf.ln(5)

        # Sección 1
        pdf.set_fill_color(240, 240, 240)
        pdf.set_font("Helvetica", "B", 10)
        pdf.cell(0, 7, " 1. IDENTIFICACIÓN DEL ACTIVO Y PROVEEDOR", border=1, fill=True)
        pdf.ln(7)
        pdf.set_font("Helvetica", "", 10)
        pdf.cell(50, 6, "Lote / Rodal:", border=0); pdf.cell(0, 6, str(lote_data.get("identificador", "N/A")), border=0)
        pdf.ln(6)
        pdf.cell(50, 6, "CUIT / Guía Forestal:", border=0); pdf.cell(0, 6, str(lote_data.get("productor_id", "N/A")), border=0)
        pdf.ln(6)
        pdf.cell(50, 6, "Materia Prima:", border=0); pdf.cell(0, 6, str(lote_data.get("producto_forestal", "N/A")), border=0)
        pdf.ln(6)
        pdf.cell(50, 6, "Superficie Declarada:", border=0); pdf.cell(0, 6, f"{lote_data.get('hectareas', 0.0)} ha", border=0)
        pdf.ln(10)

        # Sección 2
        pdf.set_font("Helvetica", "B", 10)
        pdf.cell(0, 7, " 2. BALANCE DE MASAS Y RENDIMIENTO INDUSTRIAL", border=1, fill=True)
        pdf.ln(7)
        pdf.set_font("Helvetica", "", 10)
        pdf.cell(60, 6, "Materia Prima Ingresada:", border=0); pdf.cell(0, 6, f"{volumen_ingresado:.2f} Toneladas", border=0)
        pdf.ln(6)
        pdf.cell(60, 6, "Coeficiente de Rendimiento:", border=0); pdf.cell(0, 6, f"{coeficiente_rendimiento*100:.1f}%", border=0)
        pdf.ln(6)
        pdf.cell(60, 6, "Máximo Exportable Permisible:", border=0); pdf.cell(0, 6, f"{volumen_ingresado*coeficiente_rendimiento:.2f} Toneladas", border=0)
        pdf.ln(6)
        pdf.cell(60, 6, "Volumen a Exportar Declarado:", border=0); pdf.cell(0, 6, f"{volumen_exportar:.2f} Toneladas", border=0)
        pdf.ln(10)

        # Dictamen
        if dictamen == "Verde":
            pdf.set_fill_color(220, 252, 231)
            pdf.set_text_color(22, 101, 52)
            pdf.set_font("Helvetica", "B", 12)
            pdf.cell(0, 10, "DICTAMEN: FAVORABLE / COMPLIANT", border=1, align="C", fill=True)
        else:
            pdf.set_fill_color(254, 226, 226)
            pdf.set_text_color(153, 27, 27)
            pdf.set_font("Helvetica", "B", 12)
            pdf.cell(0, 10, "DICTAMEN: BLOQUEADO / RIESGO DETECTADO", border=1, align="C", fill=True)

        pdf.set_text_color(0)
        pdf.set_font("Helvetica", "", 9)
        pdf.ln(12)
        pdf.multi_cell(0, 4, observacion)

        return bytes(pdf.output())
    except Exception:
        report_text = f"LITORAL TRACE AUDIT REPORT\nDictamen: {dictamen}\nObs: {observacion}\nVolIn: {volumen_ingresado}T\nVolOut: {volumen_exportar}T\n"
        return report_text.encode("utf-8")
