"""Servicio de Bóveda Documental Privada (Document Vault B2B) por Tenant."""
from __future__ import annotations
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any

@dataclass
class VaultDocument:
    id: str
    organization_id: int
    filename: str
    doc_type: str            # PDF_CERTIFICADO, DDS_JSON_TRACES, REMITO_EXCEL
    commodity: str           # Madera Aserrada Pino, Eucalipto, Tanino Quebracho, Carbón Vegetal
    parcel_name: str
    provider_tax_id: str
    file_size_kb: float
    status: str              # COMPLIANT, BLOQUEADO, PENDIENTE
    created_at: str
    download_url: str

def listar_documentos_boveda_tenant(
    organization_id: int,
    query_search: str | None = None,
    doc_type_filter: str | None = None
) -> list[dict[str, Any]]:
    """Consulta la bóveda privada de documentos de la organización autenticada."""
    
    # Documentos de demostración pertenecientes al tenant organization_id=1
    documentos_base = [
        VaultDocument(
            id="DOC-DDS-2026-001",
            organization_id=1,
            filename="DDS_TRACES_NT_30123456789_RODAL01.json",
            doc_type="DDS_JSON_TRACES",
            commodity="Madera Aserrada (Pino)",
            parcel_name="Rodal Norte 01",
            provider_tax_id="30-12345678-9",
            file_size_kb=14.2,
            status="COMPLIANT",
            created_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
            download_url="/api/v1/vault/download/DOC-DDS-2026-001"
        ),
        VaultDocument(
            id="DOC-PDF-2026-002",
            organization_id=1,
            filename="CERTIFICADO_AUDITORIA_30123456789_RODAL01.pdf",
            doc_type="PDF_CERTIFICADO",
            commodity="Madera Aserrada (Pino)",
            parcel_name="Rodal Norte 01",
            provider_tax_id="30-12345678-9",
            file_size_kb=185.6,
            status="COMPLIANT",
            created_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
            download_url="/api/v1/vault/download/DOC-PDF-2026-002"
        ),
        VaultDocument(
            id="DOC-DDS-2026-003",
            organization_id=1,
            filename="DDS_TRACES_NT_30987654321_CARBON02.json",
            doc_type="DDS_JSON_TRACES",
            commodity="Carbón Vegetal",
            parcel_name="Rodal Sur 02",
            provider_tax_id="30-98765432-1",
            file_size_kb=12.8,
            status="COMPLIANT",
            created_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
            download_url="/api/v1/vault/download/DOC-DDS-2026-003"
        ),
        VaultDocument(
            id="DOC-XLS-2026-004",
            organization_id=1,
            filename="REMITO_GUIA_FORESTAL_EMBARQUE_04.xlsx",
            doc_type="REMITO_EXCEL",
            commodity="Extracto de Quebracho (Tanino)",
            parcel_name="Lote Tanino 03",
            provider_tax_id="30-33333333-3",
            file_size_kb=42.1,
            status="COMPLIANT",
            created_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
            download_url="/api/v1/vault/download/DOC-XLS-2026-004"
        )
    ]
    
    resultados = [asdict(doc) for doc in documentos_base if doc.organization_id == organization_id]
    
    if query_search:
        q = query_search.lower().strip()
        resultados = [
            d for d in resultados if q in d["filename"].lower() or q in d["parcel_name"].lower() or q in d["provider_tax_id"].lower()
        ]
        
    if doc_type_filter and doc_type_filter != "TODOS":
        resultados = [d for d in resultados if d["doc_type"] == doc_type_filter]
        
    return resultados
