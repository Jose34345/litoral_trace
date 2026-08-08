import unittest
import asyncio
import sys
import json
import io
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from litoral_trace.api.auth import login_b2b, LoginRequest, get_current_tenant_user
from litoral_trace.api.lotes import (
    listar_lotes_tenant,
    evaluar_compliance_endpoint,
    LoteEvaluacionRequest,
    descargar_plantilla_excel_endpoint,
    procesar_batch_excel_endpoint
)
from fastapi import Response, UploadFile

class TestFastAPIStep3Lotes(unittest.TestCase):
    def setUp(self):
        req = LoginRequest(username="admin", password="admin123")
        token_res = asyncio.run(login_b2b(req, Response()))
        bearer_hdr = f"Bearer {token_res.access_token}"
        self.tenant_user = get_current_tenant_user(authorization=bearer_hdr)

    def test_listar_lotes_tenant(self):
        res = asyncio.run(listar_lotes_tenant(user=self.tenant_user))
        self.assertEqual(res.status_code, 200)
        body = json.loads(res.body.decode('utf-8'))
        self.assertIn("lotes", body)
        self.assertEqual(len(body["lotes"]), 2)

    def test_evaluar_compliance_endpoint(self):
        payload = LoteEvaluacionRequest(
            identificador="Rodal Test 01",
            productor_id="30-12345678-9",
            producto_forestal="Madera Aserrada (Pino)",
            hectareas=100.0,
            latitud=-27.45,
            longitud=-58.90,
            volumen_ingresado_ton=500.0,
            volumen_exportar_ton=220.0
        )
        res = asyncio.run(evaluar_compliance_endpoint(payload, user=self.tenant_user))
        self.assertEqual(res.status_code, 200)
        body = json.loads(res.body.decode('utf-8'))
        self.assertEqual(body["dictamen"], "Verde")
        self.assertIsNotNone(body["dds_traces_nt_json"])

    def test_descargar_plantilla_excel_endpoint(self):
        res = asyncio.run(descargar_plantilla_excel_endpoint())
        self.assertEqual(res.media_type, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

if __name__ == "__main__":
    unittest.main()
