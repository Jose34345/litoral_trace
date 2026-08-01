import unittest
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from main import health_check, root_index

class TestFastAPIStep1(unittest.TestCase):
    def test_health_check_endpoint(self):
        response = asyncio.run(health_check())
        self.assertEqual(response.status_code, 200)
        
        import json
        body = json.loads(response.body.decode('utf-8'))
        self.assertEqual(body["status"], "healthy")
        self.assertEqual(body["version"], "2.4.0")

    def test_root_index_endpoint(self):
        response = asyncio.run(root_index())
        self.assertEqual(response.status_code, 200)
        
        import json
        body = json.loads(response.body.decode('utf-8'))
        self.assertIn("FastAPI Litoral Trace", body["message"])

if __name__ == "__main__":
    unittest.main()
