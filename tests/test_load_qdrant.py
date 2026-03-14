import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

import load_qdrant


class UpsertBatchRetryTests(unittest.TestCase):
    def test_retry_failed_points_returns_false_when_still_failed(self):
        ids = [1, 2, 3]
        vectors = [[0.1], [0.2], [0.3]]
        payloads = [{"id": i} for i in ids]

        with patch(
            "load_qdrant.upsert_batch",
            side_effect=[
                (True, [2, 3]),   # имитация HTTP 206 с failed ids
                (True, [3]),      # после retry один id всё ещё failed
            ],
        ), patch("load_qdrant.time.sleep"):
            ok, uploaded_ok, uploaded_failed = load_qdrant.upsert_batch_with_retry(
                ids, vectors, payloads
            )

        self.assertFalse(ok)
        self.assertEqual(uploaded_ok, 2)
        self.assertEqual(uploaded_failed, 1)


class MainAccountingTests(unittest.TestCase):
    def test_main_reports_partial_batch_accounting(self):
        chunks = [
            {"id": 1, "text": "a", "metadata": {}},
            {"id": 2, "text": "b", "metadata": {}},
            {"id": 3, "text": "c", "metadata": {}},
        ]

        with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as tmp:
            for chunk in chunks:
                tmp.write(json.dumps(chunk, ensure_ascii=False) + "\n")
            chunks_file = tmp.name

        class Resp:
            def __init__(self, status_code=200):
                self.status_code = status_code

            def raise_for_status(self):
                return None

        upsert_results = [
            (True, 2, 0),
            (False, 0, 1),
        ]

        with patch("load_qdrant.CHUNKS_FILE", chunks_file), patch(
            "load_qdrant.BATCH_EMBED", 1
        ), patch("load_qdrant.UPSERT_BATCH", 2), patch(
            "load_qdrant.embed_text", side_effect=[[0.01] * load_qdrant.EMBED_DIM] * 3
        ), patch("load_qdrant.requests.get", side_effect=[Resp(200), Resp(404)]), patch(
            "load_qdrant.requests.put", return_value=Resp(200)
        ), patch("load_qdrant.create_payload_indexes"), patch(
            "load_qdrant.upsert_batch_with_retry", side_effect=upsert_results
        ):
            out = io.StringIO()
            with redirect_stdout(out):
                load_qdrant.main(append_mode=False)

        printed = out.getvalue()
        self.assertIn("uploaded_ok=2", printed)
        self.assertIn("uploaded_failed=1", printed)
        self.assertIn("skipped_embedding=0", printed)


if __name__ == "__main__":
    unittest.main()
