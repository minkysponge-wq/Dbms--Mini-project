"""The Python and JavaScript embedders must agree exactly.

Documents are embedded in Python when the cluster records an event;
search queries are embedded in JavaScript in the Next.js route. If the
two hashed text even slightly differently, every similarity score would
be meaningless - and nothing would visibly break, which is what makes
this worth a test rather than a comment.

Skipped when Node is not installed.
"""

from __future__ import annotations

import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nosql.embedding import embed  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JS_MODULE = os.path.join(ROOT, "dashboard", "lib", "embedding.js")

SAMPLES = [
    "node 2 crashed and left the cluster",
    "node 0 won the election for term 1 with a majority",
    "network partition isolated nodes 1 and 3 from the majority",
    "",
    "Partition: nodes [1,3] isolated!",
    "UPPERCASE and MiXeD case 12345",
    "unicode: café naïve",
]


class TestEmbeddingParity(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        if shutil.which("node") is None:
            raise unittest.SkipTest("node is not installed")
        if not os.path.exists(JS_MODULE):
            raise unittest.SkipTest("dashboard/lib/embedding.js not found")

    def test_vectors_match_exactly(self) -> None:
        script = (
            f"import {{ embed }} from {json.dumps(pathlib.Path(JS_MODULE).as_uri())};\n"
            f"const texts = {json.dumps(SAMPLES)};\n"
            "console.log(JSON.stringify(texts.map(t => embed(t))));\n"
        )
        with tempfile.NamedTemporaryFile(
                "w", suffix=".mjs", delete=False, encoding="utf-8") as fh:
            fh.write(script)
            path = fh.name
        try:
            out = subprocess.run(
                ["node", path], capture_output=True, text=True, timeout=60)
        finally:
            os.unlink(path)

        self.assertEqual(out.returncode, 0,
                         f"node failed: {out.stderr[:400]}")
        js_vectors = json.loads(out.stdout)
        self.assertEqual(len(js_vectors), len(SAMPLES))

        for text, js_vec in zip(SAMPLES, js_vectors):
            py_vec = embed(text)
            self.assertEqual(len(py_vec), len(js_vec), f"dimension differs for {text!r}")
            worst = max((abs(a - b) for a, b in zip(py_vec, js_vec)), default=0.0)
            self.assertLess(
                worst, 1e-12,
                f"embeddings diverge for {text!r}: max difference {worst}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
