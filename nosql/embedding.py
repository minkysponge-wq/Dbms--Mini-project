"""Text embeddings for vector search over cluster events.

A deliberately small, dependency-free embedder: text is hashed into a
fixed-dimension vector using word tokens and character trigrams, then
L2-normalised so cosine similarity reduces to a dot product.

Why this and not a transformer model
------------------------------------
A sentence-transformer would give better semantic similarity, but it
costs a multi-hundred-megabyte download and a torch install, and it
cannot run offline on a fresh machine. This embedder is deterministic,
instant, needs nothing installed, and is simple enough to explain in
full. It captures lexical and sub-word similarity - "node 3 crashed"
and "node 4 crashed and left" land close together - which is what the
search feature actually needs.

It is NOT a substitute for a trained model on genuinely paraphrased
text: "leadership instability" will not match "split vote" unless they
share characters. ``embed`` is the single swap point if a real model is
ever wanted.
"""

from __future__ import annotations

import hashlib
import math
import re

DIMENSION = 128

_WORD_RE = re.compile(r"[a-z0-9]+")


def _hash_to_bucket(token: str, dimension: int) -> tuple[int, float]:
    """Map a token to a bucket and a sign.

    The sign comes from a different byte of the same digest, which keeps
    unrelated tokens from systematically reinforcing one another - the
    standard hashing-trick correction.

    SHA-256 specifically, and not BLAKE2b: the dashboard embeds search
    queries in JavaScript and must produce byte-identical vectors, or a
    query embedded in Node would not match a document embedded in
    Python. BLAKE2b parameterises its output length in the IV, so an
    8-byte BLAKE2b is not a truncated 64-byte one and Node cannot
    reproduce it. SHA-256 is identical in both languages.
    See dashboard/lib/embedding.js, which mirrors this function.
    """
    digest = hashlib.sha256(token.encode("utf-8")).digest()
    bucket = int.from_bytes(digest[:4], "big") % dimension
    sign = 1.0 if digest[4] & 1 else -1.0
    return bucket, sign


def tokenize(text: str) -> list[str]:
    """Word tokens plus character trigrams of each word.

    Trigrams are what make the embedder robust to inflection and small
    differences: "crash", "crashed" and "crashing" share most of theirs.
    """
    lowered = text.lower()
    words = _WORD_RE.findall(lowered)
    tokens: list[str] = list(words)
    for word in words:
        padded = f"  {word} "
        for i in range(len(padded) - 2):
            tokens.append(padded[i:i + 3])
    return tokens


def embed(text: str, dimension: int = DIMENSION) -> list[float]:
    """Return an L2-normalised embedding of ``text``.

    A zero vector is returned for empty input, which scores 0 similarity
    against everything rather than raising.
    """
    vector = [0.0] * dimension
    tokens = tokenize(text or "")
    if not tokens:
        return vector

    for token in tokens:
        bucket, sign = _hash_to_bucket(token, dimension)
        vector[bucket] += sign

    norm = math.sqrt(sum(v * v for v in vector))
    if norm == 0.0:
        return vector
    return [v / norm for v in vector]


def cosine_similarity(a: list[float], b: list[float]) -> float:
    """Cosine similarity of two vectors.

    Both are expected to be normalised already, so this is a dot
    product; the norms are recomputed defensively in case they are not.
    """
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na == 0.0 or nb == 0.0:
        return 0.0
    return dot / (na * nb)
