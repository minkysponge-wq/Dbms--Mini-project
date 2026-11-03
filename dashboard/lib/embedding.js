/**
 * Text embedding for vector search.
 *
 * A direct port of nosql/embedding.py. The two MUST agree byte for
 * byte: documents are embedded in Python when the cluster records an
 * event, and queries are embedded here when an operator searches. If
 * the hashing differed at all, every similarity score would be noise.
 *
 * That is why both sides use SHA-256 rather than BLAKE2b - BLAKE2b
 * folds its output length into the IV, so an 8-byte BLAKE2b is not a
 * truncated 64-byte one and Node cannot reproduce Python's. SHA-256 is
 * identical in both.
 *
 * tests/test_embedding_parity.py checks the two implementations against
 * each other.
 */

import crypto from "crypto";

export const DIMENSION = 128;

const WORD_RE = /[a-z0-9]+/g;

function hashToBucket(token, dimension) {
  const digest = crypto.createHash("sha256").update(token, "utf8").digest();
  const bucket = digest.readUInt32BE(0) % dimension;
  const sign = digest[4] & 1 ? 1.0 : -1.0;
  return [bucket, sign];
}

/**
 * Word tokens plus character trigrams of each word. Trigrams make the
 * embedder robust to inflection: "crash", "crashed" and "crashing"
 * share most of theirs.
 */
export function tokenize(text) {
  const lowered = (text || "").toLowerCase();
  const words = lowered.match(WORD_RE) || [];
  const tokens = [...words];
  for (const word of words) {
    const padded = `  ${word} `;
    for (let i = 0; i < padded.length - 2; i++) {
      tokens.push(padded.slice(i, i + 3));
    }
  }
  return tokens;
}

/** An L2-normalised embedding of `text`. */
export function embed(text, dimension = DIMENSION) {
  const vector = new Array(dimension).fill(0);
  const tokens = tokenize(text);
  if (tokens.length === 0) return vector;

  for (const token of tokens) {
    const [bucket, sign] = hashToBucket(token, dimension);
    vector[bucket] += sign;
  }

  let norm = 0;
  for (const v of vector) norm += v * v;
  norm = Math.sqrt(norm);
  if (norm === 0) return vector;
  return vector.map((v) => v / norm);
}

export function cosineSimilarity(a, b) {
  if (!a || !b || a.length !== b.length) return 0;
  let dot = 0;
  let na = 0;
  let nb = 0;
  for (let i = 0; i < a.length; i++) {
    dot += a[i] * b[i];
    na += a[i] * a[i];
    nb += b[i] * b[i];
  }
  if (na === 0 || nb === 0) return 0;
  return dot / (Math.sqrt(na) * Math.sqrt(nb));
}
