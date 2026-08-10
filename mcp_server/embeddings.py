"""
Embedding model wrapper - lazy loads sentence-transformers on first use
so we don't pay the ~400MB memory and load-time cost at MCP server startup.

Model: sentence-transformers/all-MiniLM-L6-v2 (384-dim, matches schema.sql).
"""

import logging
import os

logger = logging.getLogger("embeddings")

_MODEL_NAME = os.environ.get("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")

_model = None


def get_model():
    """Lazy-load the SentenceTransformer model on first use."""
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer
        logger.info(f"Loading embedding model: {_MODEL_NAME}")
        _model = SentenceTransformer(_MODEL_NAME)
        logger.info("Embedding model loaded")
    return _model


def get_model_name() -> str:
    return _MODEL_NAME


def encode(text: str) -> list[float]:
    """Encode a single text string into a 384-dim embedding list."""
    model = get_model()
    return model.encode(text).tolist()


def encode_batch(texts: list[str]) -> list[list[float]]:
    """Encode a batch of texts into 384-dim embeddings."""
    model = get_model()
    return [v.tolist() for v in model.encode(texts)]
