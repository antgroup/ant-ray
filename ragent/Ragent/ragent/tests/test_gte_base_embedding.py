import pytest
import sys

from ragent.embeddings.gte_base_embedding import GteBaseEmbedding


def test_gte_base_embedding():
    embedding_model = GteBaseEmbedding()
    print(embedding_model.embed_query("What's llm?"))


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
