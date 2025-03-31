from langchain_community.embeddings import HuggingFaceEmbeddings


class GteBaseEmbedding(HuggingFaceEmbeddings):
    def __init__(self):
        super().__init__(
            model_name="thenlper/gte-base",
            model_kwargs={"device": "cpu"},
            encode_kwargs={"device": "cpu", "batch_size": 100},
        )
