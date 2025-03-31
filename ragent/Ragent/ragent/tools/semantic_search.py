import psycopg
import numpy as np

from pgvector.psycopg import register_vector
from ragent.core.BaseTool import Tool
from ragent.embeddings.gte_base_embedding import GteBaseEmbedding


@Tool(
    desc="Take a question string, computing its embedding, connect to the vector db, and finally search in the vector store to find top-k similar documents",
    type="PythonCode",
    input={"input": "single question in string format"},
    output="Top-k similar documents to the question",
    metadata={
        "tag": [
            "big data",
        ]
    },
)
def semantic_search(input, config={"k": 5}):
    embedding_model = GteBaseEmbedding()
    embedding = np.array(embedding_model.embed_query(input))
    if config is None:
        config = {"k": 5}
    k = config.get("k", 5)
    with psycopg.connect("dbname=paer") as conn:
        register_vector(conn)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM document ORDER BY embedding <-> %s LIMIT %s",
                (embedding, k),
            )
            rows = cur.fetchall()
            # semantic_context = [{"id": row[0], "text": row[1], "source": row[2]} for row in rows]
            semantic_context = [row[1] for row in rows]
    return semantic_context


if __name__ == "__main__":
    question = "介绍一下蚂蚁集团"
    results = semantic_search.invoke(question, {"k": 2})
    print(f"Query got {len(results)} results: {results}")
