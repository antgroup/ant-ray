import os
import ray
import pandas as pd
import psycopg
from bs4 import BeautifulSoup, NavigableString
from ray.data import ActorPoolStrategy
from pathlib import Path
from pgvector.psycopg import register_vector
from functools import partial
from typing import List, Dict
from langchain.embeddings.fake import FakeEmbeddings
from langchain.text_splitter import RecursiveCharacterTextSplitter

from ragent.core.BaseTool import Tool
from ragent.embeddings.gte_base_embedding import GteBaseEmbedding


EFS_DIR = Path("/Users/paer/Documents/work/codes/RAG_ray")
DOCS_DIR = Path(EFS_DIR, "docs.ray.io/small/master/")
EMBEDDING_DIMENSION = 768
EMBEDDING_MODELL_NAME = f"fake-{EMBEDDING_DIMENSION}"
DB_NAME = "paer"

TABLE_CREATE_SQL = f"{EFS_DIR}/migrations/vector-{EMBEDDING_DIMENSION}.sql"
SQL_DUMP_PATH = f"{EFS_DIR}/sql_dumps/{EMBEDDING_MODELL_NAME}.sql"


def path_to_uri(path, scheme="https://", domain="docs.ray.io"):
    return scheme + domain + str(path).split(domain)[-1]


def extract_text_from_section(section):
    texts = []
    for elem in section.children:
        if isinstance(elem, NavigableString):
            if elem.strip():
                texts.append(elem.strip())
        elif elem.name == "section":
            continue
        else:
            texts.append(elem.get_text().strip())
    return "\n".join(texts)


def extract_sections(doc_entity):
    with open(doc_entity["path"], "r", encoding="utf-8") as html_file:
        soup = BeautifulSoup(html_file, "html.parser")
    sections = soup.find_all("section")
    section_list = []
    for section in sections:
        section_id = section.get("id")
        section_text = extract_text_from_section(section)
        if section_id:
            uri = path_to_uri(path=doc_entity["path"])
            section_list.append({"source": f"{uri}#{section_id}", "text": section_text})
    return section_list


def chunk_section(section, chunk_size, chunk_overlap):
    text_splitter = RecursiveCharacterTextSplitter(
        separators=["\n\n", "\n", " ", ""],
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        length_function=len,
    )
    chunks = text_splitter.create_documents(
        texts=[section["text"]], metadatas=[{"source": section["source"]}]
    )
    return [
        {"text": chunk.page_content, "source": str(chunk.metadata["source"])}
        for chunk in chunks
    ]


class EmbedChunks:
    def __init__(self):
        self.embedding_model = GteBaseEmbedding()
        # self.embedding_model = FakeEmbeddings(size=768)

    def __call__(self, batch):
        embeddings = self.embedding_model.embed_documents(batch["text"])
        data = {
            "text": batch["text"],
            "source": batch["source"],
            "embeddings": embeddings,
        }
        return pd.DataFrame(data)


class StoreChunks:
    def __init__(
        self,
        dbname=DB_NAME,
        table_create_sql=TABLE_CREATE_SQL,
        sql_dump_path=SQL_DUMP_PATH,
    ):
        super().__init__()
        self._dbname = dbname
        self._table_create_sql = table_create_sql
        self._sql_dump_path = sql_dump_path
        os.environ["MIGRATION_FP"] = self._table_create_sql
        os.environ["SQL_DUMP_FP"] = self._sql_dump_path

    def __call__(self, batch):
        # with psycopg.connect(f"dbname={self._dbname}") as conn:
        #     register_vector(conn)
        #     with conn.cursor() as cur:
        #         for chunk in chunks:
        #             cur.execute("INSERT INTO document (text, source, embedding) VALUES (%s, %s, %s)", (chunk['text'], chunk['source'], chunk['embedding'],),)
        #         print(f"Inserted {len(chunks)} chunks into {self._dbname}")
        # return len(chunks)
        with psycopg.connect("dbname=paer") as conn:
            register_vector(conn)
            with conn.cursor() as cur:
                for text, source, embedding in zip(
                    batch["text"], batch["source"], batch["embeddings"]
                ):
                    cur.execute(
                        "INSERT INTO document (text, source, embedding) VALUES (%s, %s, %s)",
                        (
                            text,
                            source,
                            embedding,
                        ),
                    )
        return pd.DataFrame({})


@Tool(
    desc="Take a list of HTML document local disk paths, parsing the content and indexing them into the vector database.",
    type="PythonCode",
    input={
        "input": "list of document file path, each element is a dict in which the key is always 'path' and the value is the file path"
    },
    output="Number of documents indexed.",
    metadata={
        "tag": [
            "big data",
        ]
    },
)
def index_documents(
    input: List[Dict[str, str]], config: Dict[str, str] = {"parallelism": [1, 4]}
):  # parallelism 不适合作为 LLM 可调节的参数，因为 LLM 感知不到物理环境
    chunk_size = 2000
    chunk_overlap = 100
    if config is None:
        config = {"parallelism": [1, 4]}
    parallelism = config.get("parallelism", [1, 4])
    ds: ray.data.Dataset = ray.data.from_items(input)
    ds = ds.flat_map(extract_sections)
    ds = ds.flat_map(
        partial(chunk_section, chunk_size=chunk_size, chunk_overlap=chunk_overlap)
    )
    ds = ds.map_batches(
        EmbedChunks, batch_size=100, compute=ActorPoolStrategy(*parallelism)
    )
    ds = ds.map_batches(
        StoreChunks, batch_size=100, compute=ActorPoolStrategy(*parallelism)
    )
    return ds.take_all()


if __name__ == "__main__":
    files = [{"path": path} for path in DOCS_DIR.rglob("*.html") if not path.is_dir()]
    files = files[:2]
    results = index_documents.options(parallelism=1).invoke(
        files, {"parallelism": [1, 8]}
    )
    print(f"Result amount: {len(results)}")
