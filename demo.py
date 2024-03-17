import os
import ray
import sys; sys.path.append("..")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from functools import partial
from ray.data import ActorPoolStrategy

from langchain.document_loaders import ReadTheDocsLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.embeddings.fake import FakeEmbeddings

from pathlib import Path
from rag.config import EFS_DIR
from rag.config import ROOT_DIR
from rag.data import extract_sections

def plot_section_lengths(sections):
    section_lengths = []
    for section in sections:
        section_lengths.append(len(section["text"]))
    # Plot
    plt.figure(figsize=(12, 3))
    plt.plot(section_lengths, marker='o')
    plt.title("Section lengths")
    plt.ylabel("# chars")
    plt.show()


def chunk_section(section, chunk_size, chunk_overlap):
    text_splitter = RecursiveCharacterTextSplitter(
        separators=["\n\n", "\n", " ", ""],
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        length_function=len)
    chunks = text_splitter.create_documents(
        texts=[section["text"]], 
        metadatas=[{"source": section["source"]}])
    return [{"text": chunk.page_content, "source": str(chunk.metadata["source"])} for chunk in chunks]


def get_embedding_model(embedding_model_name, model_kwargs, encode_kwargs):
    embedding_model = FakeEmbeddings(size=768)  # 768 is the input size
    return embedding_model


class EmbedChunks:
    def __init__(self):
        self.embedding_model = get_embedding_model(
            embedding_model_name="fake",
            model_kwargs={"device": "cpu"},
            encode_kwargs={"device": "cpu", "batch_size": 100})

    def __call__(self, batch):
        embeddings = self.embedding_model.embed_documents(batch["text"])
        data = {"text": batch["text"], "source": batch["source"], "embeddings": embeddings}
        return pd.DataFrame(data)

ray.init(
    address='auto',
    runtime_env={
        "env_vars": {},
        "working_dir": str(ROOT_DIR)})


DOCS_DIR = Path(EFS_DIR, "docs.ray.io/en/master/")
files = [{"path": path} for path in DOCS_DIR.rglob("*.html") if not path.is_dir()]

print(f"File amount under {DOCS_DIR}: {len(files)}")

ds = ray.data.from_items(files)
print(f"{ds.count()} documents loaded, showing the first: {ds.show(limit=1)}")

sections_ds = ds.flat_map(extract_sections)
sections = sections_ds.take_all()
print(f"There're { len(sections)} sections in total.")

plot_section_lengths(sections)

chunk_size = 2000
chunk_overlap = 100
text_splitter = RecursiveCharacterTextSplitter(
    separators=["\n\n", "\n", " ", ""],
    chunk_size=chunk_size,
    chunk_overlap=chunk_overlap,
    length_function=len)

# Scale chunking
chunks_ds = sections_ds.flat_map(partial(
    chunk_section,
    chunk_size=chunk_size,
    chunk_overlap=chunk_overlap))

chunk_showing_num = 5
print(f"After chunking(chunk_size={chunk_size}, chunk_overlap={chunk_overlap}), there're {chunks_ds.count()} chunks in total. Showing the first {chunk_showing_num}.")
chunks_ds.show(chunk_showing_num)

# Embed chunks
# embedding_model_name = "thenlper/gte-base"
embedding_model_name = "fake"
embedded_chunks = chunks_ds.map_batches(
    EmbedChunks,
    # fn_constructor_kwargs={"model_name": embedding_model_name},
    batch_size=100,
    compute=ActorPoolStrategy(1, 4))

sample = embedded_chunks.take(1)
print("Embedding size:", len(sample[0]["embeddings"]))
print(f"The first embedding: {sample[0]['text']}")
