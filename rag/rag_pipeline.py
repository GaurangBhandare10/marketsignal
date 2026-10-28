"""
RAG Pipeline — MarketSignal
Chunks SEC filing text, embeds with sentence-transformers (local, free),
stores in ChromaDB, and answers queries via Groq (free LLM API).

Usage:
    python rag_pipeline.py --mode index  --ticker AAPL
    python rag_pipeline.py --mode query  --question "What risks did Apple disclose in Q3 2024?"
"""

from __future__ import annotations

import os
import argparse
import logging
from typing import Any

import chromadb
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
CHROMA_PATH   = os.environ.get("CHROMA_PERSIST_PATH", "./chroma_store")
EMBED_MODEL   = "all-MiniLM-L6-v2"   # ~80MB, runs locally, MIT license
GROQ_MODEL    = os.environ.get("GROQ_MODEL", "openai/gpt-oss-120b")
CHUNK_SIZE    = 800   # characters
CHUNK_OVERLAP = 100
TOP_K         = 5


# ── ChromaDB client ────────────────────────────────────────────────────────────
def get_chroma_collection() -> chromadb.Collection:
    client = chromadb.PersistentClient(path=CHROMA_PATH)
    return client.get_or_create_collection(
        "sec_filings",
        metadata={"hnsw:space": "cosine"},
    )


# ── Text chunking ──────────────────────────────────────────────────────────────
def chunk_text(text: str, size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> list[str]:
    chunks, start = [], 0
    while start < len(text):
        end = start + size
        chunks.append(text[start:end])
        start += size - overlap
    return [c.strip() for c in chunks if c.strip()]


# ── Indexing ───────────────────────────────────────────────────────────────────
def index_filing(ticker: str, filing_date: str, text: str):
    log.info("Indexing %s / %s (%d chars)", ticker, filing_date, len(text))
    model = SentenceTransformer(EMBED_MODEL)
    collection = get_chroma_collection()

    chunks = chunk_text(text)
    embeddings = model.encode(chunks, show_progress_bar=True).tolist()
    ids = [f"{ticker}_{filing_date}_{i}" for i in range(len(chunks))]
    metadatas = [{"ticker": ticker, "date": filing_date} for _ in chunks]

    # Upsert in batches of 100 to avoid ChromaDB memory pressure
    batch = 100
    for i in range(0, len(chunks), batch):
        collection.upsert(
            documents=chunks[i:i+batch],
            embeddings=embeddings[i:i+batch],
            ids=ids[i:i+batch],
            metadatas=metadatas[i:i+batch],
        )
    log.info("Indexed %d chunks for %s / %s", len(chunks), ticker, filing_date)


# ── Retrieval + generation ─────────────────────────────────────────────────────
def answer_question(question: str, ticker: str | None = None) -> str:
    from groq import Groq   # pip install groq

    model = SentenceTransformer(EMBED_MODEL)
    collection = get_chroma_collection()

    query_embedding = model.encode([question]).tolist()
    where = {"ticker": ticker} if ticker else None

    results = collection.query(
        query_embeddings=query_embedding,
        n_results=TOP_K,
        where=where,
        include=["documents", "metadatas"],
    )

    context_chunks = results["documents"][0]
    context = "\n\n---\n\n".join(context_chunks)

    prompt = f"""You are a financial analyst assistant.
Use only the excerpts below from SEC filings to answer the question.
If the answer is not in the excerpts, say "I don't have enough information."

EXCERPTS:
{context}

QUESTION: {question}

ANSWER:"""

    client = Groq(api_key=os.environ["GROQ_API_KEY"])
    response = client.chat.completions.create(
        model=GROQ_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
        max_tokens=512,
    )
    return response.choices[0].message.content.strip()


# ── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["index", "query"], required=True)
    parser.add_argument("--ticker",   type=str)
    parser.add_argument("--date",     type=str, help="Filing date YYYY-MM-DD (index mode)")
    parser.add_argument("--file",     type=str, help="Path to filing text file (index mode)")
    parser.add_argument("--question", type=str, help="Question (query mode)")
    args = parser.parse_args()

    if args.mode == "index":
        assert args.ticker and args.date and args.file
        with open(args.file, encoding="utf-8") as f:
            text = f.read()
        index_filing(args.ticker, args.date, text)

    elif args.mode == "query":
        assert args.question
        answer = answer_question(args.question, ticker=args.ticker)
        print(answer)
