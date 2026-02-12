from fastapi import FastAPI
from pydantic import BaseModel
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer
from transformers import AutoTokenizer, AutoModelForCausalLM
import torch

# =========================
# CONFIG
# =========================
QDRANT_URL = "http://localhost:6333"
COLLECTION_NAME = "rpd_rag"
EMBEDDING_MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
LLM_MODEL_NAME = "Qwen/Qwen2-1.5B-Instruct"

# =========================
# APP
# =========================
app = FastAPI()

class QueryRequest(BaseModel):
    query: str
    top_k: int = 5

# =========================
# STARTUP
# =========================
@app.on_event("startup")
def startup():

    global embedder
    global qdrant
    global tokenizer
    global llm

    embedder = SentenceTransformer(EMBEDDING_MODEL_NAME)

    qdrant = QdrantClient(url=QDRANT_URL)

    tokenizer = AutoTokenizer.from_pretrained(LLM_MODEL_NAME)
    llm = AutoModelForCausalLM.from_pretrained(
        LLM_MODEL_NAME,
        torch_dtype=torch.float16 if torch.cuda.is_available() else torch.float32,
        device_map="auto"
    )

# =========================
# RAG PIPELINE
# =========================
def retrieve(query: str, top_k: int):

    query_vector = embedder.encode(query).tolist()

    hits = qdrant.search(
        collection_name=COLLECTION_NAME,
        query_vector=query_vector,
        limit=top_k
    )

    contexts = [hit.payload["text"] for hit in hits]

    return "\n\n".join(contexts)


def generate_answer(prompt: str):

    inputs = tokenizer(prompt, return_tensors="pt").to(llm.device)

    outputs = llm.generate(
        **inputs,
        max_new_tokens=512,
        temperature=0.7
    )

    return tokenizer.decode(outputs[0], skip_special_tokens=True)

# =========================
# API
# =========================
@app.post("/generate")
def generate(request: QueryRequest):

    context = retrieve(request.query, request.top_k)

    final_prompt = f"""
Используй контекст ниже для генерации РПД.

Контекст:
{context}

Запрос:
{request.query}
"""

    answer = generate_answer(final_prompt)

    return {"answer": answer}
