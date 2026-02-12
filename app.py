from fastapi import FastAPI
from rpd_generate import generate_rpd  # если у тебя есть такая функция

app = FastAPI()

@app.get("/")
def health():
    return {"status": "ok"}

@app.post("/generate")
def generate(data: dict):
    query = data.get("query")
    result = generate_rpd(query)
    return {"result": result}
