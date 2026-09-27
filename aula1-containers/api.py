"""
API de análise de sentimento — Aula 1 (Containers).

Uma API mínima de "inferência": recebe uma frase em português e responde
se o sentimento é positivo ou negativo, com o grau de confiança.

Executar localmente:  uvicorn api:app --host 0.0.0.0 --port 8000
Documentação:         http://localhost:8000/docs
"""

import joblib
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(
    title="API de Sentimento — Infraestrutura Computacional para IA",
    description="Aula 1: nossa primeira aplicação de IA dentro de um container.",
    version="1.0",
)

# O modelo é carregado UMA vez, quando o container inicia — não a cada
# requisição. Esse é o padrão de qualquer serviço de inferência.
modelo = joblib.load("modelo.pkl")


class Entrada(BaseModel):
    texto: str


class Saida(BaseModel):
    sentimento: str
    confianca: float


@app.get("/")
def raiz():
    """Verificação de saúde: útil para saber se o container está no ar."""
    return {"status": "ok", "servico": "api-de-sentimento", "aula": 1}

@app.get("/sobre")
def sobre():
    return {
        "integrantes": ["Alan Beck Guimarães"]
    }

@app.post("/prediz", response_model=Saida)
def prediz(entrada: Entrada):
    """Classifica o sentimento de uma frase em português."""
    probabilidades = modelo.predict_proba([entrada.texto])[0]
    indice = probabilidades.argmax()
    return Saida(
        sentimento=modelo.classes_[indice],
        confianca=round(float(probabilidades[indice]), 4),
    )
