# main.py
import os
from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI()


@app.get("/")
def health():
    # Railway healthcheck + quick sanity check in browser
    return {"status": "ok", "message": "Jamcakes stock API running"}


# Example stocks endpoint – replace with your real logic
@app.get("/predict")
def predict(ticker: str):
    # TODO: call your v25 / OpenAI / NYSE logic here
    return {"ticker": ticker, "prediction": "stub"}


if __name__ == "__main__":
    # DO NOT hardcode 8000 / 8080
    port = int(os.getenv("PORT", 8000))
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=port)
