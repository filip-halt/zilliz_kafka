import asyncio
from queue import SimpleQueue

import uvicorn
from fastapi import FastAPI, Request, WebSocket, Form
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from typing_extensions import Annotated

from hn_consumer import get_kafka_messages

app = FastAPI()
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def chatbot_page(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    async for message in get_kafka_messages():
        await websocket.send_json(message)

@app.post("/chat")
async def ask_question(user_message: Annotated[str, Form()]):
    return templates.TemplateResponse("index.html", {"request": user_message}) 

async def start_backfill():
    from milvuskafka.runner import Runner

    r = Runner("../config.yaml")
    # Create the Milvus Collection and Kafka Topics
    r.setup(overwrite=True)
    # Start the nodes
    # These are going to be hardcoded articles that have good RAG responses.
    r.hn_runner.post_specific_ids([
        "37583593", # Does compression have any cool use cases?
        "37601297", # Is it safe to be a scientist?
        "37602239", # Is there carbon on Europa?
    ])
    r.start()

async def main():
    await start_backfill()
    config = uvicorn.Config(app, host="0.0.0.0", port=8000, reload=True, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    asyncio.run(main())



