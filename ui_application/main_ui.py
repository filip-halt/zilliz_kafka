import asyncio
from queue import SimpleQueue

import uvicorn
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from hn_consumer import get_kafka_messages

app = FastAPI()
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

# Global variable to store the latest Kafka message
latest_message = None
result_queue = SimpleQueue()


async def consume_kafka_messages():
    # Simulate consuming Kafka messages and putting them in the result_queue
    while True:
        message = await get_kafka_messages()
        result_queue.put(message)


@app.get("/", response_class=HTMLResponse)
async def chatbot_page(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.on_event('startup')
async def start():
    asyncio.create_task(consume_kafka_messages())


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    while True:
        try:
            latest_message = result_queue.get()
            await websocket.send_json(latest_message)
        except asyncio.QueueEmpty:
            if result_queue.qsize() < 1:
                break


async def main():
    config = uvicorn.Config(app, host="0.0.0.0", port=8000, reload=True, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    asyncio.run(main())



