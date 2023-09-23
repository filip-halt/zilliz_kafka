import asyncio
from queue import SimpleQueue

import uvicorn
from fastapi import FastAPI, Request, WebSocket, Form
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from typing_extensions import Annotated

from utils import get_kafka_messages, get_answers, start_backfill, send_question, create_queue, get_queue

app = FastAPI()
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def chatbot_page(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    queue = await get_queue()
    while True:
        message = await queue.get()
        await websocket.send_json(message)

@app.post("/chat")
async def ask_question(user_message: Annotated[str, Form()]):
    print(user_message)
    await send_question(str(user_message))
    return True

    
async def main():
    await start_backfill()
    config = uvicorn.Config(app, host="0.0.0.0", port=8000, reload=True, log_level="info")
    server = uvicorn.Server(config)

    await create_queue()
    asyncio.create_task(get_kafka_messages())
    asyncio.create_task(get_answers())
    await server.serve()


if __name__ == "__main__":
    asyncio.run(main())



