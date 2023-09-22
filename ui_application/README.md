# zillis_chat_demo

This repo will host a very simple python fastapi frontend that will act as the entry point for the Zillis Chatbot demo.

## Installation

To launch the user will need to create a virtual environment and then run the `requirements.txt` using the command `pip install requirements.txt`

Once this is done, the fastapi can be launched by running the command:
`uvicorn main:app --host 0.0.0.0 --port 8000
`
The frontend is now available on `http://localhost:8000` or `http://0.0.0.0:8000 