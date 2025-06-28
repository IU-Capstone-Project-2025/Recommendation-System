from fastapi.encoders import jsonable_encoder
import fastapi
from fastapi import APIRouter, Request, Form, Depends
from fastapi.responses import HTMLResponse

from src.scripts.score import Score
from src.scripts.status import Status
from src.scripts.message import Message
from src.constants import COMPLETED

router = APIRouter()


@router.post("/set_score")
async def set_score(request: Request):
    data = await request.form()
    data = jsonable_encoder(data)
    username = data["username"]
    status = data["status"]
    bookId = data["bookId"]
    user_score = data["score"]

    if status == COMPLETED:
        score = Score(username=username, bookId=bookId, user_score=user_score)
        score.set_score()

    status = Status(username=username, bookId=bookId, status=status)
    status.set_status()

    return HTMLResponse(content="OK", status_code=200)


@router.post("/send_comment")
async def send_comment(request: Request):
    data = await request.form()
    data = jsonable_encoder(data)
    username = data["username"]
    bookId = data["bookId"]
    comment = data["comment"]

    messager = Message(username=username, bookId=bookId, comment=comment)
    messager.set_message()

    return HTMLResponse(content="OK", status_code=200)

