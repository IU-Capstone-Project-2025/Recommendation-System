from json import jsonable_encoder
import fastapi
from fastapi import APIRouter,Request, Form, Depends
from fastapi.responses import HTMLResponse
from src.scripts.score import Score

router = APIRouter()

@router.post("/set_score")
async def set_score(request: Request):
    data = await request.form()
    data = jsonable_encoder(data)
    username = data["username"]
    bookId = data["bookId"]
    user_score = data["score"]

    score = Score(username = username, bookId = bookId, user_score = user_score)

    score.set_score()
    
    return {"score": "ok"}