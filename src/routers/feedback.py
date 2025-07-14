from fastapi.encoders import jsonable_encoder
from fastapi import APIRouter, Request
from fastapi.responses import Response

from src.scripts.book import Book
from src.scripts.score import Score
from src.scripts.status import Status
from src.constants import COMPLETED, READING, PLANNED, UNTRACKED
from src.scripts import auth
from src.scripts.exceptions import ObjectNotFound

router = APIRouter()


@router.post("/feedback")
async def set_score(request: Request):
    data = await request.form()
    data = jsonable_encoder(data)
    user_data = auth.get_user_data(request)

    try:
        Book(data["book_id"])
    except ObjectNotFound:
        return Response(content="Book doesn't exist", status_code=404)

    if "status" in data.keys():
        if data["status"] not in [COMPLETED, READING, PLANNED, UNTRACKED]:
            return Response(content="Unknown book status", status_code=400)
        if data["status"] == UNTRACKED:
            status = Status(user_data["preferred_username"], data["book_id"])
            status.drop_status()
        else:
            status = Status(
                user_data["preferred_username"], data["book_id"], status=data["status"]
            )
            status.set_status()

    if "score" in data.keys():
        try:
            score = int(data["score"])
            if score < 1 or score > 5:
                return Response(
                    content="score value should be integer from 1 to 5", status_code=400
                )
        except ValueError:
            return Response(
                content="score value should be integer from 1 to 5", status_code=400
            )
        Score(user_data["preferred_username"], data["book_id"], score).set_score()

    return Response(content="OK", status_code=200)
