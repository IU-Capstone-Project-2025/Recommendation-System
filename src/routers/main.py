from fastapi import APIRouter, Form, Request
from fastapi.responses import RedirectResponse
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.encoders import jsonable_encoder

from src.scripts import auth
from src.scripts.book import Book
from src.scripts.book_stats import BookStats
from src.scripts.exceptions import BadCredentials, ObjectNotFound, UsernameNotUnique
from src.constants import TOP_LIST

from src.scripts.book_list import BookList
from src.scripts.search import Search
from src.scripts.user_list import UserList
from src.scripts.user_stats import UserStats
from src.scripts.status import Status
from src.scripts.score import Score

# from fastapi.templates import Jinja2Templates
router = APIRouter()

templates = Jinja2Templates(directory="src/frontend/html")


@router.get("/", response_class=HTMLResponse)
async def root(request: Request):
    user_data = auth.get_user_data(request)
    return templates.TemplateResponse(
        "index.html", {"request": request, "user_data": user_data}
    )


@router.get("/catalog", response_class=HTMLResponse)
async def catalog(request: Request):
    user_data = auth.get_user_data(request)
    book_list = BookList(TOP_LIST)
    return templates.TemplateResponse(
        "catalog.html",
        {
            "request": request,
            "user_data": user_data,
            "books": book_list.get_book_list(0),
        },
    )


@router.get("/personal", response_class=HTMLResponse)
async def personal(request: Request):
    user_data = auth.get_user_data(request)
    user_lists = UserList(user_data["preferred_username"])
    completed = user_lists.get_completed_list()
    reading = user_lists.get_reading_list()
    planned = user_lists.get_planned_list()
    user_stats = UserStats(user_data["preferred_username"])
    return templates.TemplateResponse(
        "personal_account.html",
        {
            "request": request,
            "user_data": user_data,
            "user_lists": [completed, reading, planned],
            "user_stats": user_stats,
        },
    )


@router.get("/book", response_class=HTMLResponse)
async def book(request: Request, id: int):
    try:
        book = Book(id)
    except ObjectNotFound:
        return templates.TemplateResponse("404.html", {"request": request})

    book_stats = BookStats(id)

    user_data = auth.get_user_data(request)
    if not user_data:
        return templates.TemplateResponse(
            "book_info.html",
            {
                "request": request,
                "user_data": {},
                "book": book,
                "status": None,
                "book_stats": book_stats,
            },
        )

    status = Status(user_data["preferred_username"], id).status
    if not status:
        status = "untracked"

    score = Score(user_data["preferred_username"], id).score
    if not score:
        score = 0

    return templates.TemplateResponse(
        "book_info.html",
        {
            "request": request,
            "user_data": user_data,
            "book": book,
            "status": status,
            "score": score,
            "book_stats": book_stats,
        },
    )


@router.post("/search", response_class=HTMLResponse)
async def search(request: Request, search_string: str = Form(...)):

    import subprocess

    result = subprocess.Popen(
        ["./levenshtein_length"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd="src/scripts/searching_mechanism",
    )

    output_data, stderr_data = result.communicate(input=search_string + "\n")
    output_lines = output_data.splitlines()

    cleaned_lines = [
        line.strip()
        for line in output_lines
        if line.strip() and not line.startswith("----")
    ]

    search_instance = Search(cleaned_lines)
    books = search_instance.get_search_result()

    user_data = auth.get_user_data(request)

    return templates.TemplateResponse(
        "search_results.html",
        {
            "request": request,
            "user_data": user_data,
            "books": books,
            "query": search_string,
        },
    )


@router.get("/signin", response_class=HTMLResponse)
async def signin_get(request: Request):
    return templates.TemplateResponse("signin.html", {"request": request})


@router.get("/lost", response_class=HTMLResponse)
async def lost(request: Request):
    user_data = auth.get_user_data(request)
    return templates.TemplateResponse(
        "not_found.html", {"request": request, "user_data": user_data}
    )


@router.get("/registration", response_class=HTMLResponse)
async def register(request: Request):
    return templates.TemplateResponse("registration.html", {"request": request})


@router.post("/registration", response_class=HTMLResponse)
async def register_post(
    request: Request,
    username: str = Form(...),
    email: str = Form(...),
    password: str = Form(...),
    password_confirm: str = Form(...),
):
    if password != password_confirm:
        return templates.TemplateResponse(
            "registration.html", {"request": request, "error": "passwords don't match"}
        )

    try:
        auth.create_user(username, password, email)
    except UsernameNotUnique:
        return templates.TemplateResponse(
            "registration.html",
            {
                "request": request,
                "error": "This username is already taken, try another one",
            },
        )

    return await signin_post(request, username, password)


@router.post("/signin")
async def signin_post(
    request: Request, username: str = Form(...), password: str = Form(...)
):
    try:
        access, refresh = auth.authenticate(username=username, password=password)
    except BadCredentials:
        return templates.TemplateResponse(
            "signin.html", {"request": request, "error": "wrong password or username"}
        )

    response = RedirectResponse("/personal", status_code=303)
    response.set_cookie("access", access)
    response.set_cookie("refresh", refresh)
    return response


@router.post("/search", response_class=HTMLResponse)
async def search_post(request: Request, search_string: str = Form(...)):
    search_results = [
        {"title": "Found book", "author": "Author", "cover": "/img/book_cover.jpg"}
    ]

    return templates.TemplateResponse(
        "search_results.html",
        {"request": request, "results": search_results, "query": search_string},
    )
