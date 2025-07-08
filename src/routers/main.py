from fastapi import APIRouter, Form, Request
from fastapi.responses import RedirectResponse
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.encoders import jsonable_encoder

from src import config
from src.scripts import auth
from src.scripts.exceptions import BadCredentials, UsernameNotUnique
from src.constants import TOP_LIST

from src.scripts.book_list import BookList
from src.scripts.search import Search
from src.scripts.user_list import UserList
from src.scripts.user_stats import UserStats


# from fastapi.templates import Jinja2Templates
router = APIRouter()

templates = Jinja2Templates(directory="src/frontend/html")


@router.get("/", response_class=HTMLResponse)
async def root(request: Request):
    user_data = auth.get_user_data(request)
    print(user_data)
    return templates.TemplateResponse(
        "index.html", {"request": request, "user_data": user_data}
    )


@router.get("/catalog", response_class=HTMLResponse)
async def catalog(request: Request):
    user_data = auth.get_user_data(request)
    book_list = BookList(TOP_LIST)
    return templates.TemplateResponse(
        "catalog.html",
        {"request": request, "user_data": user_data, "books": book_list.get_book_list(0)},
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
        "personal_account.html", {"request": request, "user_data": user_data, "user_lists": [completed, reading, planned], "user_stats": user_stats}
    )


@router.get("/book", response_class=HTMLResponse)
async def account(request: Request):
    user_data = auth.get_user_data(request)
    return templates.TemplateResponse(
        "book_info.html", {"request": request, "user_data": user_data}
    )

@router.post("/search", response_class=HTMLResponse)
async def search(request: Request):
    
    import os

    data = await request.form()
    data = jsonable_encoder(data)
    search_string = data[search_string]

    
    import subprocess

    result = subprocess.run(
        ["./src/scripts/searching_mechanism/levenshtein_length", search_string],
        capture_output=True,
        text=True,
        check=True,
    )

    output_lines = result.stdout.splitlines()


    cleaned_lines = [
        line.strip() 
        for line in output_lines 
        if line.strip()
    ]

    search_instance = Search(cleaned_lines)
    books = search_instance.get_search_result()

    user_data = auth.get_user_data(request)

    return templates.TemplateResponse(
        "catalog.html",
        {"request": request, "user_data": user_data, "books": books},
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
