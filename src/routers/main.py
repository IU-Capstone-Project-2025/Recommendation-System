from fastapi import APIRouter, Form, Request
from fastapi.responses import RedirectResponse
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from src.scripts import auth
from src.scripts.book import Book
from src.scripts.exceptions import BadCredentials, ObjectNotFound, UsernameNotUnique
from src.constants import TOP_LIST

from src.scripts.book_list import BookList

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
    book_list = BookList(None, TOP_LIST)
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
    return templates.TemplateResponse(
        "personal_account.html", {"request": request, "user_data": user_data}
    )


@router.get("/book", response_class=HTMLResponse)
async def book(request: Request, id: int):
    user_data = auth.get_user_data(request)
    # try:
    #     book = Book(id)
    # except ObjectNotFound:
    #     return templates.TemplateResponse("404.html", {"request": request})

    return templates.TemplateResponse(
        "book_info.html",
        {
            "request": request,
            "user_data": user_data,
            "book": {"title": "title", "description": "description", "cover": ""},
        },
    )


@router.get("/signin", response_class=HTMLResponse)
async def signin_get(request: Request):
    return templates.TemplateResponse("signin.html", {"request": request})


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
