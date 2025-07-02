from fastapi import APIRouter, Form, Request
from fastapi.responses import RedirectResponse
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from src import config
from src.scripts import auth
from src.scripts.exceptions import BadCredentials, UsernameNotUnique

# from fastapi.templates import Jinja2Templates
router = APIRouter()

templates = Jinja2Templates(directory="src/frontend/html")


@router.get("/", response_class=HTMLResponse)
async def root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@router.get("/catalog", response_class=HTMLResponse)
async def catalog(request: Request):
    return templates.TemplateResponse("catalog.html", {"request": request})


@router.get("/personal", response_class=HTMLResponse)
async def personal(request: Request):
    return templates.TemplateResponse("personal_account.html", {"request": request})


@router.get("/book", response_class=HTMLResponse)
async def account(request: Request):
    username = request.cookies["username"] if "username" in request.cookies else None
    return templates.TemplateResponse(
        "book_info.html", {"request": request, "username": username}
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
