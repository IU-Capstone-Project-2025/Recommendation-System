import fastapi
from fastapi import APIRouter, Form, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

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
    return templates.TemplateResponse("book_info.html", {"request": request})


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
    password: str = Form(...),
    password_confirm: str = Form(...),
):
    if password != password_confirm:
        return templates.TemplateResponse(
            "registration.html", {"request": request, "error": "passwords don't match"}
        )


@router.post("/signin", response_class=RedirectResponse)
async def signin_post(
    request: Request, username: str = Form(...), password: str = Form(...)
):
    response = RedirectResponse("/personal", status_code=303)
    response.set_cookie("username", username)
    return response
