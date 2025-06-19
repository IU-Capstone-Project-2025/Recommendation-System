import fastapi
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

# from fastapi.templates import Jinja2Templates
router = APIRouter()

# templates = Jinja2Templates(directory = "src/frontend/html")
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

