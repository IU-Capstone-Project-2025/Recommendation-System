from os import environ
import fastapi
from fastapi import APIRouter, Form, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from ldap3 import ALL, MODIFY_ADD, MODIFY_REPLACE, Connection, Server
from dotenv import load_dotenv
from keycloak import KeycloakOpenID

from src.scripts.check_auth import get_auth_data

load_dotenv()

lldap_port = environ.get("LLDAP_HTTPS_CONN")
assert lldap_port != None

# from fastapi.templates import Jinja2Templates
router = APIRouter()

templates = Jinja2Templates(directory="src/frontend/html")


@router.get("/", response_class=HTMLResponse)
async def root(request: Request):
    username = request.cookies["username"] if "username" in request.cookies else None
    return templates.TemplateResponse(
        "index.html", {"request": request, "username": username}
    )


@router.get("/catalog", response_class=HTMLResponse)
async def catalog(request: Request):
    username = request.cookies["username"] if "username" in request.cookies else None
    return templates.TemplateResponse(
        "catalog.html", {"request": request, "username": username}
    )


@router.get("/personal", response_class=HTMLResponse)
async def personal(request: Request):
    username = request.cookies["username"] if "username" in request.cookies else None
    return templates.TemplateResponse(
        "personal_account.html", {"request": request, "username": username}
    )


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
    attributes = {
        "objectClass": ["inetOrgPerson", "person", "top"],
        "uid": username,
        "cn": username,
        "sn": username,
        "mail": email,
        "userPassword": password,
    }
    dn = f"uid={username},ou=people,dc=example,dc=com"
    ldap = Connection(
        Server(
            "lldap",
            port=int(lldap_port),  # pyright: ignore type
            use_ssl=False,
            get_info=ALL,
        ),
        user="uid=admin,ou=people,dc=example,dc=com",
        password=environ.get("LLDAP_LDAP_USER_PASS"),
    )
    ldap.bind()

    ldap.add(dn, attributes=attributes)
    ldap.modify(dn, changes={"userPassword": [(MODIFY_REPLACE, password)]})

    return await signin_post(request, username, password)


@router.post("/signin")
async def signin_post(
    request: Request, username: str = Form(...), password: str = Form(...)
):
    # keycloak_openid = KeycloakOpenID(
    #     server_url=environ.get("KEYCLOAK_ORIGIN"),
    #     client_id=environ.get("KEYCLOAK_CLIENT_ID"),
    #     client_secret_key=environ.get("KEYCLOAK_CLIENT_SECRET_KEY"),
    #     realm_name="backend",
    # )

    # try:
    #     token = keycloak_openid.token(username, password)
    #     access_token = token["access_token"]
    #     refresh_token = token["refresh_token"]
    # except:
    #     return templates.TemplateResponse(
    #         "signin.html", {"request": request, "error": "wrong password or username"}
    #     )
    get_auth_data(username=username, password=password)

    response = RedirectResponse("/personal", status_code=303)
    response.set_cookie("username", username)
    return response
