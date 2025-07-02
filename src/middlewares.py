import time
from typing import List
from fastapi import Request, Response
from fastapi.responses import RedirectResponse
from jose import jwt, JWTError
from keycloak import KeycloakOpenID

from src.scripts import auth


async def refresh(request: Request, call_next):
    try:
        access_token = request.cookies.get("access")
        refresh_token = request.cookies.get("refresh")
    except:
        return await call_next(request)
    request.state.access_token = access_token
    request.state.refresh_token = refresh_token

    if not access_token or not refresh_token:
        return await call_next(request)

    try:
        decoded_token = jwt.get_unverified_claims(access_token)
    except JWTError:
        request.state.access_token = None
        request.state.refresh_token = None
        response: Response = await call_next(request)
        response.delete_cookie("access")
        response.delete_cookie("refresh")
        return response

    exp = int(decoded_token.get("exp", 0))
    now = int(time.time())

    if now >= exp:
        try:
            new_tokens = auth.keycloak_openid.refresh_token(refresh_token)
        except:
            request.state.access_token = None
            request.state.refresh_token = None
            response: Response = await call_next(request)
            response.delete_cookie("access")
            response.delete_cookie("refresh")
            return response

        new_access = new_tokens["access_token"]
        new_refresh = new_tokens["refresh_token"]

        request.state.access_token = new_access
        request.state.refresh_token = new_refresh
        response: Response = await call_next(request)
        response.set_cookie("access", new_access)
        response.set_cookie("refresh", new_refresh)
        return response

    return await call_next(request)


def make_authorization_middleware(restricted_routes: List[str]):
    async def authorize(request: Request, call_next):
        user_data = auth.get_user_data(request)
        authorized = True if len(user_data.keys()) > 0 else False

        for route in restricted_routes:
            if request.url.path.startswith(route) and not authorized:
                return RedirectResponse("/signin")

        return await call_next(request)

    return authorize
