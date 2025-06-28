from os import environ

from dotenv import load_dotenv
from keycloak import KeycloakOpenID

from src import config


load_dotenv()


def get_auth_data(username: str, password: str) -> dict:

    keycloak_openid = KeycloakOpenID(
        server_url=config.KEYCLOAK_ORIGIN,
        client_id=config.KEYCLOAK_CLIENT_ID,
        client_secret_key=config.KEYCLOAK_CLIENT_SECRET_KEY,
        realm_name="backend",
    )

    try:
        token = keycloak_openid.token(username, password)
        # access_token = token["access_token"]
        # refresh_token = token["refresh_token"]
        return token
    except:
        return {"error": "wrong password or username"}

