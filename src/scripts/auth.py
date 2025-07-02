from typing import Tuple
from fastapi import Request
from keycloak import KeycloakOpenID
from ldap3 import ALL, MODIFY_REPLACE, Connection, Server

from src import config
from src.scripts.exceptions import BadCredentials, UsernameNotUnique


keycloak_openid = KeycloakOpenID(
    server_url=config.KEYCLOAK_ORIGIN,
    client_id=config.KEYCLOAK_CLIENT_ID,
    client_secret_key=config.KEYCLOAK_CLIENT_SECRET_KEY,
    realm_name=config.KEYCLOAK_REALM_NAME,
)


def create_user(username: str, password: str, email: str):
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
            config.LLDAP_ORIGIN,
            port=int(config.LLDAP_PORT),  # pyright: ignore type
            use_ssl=False,
            get_info=ALL,
        ),
        user="uid=admin,ou=people,dc=example,dc=com",
        password=config.LLDAP_LDAP_USER_PASS,
    )
    ldap.bind()

    ldap.add(dn, attributes=attributes)

    res = ldap.result.__str__()
    if "code: 1555" in res:
        raise UsernameNotUnique

    ldap.modify(dn, changes={"userPassword": [(MODIFY_REPLACE, password)]})


def authenticate(username: str, password: str) -> Tuple[str, str]:

    try:
        token = keycloak_openid.token(username, password, scope="openid profile email")
    except:
        raise BadCredentials
    access_token = token["access_token"]
    refresh_token = token["refresh_token"]
    return access_token, refresh_token


def get_user_data(request: Request) -> dict:
    try:
        access_token = request.state.access_token
    except:
        return {}
    if access_token == None:
        return {}
    data = keycloak_openid.userinfo(access_token)
    return data
