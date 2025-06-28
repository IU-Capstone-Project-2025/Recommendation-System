from os import environ
from dotenv import load_dotenv


load_dotenv()


def get_var(key: str) -> str:
    var = environ.get(key)
    assert var, f"required env variable {key} not found"
    return var


KEYCLOAK_ORIGIN = get_var("KEYCLOAK_ORIGIN")
KEYCLOAK_CLIENT_ID = get_var("KEYCLOAK_CLIENT_ID")
KEYCLOAK_CLIENT_SECRET_KEY = get_var("KEYCLOAK_CLIENT_SECRET_KEY")
KEYCLOAK_REALM_NAME = get_var("KEYCLOAK_REALM_NAME")

LLDAP_ORIGIN = get_var("LLDAP_ORIGIN")
LLDAP_PORT = int(get_var("LLDAP_HTTPS_CONN"))
LLDA_LDAP_USER_PASS = get_var("LLDAP_LDAP_USER_PASS")

POSTGRES_HOST = get_var("POSTGRES_HOST")
POSTGRES_PORT = get_var("POSTGRES_PORT")
POSTGRES_DB = get_var("POSTGRES_DB")
POSTGRES_USER = get_var("POSTGRES_USER")
POSTGRES_PASSWORD = get_var("POSTGRES_PASSWORD")
