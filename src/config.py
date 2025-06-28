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
LLDAP_PORT = int(get_var("LLDAP_HTTPS_CONN"))
LLDA_LDAP_USER_PASS = get_var("LLDAP_LDAP_USER_PASS")
