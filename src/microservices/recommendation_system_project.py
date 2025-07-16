import uvicorn
import fastapi


from starlette.staticfiles import StaticFiles

from src.routers.main import router
from src.routers.feedback import router as score_router
from src import middlewares

# fastapi application
app = fastapi.FastAPI( title="Reccomendation System backend API",
    description="This is an API for our Books recommendation system for capstone project in Innopolis University.\n \
          In our stack we use FastAPI+HTML+CSS+JS+Jinja2 for backend&frontend, Keycloak+Lldap for authentication, Airflow+ClickHouse for data processing and PostgreSQL for data storing. ",
    contact={
        "Github": "https://github.com/IU-Capstone-Project-2025/Recommendation-System"
    },
    license_info={
        "name": "MIT",
    },
)

# inject middlewares
app.middleware("http")(
    middlewares.make_authorization_middleware(["/personal", "/set_status"])
)
app.middleware("http")(middlewares.refresh)

# include routers
app.include_router(router, tags=["Main"])
app.include_router(score_router, tags=["Score"])

# mounting static data
app.mount("/css", StaticFiles(directory="src/frontend/css"), name="css")
app.mount("/js", StaticFiles(directory="src/frontend/js"), name="js")
app.mount("/img", StaticFiles(directory="src/frontend/img"), name="img")


# search_engine = None

# @app.on_event("startup")
# async def startup_event():
#     """Initialize the search engine when the application starts."""
#     global search_engine
#     from src.scripts.searching_mechanism.vector_searching import BookSearchEngine
#     engine = BookSearchEngine()
#     engine.load_books("src/scripts/searching_mechanism/titles_only.csv")
#     search_engine = engine


@app.get("/api/healthchecker",
 description="This is a health checker handler for our application.\
     We use it to check if our application is up and running during deployment.",
    tags=["Health checker"],
    responses={
        200: {"description": "Successful response. Service is up"},
        404: {"description": "Service is down"},
    }
)
def root():
    return {"message": "Healthy"}


def start():
    uvicorn.run(
        "src.microservices.recommendation_system_project:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        timeout_graceful_shutdown=10000000,
    )


if __name__ == "__main__":
    start()
