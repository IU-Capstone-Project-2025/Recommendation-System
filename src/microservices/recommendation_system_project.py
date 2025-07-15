import os

import uvicorn
import fastapi
import asyncio

from starlette.staticfiles import StaticFiles

from src.routers.main import router
from src.routers.feedback import router as score_router
from src import middlewares

# fastapi application
app = fastapi.FastAPI()

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


search_engine = None


def load_search_engine():
    from src.scripts.searching_mechanism.vector_searching import BookSearchEngine
    global search_engine
    engine = BookSearchEngine()
    engine.load_books("src/scripts/searching_mechanism/titles_only.csv")
    search_engine = engine


@app.get("/api/healthchecker")
def root():
    return {"message": "Healthy"}


def start():
    load_search_engine()
    uvicorn.run(
        "src.microservices.recommendation_system_project:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        timeout_graceful_shutdown=10000000,
    )


if __name__ == "__main__":
    start()
