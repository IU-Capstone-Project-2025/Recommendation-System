import os 

import uvicorn
import fastapi

from starlette.staticfiles import StaticFiles

from src.routers.main import router

#fastapi application
app = fastapi.FastAPI()


#include routers
app.include_router(router, tags=["Main"])

#mounting static data

app.mount("/css", StaticFiles(directory="src/frontend/css"), name="css")
app.mount("/js", StaticFiles(directory="src/frontend/js"), name="js")
app.mount("/img", StaticFiles(directory="src/frontend/img"), name="img")

@app.get('/api/healthchecker')
def root():
    return {'message': 'Hello World'}

def start():
    uvicorn.run(
        "src.microservices.recommendation_system_project:app", 
        host="0.0.0.0", 
        port=8000, 
        reload=True, 
        timeout_graceful_shutdown=10000000
    )    

if __name__ == "__main__":
    start()