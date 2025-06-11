import os 
import uvicorn

def start():
    uvicorn.run(
        "src.routers.main:app", 
        host="0.0.0.0", 
        port=8000, 
        reload=True, 
        timeout_graceful_shutdown=10000000
    )    

if __name__ == "__main__":
    start()