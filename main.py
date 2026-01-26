from fastapi import FastAPI
from auto_caller_logic.router import router as auto_caller_router
from settings_backend.routers import router as settings_router

app = FastAPI(title="Auto Dialer Web Service", version="1.0.0")

# Include routers
app.include_router(auto_caller_router)
app.include_router(settings_router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

