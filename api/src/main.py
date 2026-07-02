from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from src.config import settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    lifespan=lifespan,
    swagger_ui_parameters={"syntaxHighlight": {"theme": "obsidian"}}
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

from src.routers import ask as ask_router
from src.routers import disasters as disasters_router
from src.routers import ontology as ontology_router
from src.routers import map as map_router
from src.routers import sparql as sparql_router
# app.include_router(ask_router.router)
# app.include_router(disasters_router.router)
# app.include_router(ontology_router.router)
# app.include_router(map_router.router)
# app.include_router(sparql_router.router)
app.include_router(ask_router.router, prefix="/api")
app.include_router(disasters_router.router, prefix="/api")
app.include_router(ontology_router.router, prefix="/api")
app.include_router(map_router.router, prefix="/api")
app.include_router(sparql_router.router, prefix="/api")


@app.get("/health", tags=["meta"])
async def health():
    return {"status": "ok"}
