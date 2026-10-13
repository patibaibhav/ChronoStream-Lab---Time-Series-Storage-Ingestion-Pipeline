"""ASGI application entry point for the local development placeholder."""

from fastapi import FastAPI

app = FastAPI(
    title="Time-Series Virtual Lab",
    version="0.1.0",
    description="Local development placeholder. API features are added incrementally.",
)
