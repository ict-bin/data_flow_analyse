"""API router package for secflow-app-dataflow-analyse."""

from fastapi import APIRouter

router = APIRouter(prefix="/api/app/dataflow-analyse")

from . import tasks, config, prompts  # noqa: E402, F401
