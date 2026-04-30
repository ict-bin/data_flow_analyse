"""API router package for secflow-app-dataflow-analyse."""

from fastapi import APIRouter

router = APIRouter(prefix="/api/app/dataflow-analyse")

from . import tasks, config, models  # noqa: E402, F401
