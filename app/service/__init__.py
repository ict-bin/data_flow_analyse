"""Service package for secflow-app-dataflow-analyse."""

from .task_service import generate_prompt_from_path, get_task_service, TaskService

__all__ = ["generate_prompt_from_path", "get_task_service", "TaskService"]

