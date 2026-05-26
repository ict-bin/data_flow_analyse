from __future__ import annotations

from typing import Dict, Optional, Tuple

from fastapi import Header, HTTPException

import httpx

from app.config import get_service_yaml


class AuthServiceError(Exception):
    pass


class TokenInvalidError(AuthServiceError):
    pass


def extract_bearer_token(authorization: Optional[str]) -> str:
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")
    token = authorization.strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Invalid Authorization header")
    return token


async def _validate_token(token: str, project_id: Optional[str] = None) -> dict:
    cfg = get_service_yaml().auth_service
    params = {"project_id": project_id} if project_id else None
    try:
        async with httpx.AsyncClient(timeout=cfg.timeout) as client:
            response = await client.post(
                cfg.validate_url,
                headers={"Authorization": f"Bearer {token}"},
                params=params,
            )
    except httpx.TimeoutException as exc:
        raise AuthServiceError("认证服务请求超时") from exc
    except httpx.ConnectError as exc:
        raise AuthServiceError(f"无法连接到认证服务: {exc}") from exc
    if response.status_code == 401:
        raise TokenInvalidError("Token已过期或无效")
    if response.status_code != 200:
        raise AuthServiceError(f"认证服务返回异常状态码: {response.status_code}")
    return response.json()


async def get_current_user(
    authorization: Optional[str] = Header(None, alias="Authorization"),
) -> Tuple[Dict, str]:
    token = extract_bearer_token(authorization)
    try:
        user = await _validate_token(token)
    except TokenInvalidError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except AuthServiceError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return user, token


async def ensure_project_access(project_id: str, token: str) -> Dict:
    try:
        return await _validate_token(token, project_id=project_id)
    except TokenInvalidError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except AuthServiceError as exc:
        raise HTTPException(status_code=403, detail=f"project access denied: {exc}") from exc


def ensure_admin_user(user: Dict) -> Dict:
    platform_role = str(user.get("platform_role") or "").strip()
    role_names = {str(item).strip() for item in (user.get("role") or []) if str(item).strip()}
    token_type = str(user.get("token_type") or "").strip().lower()
    if token_type == "machine":
        return user
    if platform_role in {"super_admin", "ordinary_admin"}:
        return user
    if {"super_admin", "admin", "ordinary_admin"} & role_names:
        return user
    raise HTTPException(status_code=403, detail="需要管理员权限")
