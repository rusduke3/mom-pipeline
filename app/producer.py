import builtins
from datetime import datetime

# Добавляем автоматический таймстемп ко всем сообщениям в консоли
_orig_print = builtins.print
def print(*args, **kwargs):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _orig_print(f"[{ts}]", *args, **kwargs)

import os
import json
import uuid
import hashlib
import secrets
from pathlib import Path
from typing import Optional, Literal

import redis
from confluent_kafka import Producer
from fastapi import FastAPI, File, Form, UploadFile, Depends, HTTPException, Request, status
from fastapi.responses import HTMLResponse, Response
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from app.config import (
    BASE_DIR,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    REDIS_URL,
    TEMP_MEDIA_DIR,
    AUTH_USER,
    AUTH_PASS,
    GUEST_USER,
    GUEST_PASS,
)

# Ограничения на размер и формат загружаемых записей встреч
MAX_FILE_SIZE_MB = 150
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
ALLOWED_EXTENSIONS = {".mp4", ".mkv", ".avi", ".mov", ".webm", ".mp3", ".wav", ".m4a", ".ogg", ".flac"}

TEMP_MEDIA_DIR.mkdir(parents=True, exist_ok=True)
app = FastAPI(title="Enterprise MoM Pipeline")
security = HTTPBasic()
rdb = redis.Redis.from_url(REDIS_URL, decode_responses=True)
kafka_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})


def verify_auth(request: Request, credentials: HTTPBasicCredentials = Depends(security)) -> str:
    """Проверяет Basic Auth с защитой от timing-атак и блокировкой перебора паролей в Redis."""
    client_ip = request.headers.get("X-Real-IP") or (request.client.host if request.client else "unknown")
    ban_key = f"auth_ban:{client_ip}"
    fail_key = f"auth_fail:{client_ip}"

    if rdb.exists(ban_key):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many failed login attempts. Try again in 15 minutes.",
        )

    admin_ok = (
        secrets.compare_digest(credentials.username.encode("utf8"), AUTH_USER.encode("utf8"))
        and secrets.compare_digest(credentials.password.encode("utf8"), AUTH_PASS.encode("utf8"))
    )
    guest_ok = bool(GUEST_USER and GUEST_PASS) and (
        secrets.compare_digest(credentials.username.encode("utf8"), GUEST_USER.encode("utf8"))
        and secrets.compare_digest(credentials.password.encode("utf8"), GUEST_PASS.encode("utf8"))
    )

    if not (admin_ok or guest_ok):
        fails = rdb.incr(fail_key)
        rdb.expire(fail_key, 600)
        if fails >= 5:
            rdb.setex(ban_key, 900, "1")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Auth failed",
            headers={"WWW-Authenticate": "Basic"},
        )

    rdb.delete(fail_key)
    return credentials.username


# Подставляем актуальные лимиты размера файла в HTML-шаблон при старте сервера
HTML_PAGE = (
    (BASE_DIR / "app" / "index.html")
    .read_text(encoding="utf-8")
    .replace("{MAX_MB}", str(MAX_FILE_SIZE_MB))
    .replace("{MAX_BYTES}", str(MAX_FILE_SIZE_BYTES))
)


@app.get("/", response_class=HTMLResponse)
async def index(user: str = Depends(verify_auth)):
    """Отдаёт главную веб-страницу интерфейса."""
    return HTML_PAGE


@app.post("/api/tasks")
async def create_task(
    file: UploadFile = File(...),
    route: Literal["nda", "public"] = Form("nda"),
    meeting_context: Optional[str] = Form(""),
    force_recompute: bool = Form(False),
    user: str = Depends(verify_auth),
):
    """Принимает медиафайл, считает SHA-256 хеш для дедупликации и ставит задачу в очередь Kafka."""
    _, raw_ext = os.path.splitext(file.filename or "")
    ext = raw_ext.lower() or ".mp4"
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=400, detail=f"Неподдерживаемый формат: {ext}")

    # Генерируем безопасное имя файла на базе UUID для защиты от Path Traversal
    task_id = uuid.uuid4().hex
    safe_name = f"{task_id}{ext}"
    target_path = TEMP_MEDIA_DIR / safe_name

    # Потоково сохраняем файл на диск с проверкой лимита 150 МБ и расчётом SHA-256
    hasher = hashlib.sha256()
    total_bytes = 0
    try:
        with target_path.open("wb") as buf:
            while chunk := file.file.read(1024 * 1024):
                total_bytes += len(chunk)
                if total_bytes > MAX_FILE_SIZE_BYTES:
                    raise HTTPException(
                        status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                        detail=f"Превышен лимит {MAX_FILE_SIZE_MB} МБ",
                    )
                hasher.update(chunk)
                buf.write(chunk)
    except Exception:
        target_path.unlink(missing_ok=True)
        raise

    file_hash = hasher.hexdigest()

    # Регистрируем задачу в Redis и публикуем сообщение в топик Kafka
    payload = {
        "task_id": task_id,
        "filename": safe_name,
        "file_hash": file_hash,
        "route": route,
        "meeting_context": (meeting_context or "").strip(),
        "force_recompute": bool(force_recompute),
    }
    rdb.hset(f"task:{task_id}", mapping={"status": "queued", "route": route, "stage": "В очереди Kafka"})
    kafka_producer.produce(
        KAFKA_TOPIC,
        key=task_id.encode("utf-8"),
        value=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
    )
    kafka_producer.flush()
    return {"task_id": task_id, "status": "queued", "file_hash": file_hash[:12]}


@app.get("/api/tasks/{task_id}")
async def get_task(task_id: str, user: str = Depends(verify_auth)):
    """Возвращает текущий статус обработки или готовый протокол из Redis."""
    data = rdb.hgetall(f"task:{task_id}")
    if not data:
        raise HTTPException(status_code=404, detail="Not found")
    return data


@app.get("/api/tasks/{task_id}/transcript")
async def download_transcript(task_id: str, user: str = Depends(verify_auth)):
    """Формирует и отдаёт текстовый файл (.txt) со стенограммой и списком участников."""
    data = rdb.hgetall(f"task:{task_id}")
    if not data:
        raise HTTPException(status_code=404, detail="Not found")
    transcript = data.get("transcript", "").strip()
    if not transcript:
        raise HTTPException(status_code=404, detail="Транскрибация недоступна")

    participants = data.get("participants", "").strip()
    header = f"=== ТРАНСКРИБАЦИЯ ВСТРЕЧИ (ID: {task_id}) ===\n"
    if participants:
        header += f"Участники (VLM OCR): {participants}\n"
    header += "=" * 46 + "\n\n"

    return Response(
        content=(header + transcript).encode("utf-8"),
        media_type="text/plain; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="transcript_{task_id}.txt"'},
    )