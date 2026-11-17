from pathlib import Path
import os, json, uuid, shutil, hashlib, secrets
import redis
from confluent_kafka import Producer
from fastapi import FastAPI, File, Form, UploadFile, Depends, HTTPException, status
from fastapi.responses import HTMLResponse, Response
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from typing import Optional
from app.config import *

MAX_FILE_SIZE_MB = 150
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
ALLOWED_EXTENSIONS = {".mp4", ".mkv", ".avi", ".mov", ".webm", ".mp3", ".wav", ".m4a", ".ogg", ".flac"}

COMFY_INPUT_DIR.mkdir(parents=True, exist_ok=True)
app = FastAPI(title="Enterprise MoM Pipeline")
security = HTTPBasic()
rdb = redis.Redis.from_url(REDIS_URL, decode_responses=True)
kafka_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

def verify_auth(credentials: HTTPBasicCredentials = Depends(security)):
    u_ok = secrets.compare_digest(credentials.username.encode("utf8"), AUTH_USER.encode("utf8"))
    p_ok = secrets.compare_digest(credentials.password.encode("utf8"), AUTH_PASS.encode("utf8"))
    if not (u_ok and p_ok):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Auth failed", headers={"WWW-Authenticate": "Basic"})
    return credentials.username

HTML_PAGE = Path("app/index.html").read_text(encoding="utf-8").replace("{MAX_MB}", str(MAX_FILE_SIZE_MB)).replace("{MAX_BYTES}", str(MAX_FILE_SIZE_BYTES))

@app.get("/", response_class=HTMLResponse)
async def index(user: str = Depends(verify_auth)):
    return HTML_PAGE

@app.post("/api/tasks")
async def create_task(file: UploadFile = File(...), route: str = Form("nda"), meeting_context: Optional[str] = Form(""), user: str = Depends(verify_auth)):
    _, raw_ext = os.path.splitext(file.filename or "")
    ext = raw_ext.lower() or ".mp4"
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=400, detail=f"Неподдерживаемый формат: {ext}")

    task_id = str(uuid.uuid4())[:8]
    safe_name = f"{task_id}{ext}"
    target_path = COMFY_INPUT_DIR / safe_name

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

    payload = {
        "task_id": task_id,
        "filename": safe_name,
        "file_hash": file_hash,
        "route": route,
        "meeting_context": (meeting_context or "").strip(),
    }
    rdb.hset(f"task:{task_id}", mapping={"status": "queued", "route": route, "stage": "В очереди Kafka"})
    kafka_producer.produce(KAFKA_TOPIC, key=task_id.encode("utf-8"), value=json.dumps(payload, ensure_ascii=False).encode("utf-8"))
    kafka_producer.flush()
    return {"task_id": task_id, "status": "queued", "file_hash": file_hash[:12]}

@app.get("/api/tasks/{task_id}")
async def get_task(task_id: str, user: str = Depends(verify_auth)):
    data = rdb.hgetall(f"task:{task_id}")
    if not data: raise HTTPException(status_code=404, detail="Not found")
    return data

@app.get("/api/tasks/{task_id}/transcript")
async def download_transcript(task_id: str, user: str = Depends(verify_auth)):
    data = rdb.hgetall(f"task:{task_id}")
    if not data:
        raise HTTPException(status_code=404, detail="Not found")
    transcript = data.get("transcript", "").strip()
    if not transcript:
        raise HTTPException(status_code=404, detail="Транскрибация недоступна")
    participants = data.get("participants", "").strip()
    header = f"=== ТРАНСКРИБАЦИЯ ВСТРЕЧИ (ID: {task_id}) ===\n"
    if participants:
        header += f"Участники (OCR): {participants}\n"
    header += "=" * 46 + "\n\n"
    return Response(
        content=(header + transcript).encode("utf-8"),
        media_type="text/plain; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="transcript_{task_id}.txt"'},
    )
