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

HTML_PAGE = """<!DOCTYPE html><html lang="ru"><head><meta charset="UTF-8"><title>AI MoM Enterprise</title>
<style>
body{font-family:system-ui,sans-serif;background:#0f172a;color:#e2e8f0;max-width:820px;margin:40px auto;padding:0 20px}
.card{background:#1e293b;border-radius:12px;padding:28px;border:1px solid #334155}
label{display:block;margin-top:18px;font-weight:600;font-size:14px}
input[type="file"],textarea{width:100%;margin-top:8px;padding:10px;border-radius:8px;border:1px solid #475569;background:#0f172a;color:#f8fafc;box-sizing:border-box}
.rg{display:flex;gap:16px;margin-top:10px}.rb{flex:1;padding:14px;border-radius:8px;border:2px solid #334155;cursor:pointer;background:#0f172a}
.rb.nda:has(input:checked){border-color:#ef4444;background:rgba(239,68,68,.1)}
.rb.pub:has(input:checked){border-color:#22c55e;background:rgba(34,197,94,.1)}
button{margin-top:24px;width:100%;padding:14px;border:none;border-radius:8px;background:#3b82f6;color:#fff;font-size:16px;font-weight:600;cursor:pointer}
button:disabled{background:#475569}
#sb{margin-top:24px;padding:16px;border-radius:8px;background:#0f172a;border:1px solid #334155;display:none}
pre{white-space:pre-wrap;font-family:inherit;line-height:1.5;background:#0f172a;padding:16px;border-radius:8px;border:1px solid #334155}
.badge{display:inline-block;padding:2px 8px;border-radius:6px;font-size:12px;background:#0284c7;color:#fff;margin-left:8px}
</style></head><body><div class="card"><h2>Генератор протоколов встреч (Kafka + Redis Cache + Dual-LLM)</h2>
<form id="f"><label>1. Запись встречи (MP4, MKV, MP3, WAV, WEBM):</label><input type="file" name="file" required>
<label>2. Контур обработки:</label><div class="rg">
<label class="rb nda"><input type="radio" name="route" value="nda" checked> <b>Контур NDA (Локально)</b><br><small>Qwen 3.6 35B на RTX 3070 Ti</small></label>
<label class="rb pub"><input type="radio" name="route" value="public"> <b>Контур Public (Облако)</b><br><small>OpenRouter API (OpenAI SDK)</small></label></div>
<label>3. Контекст встречи и глоссарий (необязательно):</label>
<textarea name="meeting_context" rows="3" placeholder="Участники, роли, термины (StatProc, RedOS 8, Hexagon)..."></textarea>
<button type="submit" id="btn">Отправить в очередь Kafka</button></form>
<div id="sb"><div id="st">Ожидание...</div><div id="ra" style="display:none;margin-top:16px">
<button style="background:#10b981;width:auto;padding:8px 16px;margin-top:0" onclick="navigator.clipboard.writeText(document.getElementById('out').innerText);alert('Скопировано!')">Скопировать протокол</button>
<pre id="out"></pre></div></div></div>
<script>
let t=null;document.getElementById('f').onsubmit=async(e)=>{e.preventDefault();const b=document.getElementById('btn'),sb=document.getElementById('sb'),st=document.getElementById('st'),ra=document.getElementById('ra');b.disabled=true;sb.style.display='block';ra.style.display='none';st.innerText='Загрузка и расчёт SHA-256...';
const r=await fetch('/api/tasks',{method:'POST',body:new FormData(e.target)}),d=await r.json();if(!r.ok){st.innerText='Ошибка: '+(d.detail||'сбой');b.disabled=false;return;}
st.innerText='В очереди Kafka (ID: '+d.task_id+')...';if(t)clearInterval(t);t=setInterval(async()=>{const sr=await fetch('/api/tasks/'+d.task_id),sd=await sr.json();
if(sd.status==='processing')st.innerText='В работе: '+(sd.stage||'обработка');
else if(sd.status==='completed'){clearInterval(t);const cBadge=sd.cache_hit==='1'?'<span class="badge">⚡ ASR из кеша Redis</span>':'';st.innerHTML='Готово! Контур: <b>'+sd.route.toUpperCase()+'</b>'+cBadge;document.getElementById('out').innerText=sd.result;ra.style.display='block';b.disabled=false;}
else if(sd.status==='failed'){clearInterval(t);st.innerText='Ошибка (в DLQ): '+sd.error;b.disabled=false;}},2000);};
</script></body></html>"""

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
