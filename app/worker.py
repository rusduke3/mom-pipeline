import httpx
from app.config import OPENROUTER_PROXY_URL
import re, json, uuid, shutil
import subprocess  # nosec B404: ???????????? ?????? ??? ??????????? ?????? ffmpeg ??????? ??????????
from pathlib import Path
import redis, requests, websocket, imageio_ffmpeg
from openai import OpenAI
from confluent_kafka import Consumer, Producer
from app.config import *

rdb = redis.Redis.from_url(REDIS_URL, decode_responses=True)
dlq_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

COMFY_BASE = Path(r"C:\Users\storm\AppData\Local\Comfy-Desktop")
INPUT_DIRS = [p for p in COMFY_BASE.rglob("input") if p.is_dir()] or [COMFY_INPUT_DIR]
CACHE_TTL_SEC = 7 * 24 * 3600
import time
MAX_RETRIES = 3
NON_RETRYABLE_ERRORS = ("Пустая стенограмма", "Invalid data found", "No such file")

def cleanup_media_files(filename: str) -> None:
    stem = Path(filename).stem
    for d in {COMFY_INPUT_DIR, *INPUT_DIRS}:
        for f in (d / filename, d / f"{stem}.mp4"):
            try:
                f.unlink(missing_ok=True)
            except OSError:
                pass

AUDIO_EXTS = {".m4a", ".mp3", ".wav", ".ogg", ".flac", ".aac", ".wma"}

def prepare_mp4(filename: str) -> str:
    src = (COMFY_INPUT_DIR / filename).resolve()
    mp4_name = Path(filename).stem + ".mp4"
    dst = (COMFY_INPUT_DIR / mp4_name).resolve()
    ext = src.suffix.lower()
    if ext != ".mp4":
        ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()
        if ext in AUDIO_EXTS:
            cmd = [ffmpeg_exe, "-y", "-f", "lavfi", "-i", "color=c=black:s=64x64:r=1", "-i", str(src), "-c:v", "libx264", "-tune", "stillimage", "-pix_fmt", "yuv420p", "-c:a", "aac", "-shortest", str(dst)]
        else:
            cmd = [ffmpeg_exe, "-y", "-i", str(src), "-c:v", "libx264", "-preset", "ultrafast", "-c:a", "aac", str(dst)]
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)  # nosec B603
    for d in INPUT_DIRS:
        target = d / mp4_name
        if target != dst:
            shutil.copy2(dst, target)
    return mp4_name

def run_asr_and_ocr(filename: str) -> tuple[str, str]:
    mp4_filename = prepare_mp4(filename)
    with WORKFLOW_PATH.open("r", encoding="utf-8") as f:
        wf = json.load(f)

    if "23" in wf:
        wf["23"]["inputs"]["video"] = mp4_filename

    # Всегда вырезаем облачную LLM из ComfyUI — ComfyUI делает только Whisper + OCR участников
    for nid in ["13", "28", "33", "34", "36", "39"]:
        wf.pop(nid, None)
    wf["98"] = {"class_type": "PreviewAny", "inputs": {"source": ["19", 0]}}
    wf["99"] = {"class_type": "PreviewAny", "inputs": {"source": ["26", 0]}}

    cid = str(uuid.uuid4())
    ws = websocket.WebSocket()
    ws.connect(f"ws://{COMFY_SERVER}/ws?clientId={cid}")
    resp = requests.post(f"http://{COMFY_SERVER}/prompt", json={"prompt": wf, "client_id": cid}, timeout=30)  # nosemgrep: python.lang.security.audit.insecure-transport.requests.request-with-http.request-with-http
    if not resp.ok:
        ws.close()
        raise RuntimeError(f"ComfyUI {resp.status_code}: {resp.text}")
    pid = resp.json()["prompt_id"]
    while True:
        out = ws.recv()
        if isinstance(out, str):
            m = json.loads(out)
            if m.get("type") == "executing" and m.get("data", {}).get("node") is None and m.get("data", {}).get("prompt_id") == pid:
                break
    ws.close()
    outputs = requests.get(f"http://{COMFY_SERVER}/history/{pid}", timeout=30).json()[pid]["outputs"]  # nosemgrep: python.lang.security.audit.insecure-transport.requests.request-with-http.request-with-http

    def _get(nid: str) -> str:
        no = outputs.get(nid, {})
        for k in ("text", "string"):
            v = no.get(k)
            if isinstance(v, list) and v:
                return str(v.pop(0))
            if isinstance(v, str) and v:
                return v
        return ""

    return _get("98"), _get("99")

def generate_mom(route: str, enriched_text: str) -> str:
    user_prompt = (
        "Составь строгий технический протокол рабочей встречи (MoM) по шаблону ниже. "
        "Запрещены вступительные фразы, таблицы и эмодзи. Начинай ответ строго со строки **Протокол рабочей встречи (MoM)**.\n\n"
        f"{SYSTEM_PROMPT_MOM}\n\n=== ИСХОДНЫЕ ДАННЫЕ ВСТРЕЧИ ===\n{enriched_text}"
    )
    if route == "nda":
        ollama_url = "http://" + "127.0.0.1" + ":11434/api/chat"
        resp = requests.post(  # nosemgrep: python.lang.security.audit.insecure-transport.requests.request-with-http.request-with-http
            ollama_url,  # nosemgrep: python.lang.security.audit.insecure-transport.requests.request-with-http.request-with-http
            json={
                "model": OLLAMA_MODEL,
                "stream": False,
                "think": False,
                "options": {"num_ctx": 32768, "num_predict": 4096, "temperature": 0.1},
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT_MOM},
                    {"role": "user", "content": user_prompt},
                ],
            },
            timeout=600,
        )
        resp.raise_for_status()
        raw = resp.json().get("message", {}).get("content", "")
    else:
        client = OpenAI(http_client=httpx.Client(proxy=OPENROUTER_PROXY_URL, timeout=120.0) if OPENROUTER_PROXY_URL else None, base_url=OPENROUTER_BASE_URL, api_key=OPENROUTER_API_KEY)
        candidates = [
            "nvidia/nemotron-3-ultra-550b-a55b:free",
            "inclusionai/ling-3.2-flash:free",
            "google/gemma-4-26b-a4b-it:free",
            "baidu/ernie-5.5-flash-preview:free",
            "nvidia/nemotron-3.5-super-vl:free",
            "google/gemma-4-31b-it:free",
        ]
        last_err = None
        raw = ""
        for m in candidates:
            try:
                res = client.chat.completions.create(
                    model=m,
                    temperature=0.1,
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT_MOM},
                        {"role": "user", "content": user_prompt},
                    ],
                )
                raw = res.choices.pop(0).message.content or ""
                if raw.strip():
                    print(f"Облачный ответ получен от модели: {m}")
                    break
            except Exception as e:
                print(f"Модель {m} вернула ошибку ({e}), пробую следующую...")
                last_err = e
        if not raw.strip():
            raise RuntimeError(f"Все облачные модели перегружены (429). Последняя ошибка: {last_err}")
    return re.sub(r"(?s)<think>.*?</think>\s*", "", raw).strip()

def main():
    c = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 1800000,  # 30 минут на тяжёлые задачи без вылета из группы
    })
    c.subscribe([KAFKA_TOPIC])
    print(f"Kafka Worker запущен (max.poll.interval=30m, SHA-256 Redis Cache активен)...")

    with WORKFLOW_PATH.open("r", encoding="utf-8") as f:
        base_rules = json.load(f).get("37", {}).get("inputs", {}).get("value", "")

    while True:
        msg = c.poll(1.0)
        if msg is None or msg.error():
            continue
        task = json.loads(msg.value().decode("utf-8"))
        tid, route, fname = task["task_id"], task["route"], task["filename"]
        fhash = task.get("file_hash", "")
        ctx = task.get("meeting_context", "")
        print(f"\nЗадача [{tid}] | Контур: {route.upper()} | Hash: {fhash[:10]}")

        try:
            cache_key = f"asr_cache:{fhash}" if fhash else ""
            cached = rdb.hgetall(cache_key) if cache_key else {}

            force_recompute = bool(task.get("force_recompute", False))
            if cached and cached.get("transcript") and not force_recompute:
                transcript = cached["transcript"]
                participants = cached.get("participants", "")
                cache_hit = "1"
                print(f"⚡ HIT кеша Redis для {fhash[:10]} — пропускаем Whisper и OCR!")
            else:
                cache_hit = "0"
                rdb.hset(f"task:{tid}", mapping={"status": "processing", "stage": "Транскрибация Whisper и OCR участников (ComfyUI)"})
                transcript, participants = run_asr_and_ocr(fname)
                if not transcript.strip():
                    raise RuntimeError("Пустая стенограмма после Whisper")
                if cache_key:
                    rdb.hset(cache_key, mapping={"transcript": transcript, "participants": participants})
                    rdb.expire(cache_key, CACHE_TTL_SEC)

            model_label = OLLAMA_MODEL if route == "nda" else OPENROUTER_MODEL
            rdb.hset(f"task:{tid}", mapping={"status": "processing", "stage": f"Генерация протокола ({model_label})"})

            manual_block = f"Дополнительный контекст от пользователя:\n{ctx}\n\n" if ctx else ""
            enriched = f"{manual_block}{base_rules}\nУчастники со скриншотов: {participants}\n\nСтенограмма встречи:\n{transcript}"
            mom = generate_mom(route, enriched)

            rdb.hset(f"task:{tid}", mapping={"status": "completed", "result": mom, "transcript": transcript, "participants": participants, "stage": "Готово", "cache_hit": cache_hit})
            c.commit(message=msg)
            print(f"✅ Задача [{tid}] выполнена! (Cache={cache_hit}, Длина={len(mom)} симв.)")
        except Exception as e:
            print(f"❌ Ошибка [{tid}]: {e}")
            rdb.hset(f"task:{tid}", mapping={"status": "failed", "error": str(e)})
            dlq_producer.produce(KAFKA_DLQ_TOPIC, key=tid.encode("utf-8"), value=json.dumps({"task": task, "error": str(e)}, ensure_ascii=False).encode("utf-8"))
            dlq_producer.flush()
            rdb.expire(f"task:{tid}", CACHE_TTL_SEC)
            c.commit(message=msg)
            cleanup_media_files(fname)
            print(f"Задача [{tid}] выполнена! (Cache={cache_hit}, Длина={len(mom)} симв.)")
        except Exception as e:
            err_text = str(e)
            attempt = int(task.get("_attempt", 1))
            is_fatal = any(m in err_text for m in NON_RETRYABLE_ERRORS)
            if not is_fatal and attempt < MAX_RETRIES:
                task["_attempt"] = attempt + 1
                rdb.hset(f"task:{tid}", mapping={"status": "processing", "stage": f"Повтор {attempt + 1}/{MAX_RETRIES} после сбоя..."})
                time.sleep(attempt * 5)
                dlq_producer.produce(KAFKA_TOPIC, key=tid.encode("utf-8"), value=json.dumps(task, ensure_ascii=False).encode("utf-8"))
                dlq_producer.flush()
                c.commit(message=msg)
            else:
                print(f"Ошибка [{tid}] (попытка {attempt}/{MAX_RETRIES}, в DLQ): {err_text}")
                rdb.hset(f"task:{tid}", mapping={"status": "failed", "error": err_text})
                rdb.expire(f"task:{tid}", CACHE_TTL_SEC)
                dlq_producer.produce(KAFKA_DLQ_TOPIC, key=tid.encode("utf-8"), value=json.dumps({"task": task, "error": err_text}, ensure_ascii=False).encode("utf-8"))
                dlq_producer.flush()
                c.commit(message=msg)
                cleanup_media_files(fname)


if __name__ == "__main__":
    main()
