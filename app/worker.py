import builtins
from datetime import datetime
_orig_print = builtins.print
def print(*args, **kwargs):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _orig_print(f"[{ts}]", *args, **kwargs)
import os
import re
import gc
import json
import time
import base64
import subprocess  # nosec B404: используется только для локального вызова ffmpeg через список аргументов
from pathlib import Path

import httpx
import redis
import requests
import imageio_ffmpeg
os.environ["PATH"] = str(Path(imageio_ffmpeg.get_ffmpeg_exe()).parent) + os.pathsep + os.environ.get("PATH", "")
from openai import OpenAI
from confluent_kafka import Consumer, Producer
import torch
from faster_whisper import WhisperModel

from app.config import (
    REDIS_URL,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    KAFKA_DLQ_TOPIC,
    KAFKA_GROUP,
    COMFY_INPUT_DIR,
    OLLAMA_MODEL,
    OPENROUTER_MODEL,
    OPENROUTER_BASE_URL,
    OPENROUTER_API_KEY,
    OPENROUTER_PROXY_URL,
    SYSTEM_PROMPT_MOM,
    WORKFLOW_PATH,
)

rdb = redis.Redis.from_url(REDIS_URL, decode_responses=True)
dlq_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

CACHE_TTL_SEC = 7 * 24 * 3600
MAX_RETRIES = 3
NON_RETRYABLE_ERRORS = ("Пустая стенограмма", "Invalid data found", "No such file")
AUDIO_EXTS = {".m4a", ".mp3", ".wav", ".ogg", ".flac", ".aac", ".wma"}

WHISPER_MODEL_ID = os.getenv("WHISPER_MODEL_ID", str(Path(__file__).resolve().parent.parent / "models" / "whisper-ru-ct2"))
VLM_MODEL_ID = os.getenv("VLM_MODEL_ID", "qwen2.5vl:7b")
OLLAMA_API_BASE = os.getenv("OLLAMA_API_BASE", "http://127.0.0.1:11434")

def free_vram() -> None:
    gc.collect()
    try:
        import torch
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
            torch.cuda.ipc_collect()
    except ImportError:
        pass

def cleanup_media_files(filename: str) -> None:
    stem = Path(filename).stem
    for pattern in (filename, f"{stem}.wav", f"{stem}_frame_*.jpg"):
        for f in COMFY_INPUT_DIR.glob(pattern):
            try:
                f.unlink(missing_ok=True)
            except OSError:
                pass
def extract_wav(src_path: Path) -> Path:
    wav_path = src_path.with_suffix(".wav")
    ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()
    cmd = [ffmpeg_exe, "-y", "-i", str(src_path), "-vn", "-acodec", "pcm_s16le", "-ar", "16000", "-ac", "1", str(wav_path)]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)  # nosec B603
    return wav_path

def extract_video_frames(src_path: Path, max_frames: int = 3) -> list[str]:
    if src_path.suffix.lower() in AUDIO_EXTS:
        return []
    ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()
    stem = src_path.stem
    frame_pattern = src_path.parent / f"{stem}_frame_%02d.jpg"
    cmd = [ffmpeg_exe, "-y", "-i", str(src_path), "-vf", "fps=1/20,scale=1280:-1", "-frames:v", str(max_frames), "-q:v", "4", str(frame_pattern)]
    subprocess.run(cmd, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)  # nosec B603
    frames = []
    for img_path in sorted(src_path.parent.glob(f"{stem}_frame_*.jpg")):
        try:
            frames.append(base64.b64encode(img_path.read_bytes()).decode("ascii"))
        except OSError:
            pass
    return frames

def transcribe_audio_direct(wav_path: Path) -> str:
    print(f"Загрузка CTranslate2 Whisper ({WHISPER_MODEL_ID}) в GPU (int8_float16)...")
    model = WhisperModel(WHISPER_MODEL_ID, device="cuda", compute_type="int8_float16")
    try:
        segments, _ = model.transcribe(str(wav_path), language="ru", beam_size=5, vad_filter=True, vad_parameters={"min_silence_duration_ms": 500})
        return "\n".join(seg.text.strip() for seg in segments if seg.text.strip())
    finally:
        del model
        free_vram()
        print("Whisper выгружен из VRAM.")

def extract_participants_vlm(frames_b64: list[str]) -> str:
    if not frames_b64:
        return ""
    print(f"Распознавание участников на {len(frames_b64)} кадрах через {VLM_MODEL_ID}...")
    prompt = "Посмотри на скриншоты видеоконференции. Выпиши исключительно список имён, фамилий и ролей участников встречи, которые видны в подписях к окнам или в списке участников. Если имён на экране нет, напиши: Не определены с экрана."
    try:
        resp = requests.post(  # nosemgrep: python.lang.security.audit.insecure-transport.requests.request-with-http.request-with-http
            f"{OLLAMA_API_BASE}/api/generate",
            json={"model": VLM_MODEL_ID, "prompt": prompt, "images": frames_b64, "stream": False, "keep_alive": 0, "options": {"temperature": 0.1}},
            timeout=120,
        )
        if resp.ok:
            return resp.json().get("response", "").strip()
    except Exception as e:
        print(f"VLM OCR пропущен ({e})")
    return ""

def run_asr_and_ocr(filename: str) -> tuple[str, str]:
    src_path = (COMFY_INPUT_DIR / filename).resolve()
    if not src_path.exists():
        raise FileNotFoundError(f"No such file: {src_path}")
    wav_path = extract_wav(src_path)
    transcript = transcribe_audio_direct(wav_path)
    frames_b64 = extract_video_frames(src_path, max_frames=3)
    participants = extract_participants_vlm(frames_b64)
    return transcript, participants

def generate_mom(route: str, enriched_text: str) -> str:
    user_prompt = f"Составь строгий технический протокол рабочей встречи (MoM) по шаблону ниже. Запрещены вступительные фразы, таблицы и эмодзи. Начинай ответ строго со строки **Протокол рабочей встречи (MoM)**.\n\n{SYSTEM_PROMPT_MOM}\n\n=== ИСХОДНЫЕ ДАННЫЕ ВСТРЕЧИ ===\n{enriched_text}"
    if route == "nda":
        resp = requests.post(  # nosemgrep: python.lang.security.audit.insecure-transport.requests.request-with-http.request-with-http
            f"{OLLAMA_API_BASE}/api/chat",
            json={"model": OLLAMA_MODEL, "stream": False, "think": False, "keep_alive": "2m", "options": {"num_ctx": 32768, "num_predict": 4096, "temperature": 0.1}, "messages": [{"role": "system", "content": SYSTEM_PROMPT_MOM}, {"role": "user", "content": user_prompt}]},
            timeout=600,
        )
        resp.raise_for_status()
        raw = resp.json().get("message", {}).get("content", "")
    else:
        client = OpenAI(http_client=httpx.Client(proxy=OPENROUTER_PROXY_URL, timeout=120.0) if OPENROUTER_PROXY_URL else None, base_url=OPENROUTER_BASE_URL, api_key=OPENROUTER_API_KEY)
        candidates = ["nvidia/nemotron-3-ultra-550b-a55b:free", "inclusionai/ling-3.2-flash:free", "google/gemma-4-26b-a4b-it:free", "baidu/ernie-5.5-flash-preview:free", "nvidia/nemotron-3.5-super-vl:free", "google/gemma-4-31b-it:free"]
        last_err, raw = None, ""
        for m in candidates:
            try:
                res = client.chat.completions.create(model=m, temperature=0.1, messages=[{"role": "system", "content": SYSTEM_PROMPT_MOM}, {"role": "user", "content": user_prompt}])
                raw = res.choices.pop(0).message.content or ""
                if raw.strip():
                    break
            except Exception as e:
                last_err = e
        if not raw.strip():
            raise RuntimeError(f"Все облачные модели перегружены: {last_err}")
    return re.sub(r"(?s)<think>.*?</think>\s*", "", raw).strip()
def load_base_rules() -> str:
    if WORKFLOW_PATH.exists():
        try:
            with WORKFLOW_PATH.open("r", encoding="utf-8") as f:
                return json.load(f).get("37", {}).get("inputs", {}).get("value", "")
        except Exception as e:
            print(f"Не удалось загрузить базовые правила: {e}")
    return ""

def main():
    c = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": KAFKA_GROUP, "auto.offset.reset": "earliest", "enable.auto.commit": False, "max.poll.interval.ms": 1800000})
    c.subscribe([KAFKA_TOPIC])
    print("Direct Python Worker запущен (без ComfyUI, Whisper CUDA + Ollama VLM/LLM)...")
    base_rules = load_base_rules()

    while True:
        msg = c.poll(1.0)
        if msg is None or msg.error():
            continue
        task = json.loads(msg.value().decode("utf-8"))
        tid, route, fname = task["task_id"], task["route"], task["filename"]
        fhash, ctx = task.get("file_hash", ""), task.get("meeting_context", "")
        print(f"\nЗадача [{tid}] | Контур: {route.upper()} | Hash: {fhash[:10]}")

        try:
            cache_key = f"asr_cache:{fhash}" if fhash else ""
            cached = rdb.hgetall(cache_key) if cache_key else {}
            force_recompute = bool(task.get("force_recompute", False))

            if cached and cached.get("transcript") and not force_recompute:
                transcript, participants, cache_hit = cached["transcript"], cached.get("participants", ""), "1"
                print(f"HIT кеша Redis для {fhash[:10]} — пропускаем Whisper и VLM!")
            else:
                cache_hit = "0"
                rdb.hset(f"task:{tid}", mapping={"status": "processing", "stage": "Транскрибация Whisper (GPU) и распознавание участников"})
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
