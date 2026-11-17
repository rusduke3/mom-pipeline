import time
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
CACHE_TTL_SEC = 7 * 24 * 3600  # ?????? ???????????, OCR ? ???????? ?????? ? Redis 7 ????
MAX_RETRIES = 3                # ?? 3 ??????? ??? ????????? ?????
NON_RETRYABLE_ERRORS = ("?????? ???????????", "Invalid data found", "No such file")


def cleanup_media_files(filename: str) -> None:
    """??????? ???????? ? ????????????????? ?????????? ?? ????? ComfyUI ????? ?????? ??????."""
    stem = Path(filename).stem
    for d in {COMFY_INPUT_DIR, *INPUT_DIRS}:
        for f in (d / filename, d / f"{stem}.mp4"):
            try:
            cache_key = f"asr_cache:{fhash}" if fhash else ""
            cached = rdb.hgetall(cache_key) if cache_key else {}

            if cached and cached.get("transcript"):
                transcript = cached["transcript"]
                participants = cached.get("participants", "")
                cache_hit = "1"
                print(f"HIT ???? Redis ??? {fhash[:10]} ? ?????????? Whisper ? OCR!")
            else:
                cache_hit = "0"
                rdb.hset(f"task:{tid}", mapping={"status": "processing", "stage": "????????????? Whisper ? OCR ?????????? (ComfyUI)"})
                transcript, participants = run_asr_and_ocr(fname)
                if not transcript.strip():
                    raise RuntimeError("?????? ??????????? ????? Whisper")
                if cache_key:
                    rdb.hset(cache_key, mapping={"transcript": transcript, "participants": participants})
                    rdb.expire(cache_key, CACHE_TTL_SEC)

            model_label = OLLAMA_MODEL if route == "nda" else OPENROUTER_MODEL
            rdb.hset(f"task:{tid}", mapping={"status": "processing", "stage": f"????????? ????????? ({model_label})"})

            manual_block = f"?????????????? ???????? ?? ????????????:
{ctx}

" if ctx else ""
            enriched = f"{manual_block}{base_rules}
????????? ?? ??????????: {participants}

??????????? ???????:
{transcript}"
            mom = generate_mom(route, enriched)

            rdb.hset(
                f"task:{tid}",
                mapping={
                    "status": "completed",
                    "result": mom,
                    "transcript": transcript,
                    "participants": participants,
                    "stage": "??????",
                    "cache_hit": cache_hit,
                },
            )
            rdb.expire(f"task:{tid}", CACHE_TTL_SEC)
            c.commit(message=msg)
            cleanup_media_files(fname)
            print(f"?????? [{tid}] ?????????! (Cache={cache_hit}, ?????={len(mom)} ????.)")
        except Exception as e:
            err_text = str(e)
            attempt = int(task.get("_attempt", 1))
            is_fatal = any(marker in err_text for marker in NON_RETRYABLE_ERRORS)

            if not is_fatal and attempt < MAX_RETRIES:
                next_attempt = attempt + 1
                wait_sec = attempt * 5
                print(f"????????? ???? [{tid}] (??????? {attempt}/{MAX_RETRIES}): {err_text}. ?????? ????? {wait_sec}?...")
                rdb.hset(f"task:{tid}", mapping={"status": "processing", "stage": f"????????? ??????? {next_attempt}/{MAX_RETRIES} ????? ????..."})
                time.sleep(wait_sec)
                task["_attempt"] = next_attempt
                dlq_producer.produce(KAFKA_TOPIC, key=tid.encode("utf-8"), value=json.dumps(task, ensure_ascii=False).encode("utf-8"))
                dlq_producer.flush()
                c.commit(message=msg)
            else:
                print(f"?????? [{tid}] (??????? {attempt}/{MAX_RETRIES}, ? DLQ): {err_text}")
                rdb.hset(f"task:{tid}", mapping={"status": "failed", "error": err_text})
                rdb.expire(f"task:{tid}", CACHE_TTL_SEC)
                dlq_producer.produce(
                    KAFKA_DLQ_TOPIC,
                    key=tid.encode("utf-8"),
                    value=json.dumps({"task": task, "error": err_text}, ensure_ascii=False).encode("utf-8"),
                )
                dlq_producer.flush()
                c.commit(message=msg)
                cleanup_media_files(fname)
            print(f"✅ Задача [{tid}] выполнена! (Cache={cache_hit}, Длина={len(mom)} симв.)")
        except Exception as e:
            print(f"❌ Ошибка [{tid}]: {e}")
            rdb.hset(f"task:{tid}", mapping={"status": "failed", "error": str(e)})
            dlq_producer.produce(KAFKA_DLQ_TOPIC, key=tid.encode("utf-8"), value=json.dumps({"task": task, "error": str(e)}, ensure_ascii=False).encode("utf-8"))
            dlq_producer.flush()
            c.commit(message=msg)

if __name__ == "__main__":
    main()