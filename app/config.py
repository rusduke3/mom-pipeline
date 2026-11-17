from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env", override=True)
import os, re, json, time, uuid, random, shutil, secrets
from pathlib import Path
from typing import Optional
import redis, requests, websocket
from openai import OpenAI
from confluent_kafka import Consumer, Producer, KafkaError
from fastapi import FastAPI, File, Form, UploadFile, Depends, HTTPException, status
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "mom-tasks"
KAFKA_DLQ_TOPIC = "mom-tasks-dlq"
KAFKA_GROUP = "mom-workers-group"
REDIS_URL = "redis://localhost:6379/0"
COMFY_SERVER = "127.0.0.1:8188"
WORKFLOW_PATH = Path("app/workflow_api.json")
COMFY_INPUT_DIR = Path(r"C:\Users\storm\AppData\Local\Comfy-Desktop\ComfyUI-Installs\ComfyUI\ComfyUI\input")
OLLAMA_BASE_URL = "http://127.0.0.1:11434/v1"
OLLAMA_MODEL = "qwen3.6:35b"
AUTH_USER = os.getenv("WEB_ADMIN_USER", "admin")
AUTH_PASS = os.getenv("WEB_ADMIN_PASSWORD", "")

SYSTEM_PROMPT_MOM = """Ты — опытный технический руководитель проектов (Technical PM). Твоя задача — составить точный, сухой и технически достоверный протокол рабочей встречи (MoM) на русском языке на основе сырой стенограммы и переданного справочника участников.

ПРАВИЛА СТРОГОЙ ДОСТОВЕРНОСТИ:
1. Никогда не выдумывай календарные даты и год, если они не названы в тексте. Если в тексте сказано «завтра», «в понедельник» или «на следующей неделе во вторник» — пиши именно так, не пересчитывая в числа.
2. Определяй роли и принадлежность участников строго по правилам из переданного блока контекста встречи (формат имени, справочник спикеров) и фактам из диалога. Не додумывай принадлежность к компаниям, если для этого нет прямых оснований.
3. Устраняй фонетические искажения автоматического распознавания речи (ASR) по общему контексту IT и телекома. Строго сохраняй технические запреты и ограничения из диалога (что делать нельзя или не требуется).
4. Заменяй разговорные и жаргонные слова из устной речи на нейтральные технические термины.
5. Объединяй дублирующиеся фонетически искажённые имена и фамилии одного и того же участника, не дублируй людей в списке.
6. Игнорируй технические артефакты распознавания. Используй строго кириллицу и латиницу для терминов, любые иероглифы и посторонние символы запрещены.

СТРУКТУРА ДОКУМЕНТА:
- **Тема и участники** (ФИО и функциональная роль в контексте обсуждения);
- **1. Резюме и коммерческие рамки** (цель встречи, бюджет и договорные условия при наличии, целевой скоуп этапа, вынос остаточных работ в punch-list, исключения из периметра);
- **2. Технический план и принятые решения** (поэтапный порядок работ по узлам, правила переключения и отключения, резервное копирование, регламент ночных окон, каналы связи и управление доступами);
- **3. Поставленные задачи (Action Items)** (строго в формате: Ответственный — Описание задачи, начиная с глагола в инфинитиве — Срок из текста; без квадратных скобок и пояснений);
- **4. Открытые вопросы и риски** (технические и организационные риски с указанием влияния и мер митигации).

Оформление списков: внутренние пункты списка заканчивай точкой с запятой, последний пункт — точкой."""

# Настройки облачного контура Public (OpenRouter через официальный OpenAI SDK)
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
OPENROUTER_MODEL = "google/gemma-4-31b-it:free"


OPENROUTER_PROXY_URL = os.getenv('OPENROUTER_PROXY_URL', 'socks5://127.0.0.1:10808')
