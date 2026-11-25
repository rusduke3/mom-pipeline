# AI MoM Enterprise Pipeline

Двухконтурная асинхронная система автоматического протоколирования встреч (MoM) с поддержкой локального **NDA** и облачного **Public** контуров.

```mermaid
flowchart LR
    User([👤 Пользователь / UI]) -->|1. Медиа + Скрин + NDA/Public| GW[⚡ FastAPI Gateway]
    GW -->|2. SHA-256| Redis[(🚀 Redis Cache<br/>TTL 7 дней)]
    Redis -->|HIT| User
    Redis -->|MISS: 202 + task_id| Kafka{{📨 Apache Kafka + DLQ}}

    subgraph Local [🔒 Общий локальный контур]
        direction TB
        Kafka --> Worker[⚙️ Async Worker]
        Worker --> STT[🎙️ Local Whisper Podlodka]
        Worker --> OCR[👁️ OCR участников]
        STT & OCR --> Proto[📋 Общий базовый протокол]
    end

    Proto -.->|Кнопка в UI| Txt[📄 Скачать .TXT]
    Proto -->|3. Политика ИБ| Router{🛡️ Маршрутизатор}

    subgraph NDA [🖥️ Ветка NDA: Local GPU Ollama/Qwen]
        direction TB
        N1[Прогон 1: Стенограмма<br/>внутренний шаг] --> N2[Прогон 2: Финальный MoM<br/>152-ФЗ / 187-ФЗ]
    end

    Router -->|NDA| N1
    Router -->|Public| Cloud[☁️ VPN-контейнер<br/>OpenRouter · Gemma<br/>Сборка MoM в 1 прогон]

    N2 & Cloud --> Out[🖥️ Вывод MoM на экран UI<br/>+ запись в Redis и удаление медиа]
    Out -->|4. Результат| User
```



## Архитектура решения

- **Вход:** Веб-форма / Телефон (HTTPS через Nginx + Let's Encrypt + Fail2ban)
- **Прием задач:** FastAPI Producer -> Redis (статусы и кеш ASR по SHA-256)
- **Очередь:** Apache Kafka (KRaft + DLQ)
- **Обработка:** Direct Python Worker (`app/worker.py`) с локальным GPU-конвейером (CTranslate2/faster-whisper для транскрибации и очистки VRAM, Ollama VLM `qwen2.5vl:7b` для анализа скриншотов ВКС, Ollama `qwen3.6:35b` для формирования MoM)
- **Маршрутизация LLM:** Локальная модель в контуре NDA или облачная через внешний HTTP-прокси (`OPENROUTER_PROXY_URL`) в контуре Public.

## Ключевые возможности и безопасность

- **Прямой GPU-инференс:** работа без GUI-прослоек с выгрузкой весов из VRAM.
- **Двухконтурность (NDA / Public):** локальный инференс или безопасный проход через внешний HTTP-прокси.
- **Отказоустойчивость:** разделение приема и тяжелых задач через Kafka, дедупликация через Redis SHA-256.
- **DevSecOps:** защита от timing-атак, валидация путей, Bandit SAST и pre-commit хуки.

## Оптимизации Спринта 2

- **Кеширование ASR:** повторные задачи с тем же SHA-256 не пересчитываются (TTL 7 суток), `force_recompute` обходит кеш.
- **Очистка VRAM:** `free_vram()` после каждого инференса, ленивая загрузка WhisperModel с кэшированием между задачами.
- **Ручной commit Kafka:** `max.poll.interval.ms` 30 мин, ретраи до 3, фатальные ошибки сразу в DLQ.
- **Ротация free-моделей:** Public-контур через OpenAI SDK → OpenRouter с ротацией бесплатных моделей.

## Быстрый запуск

1. `cp .env.example .env`
2. `docker compose up -d`
3. `uvicorn app.producer:app --host 127.0.0.1 --port 8000` и `python -m app.worker`

© 2026 Сироткин Станислав
