from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # DSN мониторируемой БД (Supabase/Postgres)
    DATABASE_URL: str

    # DSN хранилища метрик (локально — SQLite)
    MONITOR_DB_URL: str = "sqlite:///monitor.db"

    # Схема PostgreSQL для мониторинга
    MONITORED_SCHEMA: str = "public"

    # Секрет для Flask-сессий/CSRF
    SECRET_KEY: str = "dev-secret"

    # Интервал сбора метрик (минуты)
    COLLECT_INTERVAL_MINUTES: int = 15

    # Уровень логирования
    LOG_LEVEL: str = "INFO"

    # Режим Flask
    FLASK_ENV: str = "development"

    # NVIDIA NIM LLM
    NIM_API_KEY: str = ""
    NIM_BASE_URL: str = "https://integrate.api.nvidia.com/v1"
    NIM_MODEL: str = "meta/llama-3.3-70b-instruct"

    # Telegram Bot alerts
    TELEGRAM_BOT_TOKEN: str = ""
    TELEGRAM_CHAT_ID: str = ""
    TELEGRAM_THROTTLE_MINUTES: int = 30

    # SMTP for password-reset emails (#133). Empty SMTP_HOST → backend
    # falls back to ``memory`` which captures sent messages in an in-process
    # outbox; useful for tests and local dev without an SMTP server.
    SMTP_HOST: str = ""
    SMTP_PORT: int = 587
    SMTP_USER: str = ""
    SMTP_PASSWORD: str = ""
    SMTP_FROM: str = "no-reply@dbmonitor.local"
    SMTP_USE_TLS: bool = True

    # Absolute base URL the app is served from — used to build links in
    # outgoing emails (reset-password link, future invite links, etc).
    # Reading request.host inside the route would pick up internal
    # hostnames behind a reverse proxy / forwarded headers, which is
    # wrong for user-facing links. Set this explicitly per environment.
    APP_BASE_URL: str = "http://localhost:5001"


settings = Settings()
