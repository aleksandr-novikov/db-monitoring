from pydantic_settings import BaseSettings


class Settings(BaseSettings):
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

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()