from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    APP_NAME: str = "Robot Data Pipeline Backend"
    APP_VERSION: str = "1.0.0"
    DATABASE_URL: str = "sqlite:///./robot_data.db"
    API_V1_PREFIX: str = "/api/v1"

    # 持久化发件箱 / 进程内派发器
    OUTBOX_AUTO_DISPATCH: bool = True
    OUTBOX_POLL_INTERVAL: float = 1.0
    OUTBOX_BATCH_SIZE: int = 10
    OUTBOX_MAX_ATTEMPTS: int = 10
    OUTBOX_LOCK_TIMEOUT_SECONDS: int = 300
    OUTBOX_RETRY_BASE_SECONDS: float = 5.0
    OUTBOX_RETRY_FACTOR: float = 2.0
    OUTBOX_RETRY_MAX_SECONDS: float = 3600.0
    OUTBOX_CONSUMER_NAME: str = "analysis-component"

    class Config:
        env_file = ".env"


settings = Settings()
