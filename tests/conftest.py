"""pytest 公共夹具。

所有测试使用独立的文件型 SQLite（与生产同构），并在导入应用
配置前通过环境变量指向临时库、关闭后台派发线程；并发测试依赖
WAL 与 busy_timeout 减少多线程写锁碰撞。
"""

import os
import tempfile

_TMP_DB = os.path.join(tempfile.gettempdir(), "robot_data_outbox_pytest.db")
os.environ["DATABASE_URL"] = f"sqlite:///{_TMP_DB}"
os.environ["OUTBOX_AUTO_DISPATCH"] = "false"

import pytest  # noqa: E402
from sqlalchemy import create_engine, event  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402

from app.database import Base  # noqa: E402
import app.models  # noqa: E402,F401  确保全部模型已注册


@pytest.fixture()
def engine():
    eng = create_engine(
        f"sqlite:///{_TMP_DB}",
        connect_args={"check_same_thread": False, "timeout": 30},
    )

    @event.listens_for(eng, "connect")
    def _set_sqlite_pragma(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA busy_timeout=30000")
        cursor.close()

    Base.metadata.drop_all(bind=eng)
    Base.metadata.create_all(bind=eng)
    yield eng
    eng.dispose()


@pytest.fixture()
def session_factory(engine):
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)


@pytest.fixture()
def db(session_factory):
    session = session_factory()
    try:
        yield session
    finally:
        session.close()
