import os
import tempfile

# 必须在导入任何 app 模块之前指向临时数据库，避免在仓库根目录生成 robot_data.db
_TEST_DB_DIR = tempfile.mkdtemp(prefix="robot_data_test_")
os.environ.setdefault("DATABASE_URL", f"sqlite:///{_TEST_DB_DIR}/app.db")

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.database import Base, get_db


@pytest.fixture()
def session_factory(tmp_path):
    """每个测试独立的 SQLite 文件与会话工厂。"""
    engine = create_engine(
        f"sqlite:///{tmp_path}/test.db",
        connect_args={"check_same_thread": False},
    )
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine)
    yield factory
    engine.dispose()


@pytest.fixture()
def db(session_factory):
    session = session_factory()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture()
def client(session_factory):
    """FastAPI 测试客户端：业务接口与发件箱派发都指向测试数据库。"""
    from fastapi.testclient import TestClient

    from main import app

    def override_get_db():
        session = session_factory()
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_db] = override_get_db
    app.state.outbox_session_factory = session_factory
    # 不使用 with 语法，避免触发 lifespan 启动后台派发线程，保证测试确定性
    test_client = TestClient(app)
    yield test_client
    app.dependency_overrides.clear()
