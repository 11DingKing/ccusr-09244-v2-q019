from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey, Float, Boolean, JSON, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.database import Base


class RobotModel(Base):
    __tablename__ = "robot_models"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), unique=True, nullable=False, index=True)
    manufacturer = Column(String(100), nullable=False)
    description = Column(Text, nullable=True)
    capabilities = Column(JSON, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    operations = relationship("OperationData", back_populates="robot_model")
    datasets = relationship("Dataset", back_populates="robot_model")


class Scene(Base):
    __tablename__ = "scenes"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), unique=True, nullable=False, index=True)
    category = Column(String(50), nullable=False, index=True)
    description = Column(Text, nullable=True)
    environment_tags = Column(JSON, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    operations = relationship("OperationData", back_populates="scene")
    datasets = relationship("Dataset", back_populates="scene")


class Skill(Base):
    __tablename__ = "skills"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), unique=True, nullable=False, index=True)
    category = Column(String(50), nullable=False, index=True)
    description = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    operations = relationship("OperationData", back_populates="skill")


class OperationData(Base):
    __tablename__ = "operation_data"

    id = Column(Integer, primary_key=True, index=True)
    robot_model_id = Column(Integer, ForeignKey("robot_models.id"), nullable=False, index=True)
    scene_id = Column(Integer, ForeignKey("scenes.id"), nullable=False, index=True)
    skill_id = Column(Integer, ForeignKey("skills.id"), nullable=False, index=True)
    robot_serial = Column(String(100), nullable=True, index=True)

    motion_trajectory = Column(JSON, nullable=False)
    perception_records = Column(JSON, nullable=False)
    grasp_result = Column(JSON, nullable=True)

    timestamp_start = Column(DateTime(timezone=True), nullable=False)
    timestamp_end = Column(DateTime(timezone=True), nullable=False)
    duration_ms = Column(Integer, nullable=True)

    environment_conditions = Column(JSON, nullable=True)
    hardware_status = Column(JSON, nullable=True)

    quality_score = Column(Float, nullable=True)
    completeness_score = Column(Float, nullable=True)
    data_grade = Column(String(10), nullable=True, index=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    robot_model = relationship("RobotModel", back_populates="operations")
    scene = relationship("Scene", back_populates="operations")
    skill = relationship("Skill", back_populates="operations")
    annotation = relationship("Annotation", back_populates="operation_data", uselist=False, cascade="all, delete-orphan")
    dataset_items = relationship("DatasetItem", back_populates="operation_data", cascade="all, delete-orphan")


class Annotation(Base):
    __tablename__ = "annotations"

    id = Column(Integer, primary_key=True, index=True)
    operation_data_id = Column(Integer, ForeignKey("operation_data.id"), nullable=False, unique=True, index=True)

    is_success = Column(Boolean, nullable=False, index=True)
    failure_category = Column(String(50), nullable=True, index=True)
    failure_subcategory = Column(String(100), nullable=True)
    failure_description = Column(Text, nullable=True)

    annotator = Column(String(100), nullable=True)
    annotation_time = Column(DateTime(timezone=True), server_default=func.now())
    review_status = Column(String(20), default="pending", index=True)
    reviewer = Column(String(100), nullable=True)
    review_notes = Column(Text, nullable=True)

    annotation_quality_score = Column(Float, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    operation_data = relationship("OperationData", back_populates="annotation")


class Dataset(Base):
    __tablename__ = "datasets"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(200), nullable=False, index=True)
    description = Column(Text, nullable=True)
    version = Column(String(20), default="1.0")

    robot_model_id = Column(Integer, ForeignKey("robot_models.id"), nullable=False, index=True)
    scene_id = Column(Integer, ForeignKey("scenes.id"), nullable=False, index=True)
    skill_id = Column(Integer, ForeignKey("skills.id"), nullable=True, index=True)

    owner_team = Column(String(100), nullable=False)
    contact_person = Column(String(100), nullable=True)
    review_status = Column(String(20), default="draft", index=True)
    is_published = Column(Boolean, default=False, index=True)
    published_at = Column(DateTime(timezone=True), nullable=True)

    current_version = Column(Integer, default=1)

    total_items = Column(Integer, default=0)
    success_count = Column(Integer, default=0)
    failure_count = Column(Integer, default=0)
    annotation_complete_rate = Column(Float, default=0.0)
    average_quality_score = Column(Float, nullable=True)
    reuse_count = Column(Integer, default=0, index=True)

    data_grade = Column(String(10), nullable=True, index=True)
    tags = Column(JSON, nullable=True)
    license_info = Column(String(200), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    robot_model = relationship("RobotModel", back_populates="datasets")
    scene = relationship("Scene", back_populates="datasets")
    items = relationship("DatasetItem", back_populates="dataset", cascade="all, delete-orphan")
    reuse_records = relationship("DatasetReuse", back_populates="dataset", cascade="all, delete-orphan")
    versions = relationship("DatasetVersion", back_populates="dataset", cascade="all, delete-orphan")
    reviews = relationship("DatasetReview", back_populates="dataset", cascade="all, delete-orphan")
    subscriptions = relationship("DatasetSubscription", back_populates="dataset", cascade="all, delete-orphan")


class DatasetItem(Base):
    __tablename__ = "dataset_items"

    id = Column(Integer, primary_key=True, index=True)
    dataset_id = Column(Integer, ForeignKey("datasets.id"), nullable=False, index=True)
    operation_data_id = Column(Integer, ForeignKey("operation_data.id"), nullable=False, index=True)
    added_at = Column(DateTime(timezone=True), server_default=func.now())

    dataset = relationship("Dataset", back_populates="items")
    operation_data = relationship("OperationData", back_populates="dataset_items")


class DatasetReuse(Base):
    __tablename__ = "dataset_reuses"

    id = Column(Integer, primary_key=True, index=True)
    dataset_id = Column(Integer, ForeignKey("datasets.id"), nullable=False, index=True)
    dataset_version_id = Column(Integer, ForeignKey("dataset_versions.id"), nullable=True, index=True)
    reusing_team = Column(String(100), nullable=False)
    purpose = Column(String(200), nullable=True)
    project_name = Column(String(200), nullable=True)
    reuse_date = Column(DateTime(timezone=True), server_default=func.now())
    notes = Column(Text, nullable=True)

    dataset = relationship("Dataset", back_populates="reuse_records")
    version = relationship("DatasetVersion", back_populates="reuse_records")


class DatasetVersion(Base):
    __tablename__ = "dataset_versions"

    id = Column(Integer, primary_key=True, index=True)
    dataset_id = Column(Integer, ForeignKey("datasets.id"), nullable=False, index=True)
    version_number = Column(Integer, nullable=False)
    version_label = Column(String(20), nullable=False)
    change_description = Column(Text, nullable=True)

    total_items = Column(Integer, default=0)
    success_count = Column(Integer, default=0)
    failure_count = Column(Integer, default=0)
    annotation_complete_rate = Column(Float, default=0.0)
    average_quality_score = Column(Float, nullable=True)
    data_grade = Column(String(10), nullable=True)

    created_by = Column(String(100), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    dataset = relationship("Dataset", back_populates="versions")
    reuse_records = relationship("DatasetReuse", back_populates="version")


class DatasetReview(Base):
    __tablename__ = "dataset_reviews"

    id = Column(Integer, primary_key=True, index=True)
    dataset_id = Column(Integer, ForeignKey("datasets.id"), nullable=False, index=True)
    dataset_version_id = Column(Integer, ForeignKey("dataset_versions.id"), nullable=True, index=True)
    action = Column(String(20), nullable=False, index=True)
    reviewer = Column(String(100), nullable=True)
    review_notes = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    dataset = relationship("Dataset", back_populates="reviews")
    version = relationship("DatasetVersion")


class DatasetSubscription(Base):
    __tablename__ = "dataset_subscriptions"

    id = Column(Integer, primary_key=True, index=True)
    dataset_id = Column(Integer, ForeignKey("datasets.id"), nullable=False, index=True)
    subscriber_team = Column(String(100), nullable=False)
    contact_person = Column(String(100), nullable=True)
    notify_on_new_version = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    dataset = relationship("Dataset", back_populates="subscriptions")


class OutboxEvent(Base):
    """持久化发件箱事件：与业务写入在同一事务落库，由进程内派发器异步派发。"""

    __tablename__ = "outbox_events"

    STATUS_PENDING = "pending"
    STATUS_CLAIMED = "claimed"
    STATUS_DELIVERED = "delivered"
    STATUS_QUARANTINED = "quarantined"

    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(String(64), unique=True, nullable=False, index=True)
    event_type = Column(String(100), nullable=False, index=True)
    payload_version = Column(Integer, nullable=False)
    payload = Column(JSON, nullable=False)

    status = Column(String(20), nullable=False, default=STATUS_PENDING, index=True)
    attempts = Column(Integer, nullable=False, default=0)
    max_attempts = Column(Integer, nullable=False, default=5)
    available_at = Column(DateTime(timezone=True), nullable=False, index=True)

    claimed_by = Column(String(100), nullable=True)
    claimed_at = Column(DateTime(timezone=True), nullable=True)
    delivered_at = Column(DateTime(timezone=True), nullable=True)
    last_error = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    attempt_records = relationship("OutboxAttempt", back_populates="event", cascade="all, delete-orphan")


class OutboxAttempt(Base):
    """一次派发尝试的结果记录。"""

    __tablename__ = "outbox_attempts"

    OUTCOME_SUCCESS = "success"
    OUTCOME_FAILURE = "failure"

    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(String(64), ForeignKey("outbox_events.event_id"), nullable=False, index=True)
    attempt_number = Column(Integer, nullable=False)
    outcome = Column(String(20), nullable=False)
    error = Column(Text, nullable=True)
    attempted_at = Column(DateTime(timezone=True), nullable=False)

    event = relationship("OutboxEvent", back_populates="attempt_records")


class ConsumedEvent(Base):
    """消费者确认记录：(event_id, consumer) 唯一，保证重复确认不产生二次影响。"""

    __tablename__ = "consumed_events"
    __table_args__ = (
        UniqueConstraint("event_id", "consumer", name="uq_consumed_event_consumer"),
    )

    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(String(64), nullable=False, index=True)
    consumer = Column(String(100), nullable=False)
    consumed_at = Column(DateTime(timezone=True), nullable=False)


class AnalyticsProjection(Base):
    """内部分析组件的消费投影，用于观察事件副作用是否只生效一次。"""

    __tablename__ = "analytics_projection"

    id = Column(Integer, primary_key=True, index=True)
    metric = Column(String(120), unique=True, nullable=False, index=True)
    value = Column(Integer, nullable=False, default=0)
    updated_at = Column(DateTime(timezone=True), nullable=False)
