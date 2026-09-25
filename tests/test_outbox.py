"""持久化发件箱核心行为测试。

覆盖需求要求的关键场景：提交回滚、并发领取、确认丢失、
重试恢复（可注入时钟）、死信隔离与服务重启。
"""

from datetime import timedelta

import pytest

from app.events.consumer import AnalysisProjectionHandler
from app.events.contract import (
    EVENT_ANNOTATION_APPROVED,
    EVENT_DATASET_PUBLISHED,
    EVENT_DATASET_VERSION_REVOKED,
)
from app.events.dispatcher import (
    ATTEMPT_FAILED,
    ATTEMPT_RECLAIMED,
    ATTEMPT_SUCCEEDED,
    STATUS_DEAD_LETTER,
    STATUS_DELIVERED,
    STATUS_PENDING,
    STATUS_PROCESSING,
    EventDispatcher,
    FixedClock,
    compute_next_retry_at,
)
from app.events.outbox import record_event
from app.models import (
    AnalysisEventLog,
    InboxEventConfirmation,
    OutboxDeliveryAttempt,
    OutboxEvent,
    RobotModel,
)
from app.services import outbox_admin

CONSUMER = "analysis-component"
PUBLISHED_FIELDS = dict(
    dataset_id=1,
    dataset_name="数据集甲",
    version_label="1.0",
    version_number=1,
    robot_model_id=2,
    scene_id=3,
    owner_team="感知团队",
    total_items=10,
    success_count=8,
    failure_count=2,
    annotation_complete_rate=0.9,
    published_at="2026-01-01T00:00:00+00:00",
)


def make_dispatcher(session_factory, clock, handler=None, **kwargs):
    handler = handler or AnalysisProjectionHandler(session_factory, consumer=CONSUMER)
    return EventDispatcher(
        session_factory,
        {
            EVENT_DATASET_PUBLISHED: handler,
            EVENT_DATASET_VERSION_REVOKED: handler,
            EVENT_ANNOTATION_APPROVED: handler,
        },
        clock=clock,
        consumer_name=CONSUMER,
        **kwargs,
    )


def seed_event(db, event_type=EVENT_DATASET_PUBLISHED, aggregate_id=1, now=None, **overrides):
    fields = dict(PUBLISHED_FIELDS)
    fields.update(overrides)
    if event_type == EVENT_DATASET_VERSION_REVOKED:
        fields = dict(
            dataset_id=1,
            dataset_name="数据集甲",
            version_label="1.0",
            version_number=1,
            revoked_at="2026-01-02T00:00:00+00:00",
        )
        fields.update(overrides)
    elif event_type == EVENT_ANNOTATION_APPROVED:
        fields = dict(
            annotation_id=1,
            operation_data_id=10,
            robot_model_id=2,
            scene_id=3,
            skill_id=4,
            is_success=False,
            failure_category="感知异常",
            approved_at="2026-01-02T00:00:00+00:00",
        )
        fields.update(overrides)
    return record_event(
        db,
        event_type,
        aggregate_type="dataset",
        aggregate_id=aggregate_id,
        now=now,
        **fields,
    )


# ---------- 事务原子性：提交/回滚 ----------

def test_commit_persists_business_row_and_event(db):
    event = seed_event(db)
    db.add(RobotModel(name="机型X", manufacturer="厂"))
    db.commit()

    assert db.query(OutboxEvent).count() == 1
    assert db.query(RobotModel).filter(RobotModel.name == "机型X").count() == 1
    assert event.status == STATUS_PENDING
    assert event.attempt_count == 0
    assert event.payload_version == "1.0"


def test_rollback_removes_both_event_and_business_write(db):
    """事务回滚：业务数据与事件必须一起消失，杜绝“状态已变事件缺失”的反向隐患。"""
    seed_event(db)
    db.add(RobotModel(name="机型回滚", manufacturer="厂"))
    db.flush()
    assert db.query(OutboxEvent).count() == 1

    db.rollback()

    assert db.query(OutboxEvent).count() == 0
    assert db.query(RobotModel).filter(RobotModel.name == "机型回滚").count() == 0


# ---------- 顺序领取与正常投递 ----------

def test_claim_is_fifo_and_delivers_projection(db, session_factory):
    clock = FixedClock()
    for i in range(3):
        seed_event(db, aggregate_id=i + 1, now=clock.now())
    db.commit()

    dispatcher = make_dispatcher(session_factory, clock)
    first_batch = dispatcher.claim_due(limit=2)
    assert [e.id for e in first_batch] == [1, 2]
    first_summary = dispatcher.deliver(first_batch)
    assert first_summary.succeeded == 2

    summary = dispatcher.process_batch()  # 领取并投递剩余的 3 号
    assert summary.succeeded == 1

    rows = (
        db.query(OutboxEvent)
        .filter(OutboxEvent.status == STATUS_DELIVERED)
        .order_by(OutboxEvent.id)
        .all()
    )
    assert [r.id for r in rows] == [1, 2, 3]
    assert db.query(AnalysisEventLog).count() == 3
    succeeded = (
        db.query(OutboxDeliveryAttempt)
        .filter(OutboxDeliveryAttempt.status == ATTEMPT_SUCCEEDED)
        .count()
    )
    assert succeeded == 3


# ---------- 并发领取 ----------

def test_concurrent_claimers_never_duplicate_an_event(session_factory):
    import threading

    clock = FixedClock()
    total = 60
    setup = session_factory()
    for i in range(total):
        seed_event(setup, aggregate_id=i + 1, now=clock.now())
    setup.commit()
    setup.close()

    claimed_by_workers = []
    lock = threading.Lock()
    barrier = threading.Barrier(8)

    def worker():
        dispatcher = make_dispatcher(session_factory, clock, batch_size=total)
        # 包装 handler 记录实际领取到的事件
        original = AnalysisProjectionHandler(session_factory, consumer=CONSUMER)

        class RecordingHandler:
            def handle(self_inner, event):
                with lock:
                    claimed_by_workers.append(event.id)
                original.handle(event)

        dispatcher._handlers[EVENT_DATASET_PUBLISHED] = RecordingHandler()
        barrier.wait()
        for _ in range(20):
            summary = dispatcher.process_batch()
            if summary.claimed == 0:
                break

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    verify = session_factory()
    try:
        assert len(claimed_by_workers) == total
        assert len(set(claimed_by_workers)) == total
        assert verify.query(AnalysisEventLog).count() == total
        assert verify.query(InboxEventConfirmation).count() == total
        stuck = (
            verify.query(OutboxEvent)
            .filter(OutboxEvent.status.in_([STATUS_PENDING, STATUS_PROCESSING]))
            .count()
        )
        assert stuck == 0
        assert (
            verify.query(OutboxEvent)
            .filter(OutboxEvent.status == STATUS_DELIVERED)
            .count()
            == total
        )
    finally:
        verify.close()


# ---------- 确认丢失 / 重复确认幂等 ----------

def test_ack_loss_after_restart_is_idempotent(session_factory):
    clock = FixedClock()
    db = session_factory()
    seed_event(db, now=clock.now())
    db.commit()

    # 第一个进程：领取、消费者副作用已落地（inbox+投影已提交），
    # 但在记录 delivered 确认之前崩溃
    dispatcher_a = make_dispatcher(
        session_factory, clock, lock_timeout=timedelta(minutes=5)
    )
    claimed = dispatcher_a.claim_due()
    assert len(claimed) == 1
    event = claimed[0]
    AnalysisProjectionHandler(session_factory, consumer=CONSUMER).handle(event)
    # 故意不调用 _record_success —— 模拟进程退出导致确认丢失

    assert db.query(AnalysisEventLog).count() == 1
    row = db.get(OutboxEvent, event.id)
    assert row.status == STATUS_PROCESSING

    # 锁未超时前不能被重复领取
    assert dispatcher_a.claim_due() == []

    # 服务重启：全新派发器对象，时钟推进超过锁超时
    clock.advance(timedelta(minutes=6))
    dispatcher_b = make_dispatcher(
        session_factory, clock, lock_timeout=timedelta(minutes=5)
    )
    summary = dispatcher_b.process_batch()
    assert summary.claimed == 1
    assert summary.succeeded == 1

    db.expire_all()
    row = db.get(OutboxEvent, event.id)
    assert row.status == STATUS_DELIVERED
    assert row.attempt_count == 1
    # 关键断言：重投没有产生二次副作用
    assert db.query(AnalysisEventLog).count() == 1
    assert db.query(InboxEventConfirmation).count() == 1

    statuses = [
        a.status
        for a in db.query(OutboxDeliveryAttempt)
        .filter(OutboxDeliveryAttempt.event_id == event.id)
        .order_by(OutboxDeliveryAttempt.id)
        .all()
    ]
    assert ATTEMPT_RECLAIMED in statuses
    assert statuses[-1] == ATTEMPT_SUCCEEDED


def test_explicit_duplicate_confirmation_has_no_double_effect(session_factory):
    """消费者对同一事件重复确认两次，投影仍只生效一次。"""
    clock = FixedClock()
    db = session_factory()
    event = seed_event(db, now=clock.now())
    db.commit()

    dispatcher = make_dispatcher(session_factory, clock)
    dispatcher.process_batch()

    # 再次手动投递同一事件（绕过领取），处理器必须幂等
    claimed = {e.id: e for e in dispatcher.claim_due()}
    assert claimed == {}
    # 直接复用已投递事件再调用一次处理器
    row = db.get(OutboxEvent, event.id)
    from app.events.dispatcher import DispatchedEvent

    duplicate = DispatchedEvent(
        id=row.id,
        event_type=row.event_type,
        aggregate_type=row.aggregate_type,
        aggregate_id=row.aggregate_id,
        payload_version=row.payload_version,
        payload=row.payload,
        attempt_number=99,
    )
    handler = AnalysisProjectionHandler(session_factory, consumer=CONSUMER)
    handler.handle(duplicate)
    handler.handle(duplicate)

    assert db.query(AnalysisEventLog).filter(AnalysisEventLog.event_id == row.id).count() == 1


# ---------- 重试恢复（可注入时钟 + 退避） ----------

class FlakyHandler:
    """前 fail_times 次抛错，之后走标准幂等投影。"""

    def __init__(self, session_factory, fail_times):
        self._session_factory = session_factory
        self._fail_times = fail_times
        self.calls = 0

    def handle(self, event):
        self.calls += 1
        if self.calls <= self._fail_times:
            raise RuntimeError(f"模拟下游故障 #{self.calls}")
        AnalysisProjectionHandler(self._session_factory, consumer=CONSUMER).handle(event)


def test_backoff_schedule_uses_injected_clock():
    start = FixedClock().now()
    first = compute_next_retry_at(start, 1, 5.0, 2.0, 3600.0)
    second = compute_next_retry_at(start, 2, 5.0, 2.0, 3600.0)
    assert first == start + timedelta(seconds=5)
    assert second == start + timedelta(seconds=10)
    capped = compute_next_retry_at(start, 20, 5.0, 2.0, 60.0)
    assert capped == start + timedelta(seconds=60)


def test_retry_then_recovery(session_factory):
    clock = FixedClock()
    db = session_factory()
    seed_event(db, now=clock.now())
    db.commit()

    handler = FlakyHandler(session_factory, fail_times=2)
    dispatcher = make_dispatcher(
        session_factory, clock, handler=handler, base_retry_delay=5.0,
        backoff_factor=2.0, max_retry_delay=3600.0,
    )

    # 第一次：失败，5 秒后才可再领
    s1 = dispatcher.process_batch()
    assert s1.failed == 1 and s1.retried == 1
    assert dispatcher.claim_due() == []

    clock.advance(timedelta(seconds=4))
    assert dispatcher.claim_due() == []
    clock.advance(timedelta(seconds=1))

    # 第二次：仍失败，下一次退避到 10 秒后
    s2 = dispatcher.process_batch()
    assert s2.failed == 1
    row = db.get(OutboxEvent, 1)
    assert row.status == STATUS_PENDING
    assert row.attempt_count == 2
    assert row.available_at == clock.now() + timedelta(seconds=10)

    clock.advance(timedelta(seconds=10))
    s3 = dispatcher.process_batch()
    assert s3.succeeded == 1

    db.expire_all()
    row = db.get(OutboxEvent, 1)
    assert row.status == STATUS_DELIVERED
    assert row.attempt_count == 3
    assert row.last_error is None
    assert db.query(AnalysisEventLog).count() == 1

    attempts = (
        db.query(OutboxDeliveryAttempt)
        .filter(OutboxDeliveryAttempt.event_id == 1)
        .order_by(OutboxDeliveryAttempt.id)
        .all()
    )
    assert [a.status for a in attempts] == [ATTEMPT_FAILED, ATTEMPT_FAILED, ATTEMPT_SUCCEEDED]
    assert "模拟下游故障" in attempts[0].error


# ---------- 死信隔离、查询与重投 ----------

class AlwaysFailHandler:
    def __init__(self):
        self.calls = 0

    def handle(self, event):
        self.calls += 1
        raise RuntimeError("永久性故障")


def test_exhausting_attempts_enters_queryable_dead_letter(session_factory):
    """超过上限即隔离，且推进时钟也不会再被领取。"""
    clock = FixedClock()
    db = session_factory()
    seed_event(db, now=clock.now(), max_attempts=2)
    db.commit()

    handler = AlwaysFailHandler()
    dispatcher = make_dispatcher(
        session_factory, clock, handler=handler, base_retry_delay=5.0,
    )

    dispatcher.process_batch()  # 第 1 次失败 -> 回到 pending
    clock.advance(timedelta(seconds=5))
    summary = dispatcher.process_batch()  # 第 2 次失败 -> 死信
    assert summary.failed == 1
    assert summary.dead_lettered == 1

    row = db.get(OutboxEvent, 1)
    assert row.status == STATUS_DEAD_LETTER
    assert row.attempt_count == 2
    assert row.dead_letter_at is not None
    assert "永久性故障" in (row.dead_letter_reason or "")

    # 隔离后即使远超退避时间也不再被领取
    clock.advance(timedelta(hours=24))
    assert dispatcher.claim_due() == []

    # 可按状态查询
    found = outbox_admin.list_events(db, status=STATUS_DEAD_LETTER)
    assert [e.id for e in found] == [1]
    assert len(outbox_admin.list_attempts(db, 1)) == 2


def test_dead_letter_isolation_and_replay(session_factory):
    clock = FixedClock()
    db = session_factory()
    seed_event(db, now=clock.now(), max_attempts=2)
    db.commit()

    fail_handler = AlwaysFailHandler()
    dispatcher = make_dispatcher(
        session_factory, clock, handler=fail_handler, base_retry_delay=5.0,
    )

    dispatcher.process_batch()
    clock.advance(timedelta(seconds=5))
    summary = dispatcher.process_batch()
    assert summary.dead_lettered == 1

    # 修复故障后重投并清零预算，投递成功
    good = make_dispatcher(session_factory, clock)
    replayed = outbox_admin.replay_dead_letter(
        db, 1, now=clock.now(), reset_attempts=True
    )
    assert replayed.status == STATUS_PENDING
    assert replayed.attempt_count == 0
    summary = good.process_batch()
    assert summary.succeeded == 1
    db.expire_all()
    assert db.get(OutboxEvent, 1).status == STATUS_DELIVERED
    assert db.query(AnalysisEventLog).count() == 1


def test_replay_rejects_live_event(session_factory):
    clock = FixedClock()
    db = session_factory()
    seed_event(db, now=clock.now())
    db.commit()
    with pytest.raises(ValueError):
        outbox_admin.replay_dead_letter(db, 1)


# ---------- 服务重启 ----------

def test_restart_process_continues_undispatched_events(session_factory):
    clock = FixedClock()
    db = session_factory()
    for i in range(3):
        seed_event(db, aggregate_id=i + 1, now=clock.now())
    db.commit()
    db.close()

    # 旧进程只投递了第一条就退出
    old = make_dispatcher(session_factory, clock)
    old.process_batch(limit=1)

    # 新进程启动，状态全部来自数据库，继续投递剩余事件
    new = make_dispatcher(session_factory, clock)
    summary = new.process_batch()
    assert summary.succeeded == 2

    check = session_factory()
    try:
        assert (
            check.query(OutboxEvent).filter(OutboxEvent.status == STATUS_DELIVERED).count()
            == 3
        )
        assert check.query(AnalysisEventLog).count() == 3
    finally:
        check.close()
