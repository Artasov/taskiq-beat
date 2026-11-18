import pytest

from taskiq_beat import SchedulerConfig


def test_environment_overrides_preserve_application_defaults() -> None:
    defaults = SchedulerConfig(scheduler_id="app", run_history_retention_days=30, dispatch_batch_size=12)
    config = SchedulerConfig.from_env(
        prefix="BEAT_",
        environ={
            "BEAT_ID": "replica",
            "BEAT_DISPATCH_BATCH_SIZE": "",
            "BEAT_SYNC_INTERVAL_SECONDS": "0.5",
            "BEAT_RECORD_RUNS": "off",
            "BEAT_RUN_HISTORY_RETENTION_DAYS": "none",
            "BEAT_COMPLETED_JOB_RETENTION_DAYS": "7",
        },
        defaults=defaults,
    )
    assert config.scheduler_id == "replica"
    assert config.dispatch_batch_size == 12
    assert config.sync_interval_seconds == 0.5
    assert config.record_runs is False
    assert config.run_history_retention_days is None
    assert config.completed_job_retention_days == 7
    assert defaults.run_history_retention_days == 30


@pytest.mark.parametrize("key,value", [("RECORD_RUNS", "offf"), ("DISPATCH_BATCH_SIZE", "many")])
def test_invalid_environment_override_names_the_variable(key: str, value: str) -> None:
    with pytest.raises(ValueError, match=f"TASKIQ_SCHEDULER_{key}"):
        SchedulerConfig.from_env(environ={f"TASKIQ_SCHEDULER_{key}": value})
