import pytest
from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError

from mysql_ch_replicator import clickhouse_api


def _build_api(monkeypatch, *, max_unfinished_mutations_to_wait=900):
    api = clickhouse_api.ClickhouseApi.__new__(clickhouse_api.ClickhouseApi)
    api.database = 'test_db'
    api.erase_batch_size = 100
    api.max_unfinished_mutations_to_wait = max_unfinished_mutations_to_wait
    api.mutation_backpressure_sleep = 10
    api.stats = clickhouse_api.GeneralStats()
    api.get_on_cluster_clause = lambda: ''

    sleeps = []
    monkeypatch.setattr(clickhouse_api.time, 'sleep', lambda seconds: sleeps.append(seconds))
    return api, sleeps


def test_erase_waits_for_mutation_backlog(monkeypatch):
    api, sleeps = _build_api(monkeypatch, max_unfinished_mutations_to_wait=2)
    unfinished_mutations = iter([2, 1])
    commands = []

    api._get_unfinished_mutations_count = lambda table_name: next(unfinished_mutations)
    api.execute_command = lambda query: commands.append(query)

    api.erase('test_table', ['id'], {'1'})

    assert sleeps == [10]
    assert commands == [
        '\nDELETE FROM `test_db`.`test_table`  WHERE (id) IN ((1))\n'
    ]
    assert api.stats.general.erases.events == 1
    assert api.stats.general.erases.records == 1


def test_erase_retries_too_many_mutations_error(monkeypatch):
    api, sleeps = _build_api(monkeypatch, max_unfinished_mutations_to_wait=0)
    commands = []

    def execute_command(query):
        commands.append(query)
        if len(commands) == 1:
            raise DatabaseError(
                'Code: 692. DB::Exception: Too many unfinished mutations (1000). '
                '(TOO_MANY_MUTATIONS)'
            )

    api.execute_command = execute_command

    api.erase('test_table', ['id'], {'1'})

    assert sleeps == [10]
    assert len(commands) == 2
    assert api.stats.general.erases.events == 1
    assert api.stats.general.erases.records == 1


def test_erase_retries_session_locked_error(monkeypatch):
    api, sleeps = _build_api(monkeypatch, max_unfinished_mutations_to_wait=0)
    commands = []

    def execute_command(query):
        commands.append(query)
        if len(commands) == 1:
            raise DatabaseError(
                'Code: 373. DB::Exception: Session abc is locked by a concurrent client. '
                '(SESSION_IS_LOCKED)'
            )

    api.execute_command = execute_command

    api.erase('test_table', ['id'], {'1'})

    assert sleeps == [10]
    assert len(commands) == 2
    assert api.stats.general.erases.events == 1
    assert api.stats.general.erases.records == 1


def test_erase_retries_operational_error(monkeypatch):
    api, sleeps = _build_api(monkeypatch, max_unfinished_mutations_to_wait=0)
    commands = []

    def execute_command(query):
        commands.append(query)
        if len(commands) == 1:
            raise OperationalError(
                'Error HTTPConnectionPool(host=\'localhost\', port=8123): '
                'Read timed out. (read timeout=120) executing HTTP request attempt 1'
            )

    api.execute_command = execute_command

    api.erase('test_table', ['id'], {'1'})

    assert sleeps == [10]
    assert len(commands) == 2
    assert api.stats.general.erases.events == 1
    assert api.stats.general.erases.records == 1


def test_erase_does_not_retry_other_database_errors(monkeypatch):
    api, sleeps = _build_api(monkeypatch, max_unfinished_mutations_to_wait=0)

    def execute_command(query):
        raise DatabaseError('Code: 60. DB::Exception: Table does not exist')

    api.execute_command = execute_command

    with pytest.raises(DatabaseError, match='Table does not exist'):
        api.erase('test_table', ['id'], {'1'})

    assert sleeps == []
    assert api.stats.general.erases.events == 0
