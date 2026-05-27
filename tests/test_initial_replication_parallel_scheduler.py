from collections import Counter, defaultdict
import hashlib
from types import SimpleNamespace

from mysql_ch_replicator import db_replicator_initial
from mysql_ch_replicator.db_replicator_initial import DbReplicatorInitial


class _ImmediateProcess:
    returncode = 0

    def poll(self):
        return 0

    def terminate(self):
        pass


class _DummyState:
    def __init__(self):
        self.initial_replication_completed_tables = []
        self.initial_replication_parallel_worker_counts = {}
        self.initial_replication_parallel_completed_workers = {}
        self.initial_replication_table_checkpoints = {}
        self.initial_replication_max_primary_key = None
        self.initial_replication_table = None
        self.initial_replication_row_estimates = {}
        self.tables_last_record_version = {}

    def save(self):
        pass


class _DummyClickHouseApi:
    def __init__(self):
        self.tables_last_record_version = {}

    def get_max_record_version(self, table_name):
        return 0


def _make_initial_replicator(threads, tmp_path):
    state = _DummyState()
    data_dir = tmp_path / 'binlog'
    db_dir = data_dir / 'source_db'
    db_dir.mkdir(parents=True)
    replicator = SimpleNamespace(
        config=SimpleNamespace(
            initial_replication_threads=threads,
            binlog_replicator=SimpleNamespace(data_dir=str(data_dir)),
        ),
        settings_file='config.yaml',
        database='source_db',
        target_database_tmp='target_db_tmp',
        state=state,
        clickhouse_api=_DummyClickHouseApi(),
        get_target_table_name=lambda table_name: table_name,
    )
    return DbReplicatorInitial(replicator), state, db_dir


def _capture_worker_commands(monkeypatch):
    commands = []

    def fake_popen(cmd):
        commands.append(cmd)
        return _ImmediateProcess()

    monkeypatch.setattr(db_replicator_initial.subprocess, 'Popen', fake_popen)
    return commands


def _value_after(cmd, flag):
    return cmd[cmd.index(flag) + 1]


def _create_worker_state_file(db_dir, table_name, worker_id, total_workers):
    table_hash = hashlib.sha256(table_name.encode('utf-8')).hexdigest()[:16]
    state_path = db_dir / f'state_worker_{worker_id}_of_{total_workers}_{table_hash}.pckl'
    state_path.write_bytes(b'checkpoint')


def test_parallel_scheduler_uses_one_worker_per_table_when_tables_fill_pool(monkeypatch, tmp_path):
    initial, state, _ = _make_initial_replicator(threads=4, tmp_path=tmp_path)
    commands = _capture_worker_commands(monkeypatch)

    table_names = ['table_1', 'table_2', 'table_3', 'table_4', 'table_5']
    initial.perform_initial_replication_table_parallel(table_names)

    tables = [_value_after(cmd, '--table') for cmd in commands]
    assert Counter(tables) == Counter({table_name: 1 for table_name in table_names})
    assert {_value_after(cmd, '--total_workers') for cmd in commands} == {'1'}
    assert set(state.initial_replication_completed_tables) == set(table_names)


def test_parallel_scheduler_splits_tables_only_when_pool_has_spare_workers(monkeypatch, tmp_path):
    initial, state, _ = _make_initial_replicator(threads=4, tmp_path=tmp_path)
    commands = _capture_worker_commands(monkeypatch)

    table_names = ['table_1', 'table_2']
    initial.perform_initial_replication_table_parallel(table_names)

    worker_ids_by_table = defaultdict(set)
    total_workers_by_table = defaultdict(set)
    for cmd in commands:
        table_name = _value_after(cmd, '--table')
        worker_ids_by_table[table_name].add(_value_after(cmd, '--worker_id'))
        total_workers_by_table[table_name].add(_value_after(cmd, '--total_workers'))

    assert worker_ids_by_table == {
        'table_1': {'0', '1'},
        'table_2': {'0', '1'},
    }
    assert total_workers_by_table == {
        'table_1': {'2'},
        'table_2': {'2'},
    }
    assert set(state.initial_replication_completed_tables) == set(table_names)


def test_parallel_scheduler_keeps_existing_worker_count_for_interrupted_table(monkeypatch, tmp_path):
    initial, state, db_dir = _make_initial_replicator(threads=4, tmp_path=tmp_path)
    commands = _capture_worker_commands(monkeypatch)
    _create_worker_state_file(db_dir, 'table_2', worker_id=2, total_workers=4)

    table_names = ['table_1', 'table_2', 'table_3', 'table_4', 'table_5']
    initial.perform_initial_replication_table_parallel(table_names)

    worker_ids_by_table = defaultdict(set)
    total_workers_by_table = defaultdict(set)
    for cmd in commands:
        table_name = _value_after(cmd, '--table')
        worker_ids_by_table[table_name].add(_value_after(cmd, '--worker_id'))
        total_workers_by_table[table_name].add(_value_after(cmd, '--total_workers'))

    assert worker_ids_by_table['table_2'] == {'0', '1', '2', '3'}
    assert total_workers_by_table['table_2'] == {'4'}
    for table_name in {'table_1', 'table_3', 'table_4', 'table_5'}:
        assert worker_ids_by_table[table_name] == {'0'}
        assert total_workers_by_table[table_name] == {'1'}
    assert set(state.initial_replication_completed_tables) == set(table_names)


def test_parallel_scheduler_skips_parent_recorded_completed_workers(monkeypatch, tmp_path):
    initial, state, _ = _make_initial_replicator(threads=4, tmp_path=tmp_path)
    commands = _capture_worker_commands(monkeypatch)
    state.initial_replication_parallel_worker_counts['table_1'] = 4
    state.initial_replication_parallel_completed_workers['table_1'] = [0, 2]

    initial.perform_initial_replication_table_parallel(['table_1'])

    assert [_value_after(cmd, '--worker_id') for cmd in commands] == ['1', '3']
    assert {_value_after(cmd, '--total_workers') for cmd in commands} == {'4'}
    assert state.initial_replication_completed_tables == ['table_1']
    assert state.initial_replication_parallel_worker_counts == {}
    assert state.initial_replication_parallel_completed_workers == {}
