from mysql_ch_replicator.db_replicator_initial import DbReplicatorInitial
from mysql_ch_replicator.table_structure import TableField, TableStructure


class _DummyState:
    def __init__(self, original_structure):
        self.initial_replication_structure_signatures = {}
        self.tables_structure = {'security_logs': (original_structure, None)}


class _DummyMysqlApi:
    def __init__(self, create_statement):
        self.create_statement = create_statement

    def get_table_create_statement(self, table_name):
        assert table_name == 'security_logs'
        return self.create_statement


class _DummyConverter:
    def __init__(self, parsed_structure):
        self.parsed_structure = parsed_structure

    def parse_mysql_table_structure(self, create_statement, required_table_name=None):
        assert required_table_name == 'security_logs'
        return self.parsed_structure


class _DummyReplicator:
    def __init__(self, state, mysql_api, converter):
        self.state = state
        self.mysql_api = mysql_api
        self.converter = converter


def _build_security_logs_structure():
    structure = TableStructure(
        table_name='security_logs',
        charset='utf8mb4',
        charset_python='utf_8',
        fields=[
            TableField(name='id', field_type='int(10)', parameters='unsigned NOT NULL AUTO_INCREMENT'),
            TableField(name='user_id', field_type='int(11)', parameters='DEFAULT NULL'),
            TableField(name='message', field_type='text', parameters='NOT NULL'),
            TableField(name='category', field_type='varchar(255)', parameters='DEFAULT NULL'),
            TableField(name='created_at', field_type='timestamp', parameters='NULL DEFAULT NULL'),
            TableField(name='updated_at', field_type='timestamp', parameters='NULL DEFAULT NULL'),
        ],
        primary_keys=['id'],
    )
    structure.preprocess()
    return structure


def test_get_structure_signature_ignores_auto_increment_table_option():
    replicator = _DummyReplicator(None, None, None)
    initial = DbReplicatorInitial(replicator)

    statement_1 = (
        'CREATE TABLE `security_logs` (`id` int(10) unsigned NOT NULL AUTO_INCREMENT, '
        'PRIMARY KEY (`id`)) ENGINE=InnoDB AUTO_INCREMENT=101 DEFAULT CHARSET=utf8mb4'
    )
    statement_2 = (
        'CREATE TABLE `security_logs` (`id` int(10) unsigned NOT NULL AUTO_INCREMENT, '
        'PRIMARY KEY (`id`)) ENGINE=InnoDB AUTO_INCREMENT=250000 DEFAULT CHARSET=utf8mb4'
    )

    assert initial.get_structure_signature(statement_1) == initial.get_structure_signature(statement_2)


def test_verify_table_structure_unchanged_does_not_raise_for_equivalent_parsed_structure():
    original_structure = _build_security_logs_structure()
    parsed_current_structure = _build_security_logs_structure()

    create_statement = (
        'CREATE TABLE `security_logs` (`id` int(10) unsigned NOT NULL AUTO_INCREMENT, '
        '`user_id` int(11) DEFAULT NULL, `message` text NOT NULL, `category` varchar(255) DEFAULT NULL, '
        '`created_at` timestamp NULL DEFAULT NULL, `updated_at` timestamp NULL DEFAULT NULL, '
        'PRIMARY KEY (`id`)) ENGINE=InnoDB AUTO_INCREMENT=500 DEFAULT CHARSET=utf8mb4'
    )

    state = _DummyState(original_structure)
    mysql_api = _DummyMysqlApi(create_statement)
    converter = _DummyConverter(parsed_current_structure)
    replicator = _DummyReplicator(state, mysql_api, converter)
    initial = DbReplicatorInitial(replicator)

    # Force signature mismatch so verification reaches the parsed-structure fallback.
    state.initial_replication_structure_signatures['security_logs'] = 'outdated-signature'

    initial.verify_table_structure_unchanged('security_logs')

    assert state.initial_replication_structure_signatures['security_logs'] == initial.get_structure_signature(
        create_statement,
    )
