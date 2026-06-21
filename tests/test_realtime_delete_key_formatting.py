from mysql_ch_replicator.db_replicator_realtime import DbReplicatorRealtime
from mysql_ch_replicator.table_structure import TableField, TableStructure


def _structure(fields, primary_keys):
    structure = TableStructure(fields=fields, primary_keys=primary_keys)
    structure.preprocess()
    return structure


def test_record_id_quotes_clickhouse_enum_primary_key_values():
    realtime = DbReplicatorRealtime.__new__(DbReplicatorRealtime)
    structure = _structure(
        fields=[
            TableField(name='character_id', field_type='UInt64'),
            TableField(name='role', field_type='String'),
            TableField(
                name='scope',
                field_type="Enum8('roles' = 1, 'roles_at_hq' = 2, 'roles_at_base' = 3, 'roles_at_other' = 4)",
            ),
        ],
        primary_keys=['character_id', 'role', 'scope'],
    )

    record_id = realtime._get_record_id(
        structure,
        [2118165833, 'Config_Equipment', 'roles_at_other'],
    )

    assert record_id == "2118165833,'Config_Equipment','roles_at_other'"


def test_record_id_keeps_clickhouse_enum_index_values_numeric():
    realtime = DbReplicatorRealtime.__new__(DbReplicatorRealtime)
    structure = _structure(
        fields=[
            TableField(name='id', field_type='UInt64'),
            TableField(name='scope', field_type="Enum8('roles' = 1, 'roles_at_hq' = 2)"),
        ],
        primary_keys=['id', 'scope'],
    )

    record_id = realtime._get_record_id(structure, [1, 2])

    assert record_id == '1,2'


def test_record_id_quotes_wrapped_string_primary_key_values():
    realtime = DbReplicatorRealtime.__new__(DbReplicatorRealtime)
    structure = _structure(
        fields=[
            TableField(name='id', field_type='UInt64'),
            TableField(name='name', field_type='Nullable(LowCardinality(String))'),
            TableField(name='code', field_type='LowCardinality(FixedString(8))'),
        ],
        primary_keys=['id', 'name', 'code'],
    )

    record_id = realtime._get_record_id(structure, [1, 'hq', 'ABCDEFGH'])

    assert record_id == "1,'hq','ABCDEFGH'"


def test_record_id_escapes_clickhouse_string_primary_key_values():
    realtime = DbReplicatorRealtime.__new__(DbReplicatorRealtime)
    structure = _structure(
        fields=[
            TableField(name='id', field_type='UInt64'),
            TableField(name='scope', field_type='String'),
        ],
        primary_keys=['id', 'scope'],
    )

    record_id = realtime._get_record_id(structure, [1, "role\\owner's"])

    assert record_id == "1,'role\\\\owner\\'s'"
