import json
import os.path
import hashlib
import re
import time
import sys
import subprocess
import pickle
from logging import getLogger
from enum import Enum

from .config import Settings
from .mysql_api import MySQLApi
from .clickhouse_api import ClickhouseApi
from .converter import MysqlToClickhouseConverter
from .table_structure import TableStructure
from .utils import touch_all_files
from .common import Status

logger = getLogger(__name__)

class DbReplicatorInitial:
    
    SAVE_STATE_INTERVAL = 10
    BINLOG_TOUCH_INTERVAL = 120
    MYSQL_PARTITION_BY_RE = re.compile(
        r'PARTITION\s+BY\s+(?:LINEAR\s+)?(?:RANGE|LIST|HASH|KEY)\s*(?:COLUMNS\s*)?\((.*?)\)',
        re.IGNORECASE | re.DOTALL,
    )
    MYSQL_PARTITION_NAME_RE = re.compile(
        r'PARTITION\s+`?([A-Za-z0-9_]+)`?\s+(?:VALUES|ENGINE|COMMENT|DATA|INDEX|MAX_ROWS|MIN_ROWS|TABLESPACE|NODEGROUP|SUBPARTITION)',
        re.IGNORECASE,
    )
    MYSQL_SIMPLE_IDENTIFIER_RE = re.compile(r'^`?([A-Za-z_][A-Za-z0-9_]*)`?$')
    MYSQL_VOLATILE_TABLE_OPTIONS_RE = re.compile(r'\bAUTO_INCREMENT\s*=\s*\d+\b', re.IGNORECASE)

    def __init__(self, replicator):
        self.replicator = replicator
        self.last_touch_time = 0
        self.last_save_state_time = 0

    def create_initial_structure(self):
        self.replicator.state.status = Status.CREATING_INITIAL_STRUCTURES
        for table in self.replicator.state.tables:
            self.create_initial_structure_table(table)
        self.replicator.state.save()

    def parse_tables_structure(self):
        for table in self.replicator.state.tables:
            if not self.replicator.config.is_table_matches(table):
                continue
            if self.replicator.single_table and self.replicator.single_table != table:
                continue
            mysql_create_statement = self.replicator.mysql_api.get_table_create_statement(table)
            mysql_structure = self.replicator.converter.parse_mysql_table_structure(
                mysql_create_statement, required_table_name=table,
            )
            self.store_table_structure_signature(table, mysql_create_statement)
            target_table_name = self.replicator.get_target_table_name(table)
            
            if self.replicator.skip_initial_replication:
                clickhouse_structure = self.replicator.clickhouse_api.get_table_structure(target_table_name)
                clickhouse_structure.preprocess()
            else:
                clickhouse_structure = self.replicator.converter.convert_table_structure(mysql_structure)
                clickhouse_structure.table_name = target_table_name
            
            self.replicator.state.tables_structure[table] = (mysql_structure, clickhouse_structure)

    def create_initial_structure_table(self, table_name):
        if not self.replicator.config.is_table_matches(table_name):
            return

        if self.replicator.single_table and self.replicator.single_table != table_name:
            return

        mysql_create_statement = self.replicator.mysql_api.get_table_create_statement(table_name)
        mysql_structure = self.replicator.converter.parse_mysql_table_structure(
            mysql_create_statement, required_table_name=table_name,
        )
        self.store_table_structure_signature(table_name, mysql_create_statement)
        self.validate_mysql_structure(mysql_structure)
        clickhouse_structure = self.replicator.converter.convert_table_structure(mysql_structure)
        
        target_table_name = self.replicator.get_target_table_name(table_name)
        clickhouse_structure.table_name = target_table_name
        
        # Always set if_not_exists to True to prevent errors when tables already exist
        clickhouse_structure.if_not_exists = True
        
        self.replicator.state.tables_structure[table_name] = (mysql_structure, clickhouse_structure)
        indexes = self.replicator.config.get_indexes(self.replicator.database, table_name)
        partition_bys = self.replicator.config.get_partition_bys(self.replicator.database, table_name)
        order_bys = self.replicator.config.get_order_bys(self.replicator.database, table_name)

        if not self.replicator.is_parallel_worker:
            # Drop table if multiple MySQL databases map to same ClickHouse database
            if self.replicator.is_multi_mysql_to_single_ch:
                logger.info(f'dropping table {target_table_name} before recreating (multi-mysql-to-single-ch mode)')
                self.replicator.clickhouse_api.drop_table(target_table_name)
            
            self.replicator.clickhouse_api.create_table(clickhouse_structure, additional_indexes=indexes, additional_partition_bys=partition_bys, additional_order_bys=order_bys)

    def validate_mysql_structure(self, mysql_structure: TableStructure):
        for key_idx in mysql_structure.primary_key_ids:
            primary_field = mysql_structure.fields[key_idx]
            if 'not null' not in primary_field.parameters.lower():
                logger.warning('primary key validation failed')
                logger.warning(
                    f'\n\n\n    !!!  WARNING - PRIMARY KEY NULLABLE (field "{primary_field.name}", table "{mysql_structure.table_name}") !!!\n\n'
                    'There could be errors replicating nullable primary key\n'
                    'Please ensure all tables has NOT NULL parameter for primary key\n'
                    'Or mark tables as skipped, see "exclude_tables" option\n\n\n'
                )

    def prevent_binlog_removal(self):
        if time.time() - self.last_touch_time < self.BINLOG_TOUCH_INTERVAL:
            return
        binlog_directory = os.path.join(self.replicator.config.binlog_replicator.data_dir, self.replicator.database)
        logger.info(f'touch binlog {binlog_directory}')
        if not os.path.exists(binlog_directory):
            return
        self.last_touch_time = time.time()
        touch_all_files(binlog_directory)

    def save_state_if_required(self, force=False):
        curr_time = time.time()
        if curr_time - self.last_save_state_time < self.SAVE_STATE_INTERVAL and not force:
            return
        self.last_save_state_time = curr_time
        self.replicator.state.tables_last_record_version = self.replicator.clickhouse_api.tables_last_record_version
        self.replicator.state.save()

    def get_structure_signature(self, create_statement):
        # Ignore volatile table options that do not affect schema.
        create_statement = self.MYSQL_VOLATILE_TABLE_OPTIONS_RE.sub('AUTO_INCREMENT=0', create_statement)
        normalized_statement = re.sub(r'\s+', ' ', create_statement).strip()
        return hashlib.sha256(normalized_statement.encode('utf-8')).hexdigest()

    def store_table_structure_signature(self, table_name, create_statement):
        signature = self.get_structure_signature(create_statement)
        self.replicator.state.initial_replication_structure_signatures[table_name] = signature

    def verify_table_structure_unchanged(self, table_name):
        expected_signature = self.replicator.state.initial_replication_structure_signatures.get(table_name)
        if not expected_signature:
            logger.warning(f'Could not find original structure signature for table {table_name}')
            return

        current_mysql_create_statement = self.replicator.mysql_api.get_table_create_statement(table_name)
        current_signature = self.get_structure_signature(current_mysql_create_statement)
        if current_signature == expected_signature:
            return

        current_mysql_structure = self.replicator.converter.parse_mysql_table_structure(
            current_mysql_create_statement, required_table_name=table_name,
        )
        original_mysql_structure, _ = self.replicator.state.tables_structure.get(table_name, (None, None))

        # Fallback to parsed structure comparison to avoid false positives from
        # non-structural CREATE TABLE statement differences.
        if original_mysql_structure is not None and self._compare_table_structures(
            current_mysql_structure, original_mysql_structure,
        ):
            self.replicator.state.initial_replication_structure_signatures[table_name] = current_signature
            return

        logger.warning(
            f'\n\n\n    !!!  WARNING - TABLE STRUCTURE CHANGED DURING REPLICATION (table "{table_name}") !!!\n\n'
            'The MySQL table structure has changed since the initial replication started.\n'
            'This may cause data inconsistency and replication issues.\n'
        )
        logger.error(f'Original structure: {original_mysql_structure}')
        logger.error(f'Current structure: {current_mysql_structure}')
        raise Exception(
            f'Table structure changes detected in: {table_name}. '
            'Initial replication aborted to prevent data inconsistency. '
            'Please restart replication after reviewing the changes.'
        )

    def get_initial_replication_tables(self, start_table=None, include_completed=False):
        tables = []
        start_found = start_table is None

        for table in self.replicator.state.tables:
            if not self.replicator.config.is_table_matches(table):
                continue
            if self.replicator.single_table and self.replicator.single_table != table:
                continue
            if not start_found:
                if table != start_table:
                    continue
                start_found = True
            if not include_completed and table in self.replicator.state.initial_replication_completed_tables:
                continue
            tables.append(table)

        return tables

    def get_parallel_worker_identity(self, table_name):
        eligible_tables = self.get_initial_replication_tables(include_completed=True)
        worker_id = eligible_tables.index(table_name)
        total_workers = max(len(eligible_tables), 1)
        return worker_id, total_workers

    def get_mysql_partition_metadata(self, table_name):
        create_statement = self.replicator.mysql_api.get_table_create_statement(table_name)
        normalized_statement = re.sub(r'\s+', ' ', create_statement).strip()

        partition_columns = []
        partition_match = self.MYSQL_PARTITION_BY_RE.search(normalized_statement)
        if partition_match:
            partition_parts = [part.strip() for part in partition_match.group(1).split(',') if part.strip()]
            for partition_part in partition_parts:
                identifier_match = self.MYSQL_SIMPLE_IDENTIFIER_RE.fullmatch(partition_part)
                if not identifier_match:
                    partition_columns = []
                    break
                partition_columns.append(identifier_match.group(1))

        partition_names = []
        for partition_name_match in self.MYSQL_PARTITION_NAME_RE.finditer(normalized_statement):
            partition_name = partition_name_match.group(1)
            if partition_name.lower() == 'by':
                continue
            partition_names.append(partition_name)

        return partition_columns, partition_names

    def format_query_start_values(self, start_values, field_types):
        if start_values is None:
            return None

        query_start_values = start_values.copy()
        for i in range(len(query_start_values)):
            key_type = field_types[i]
            value = query_start_values[i]
            if 'int' not in key_type.lower():
                value = f"'{value}'"
            query_start_values[i] = value
        return query_start_values

    def update_table_checkpoint(self, table_name, partition_name, max_primary_key, max_order_by_value):
        self.replicator.state.initial_replication_table = table_name
        self.replicator.state.initial_replication_max_primary_key = max_primary_key
        if partition_name is None and max_order_by_value is None:
            self.replicator.state.initial_replication_table_checkpoints.pop(table_name, None)
            return

        self.replicator.state.initial_replication_table_checkpoints[table_name] = {
            'partition_name': partition_name,
            'max_primary_key': max_primary_key,
            'max_order_by_value': max_order_by_value,
        }

    def mark_table_completed(self, table_name):
        if table_name not in self.replicator.state.initial_replication_completed_tables:
            self.replicator.state.initial_replication_completed_tables.append(table_name)
        self.replicator.state.initial_replication_table_checkpoints.pop(table_name, None)
        self.replicator.state.initial_replication_max_primary_key = None
        self.replicator.state.initial_replication_table = None

    def perform_initial_replication(self):
        self.replicator.clickhouse_api.database = self.replicator.target_database_tmp
        logger.info('running initial replication')
        self.replicator.state.status = Status.PERFORMING_INITIAL_REPLICATION
        self.replicator.state.save()
        start_table = self.replicator.state.initial_replication_table
        tables_to_process = self.get_initial_replication_tables(start_table=start_table)

        if not self.replicator.is_parallel_worker and self.replicator.config.initial_replication_threads > 1 and not self.replicator.single_table:
            self.perform_initial_replication_table_parallel(tables_to_process)
        else:
            for table in tables_to_process:
                self.perform_initial_replication_table(table)

        if not self.replicator.is_parallel_worker:
            # Skip database swap if:
            # 1. ignore_deletes is enabled - we're replicating directly to target
            # 2. Multiple MySQL databases map to same ClickHouse database - we're replicating directly to target
            # 3. cluster_mode is enabled - bcz once Distributed & Replicated tables are created we can't change their schema.
            should_skip_db_swap = (
                self.replicator.config.ignore_deletes or 
                self.replicator.is_multi_mysql_to_single_ch or 
                self.replicator.config.cluster_mode
            )
            
            if not should_skip_db_swap:
                logger.info(f'initial replication - swapping database')
                if self.replicator.target_database in self.replicator.clickhouse_api.get_databases():
                    self.replicator.clickhouse_api.execute_command(
                        f'RENAME DATABASE `{self.replicator.target_database}` TO `{self.replicator.target_database}_old`',
                    )
                    self.replicator.clickhouse_api.execute_command(
                        f'RENAME DATABASE `{self.replicator.target_database_tmp}` TO `{self.replicator.target_database}`',
                    )
                    self.replicator.clickhouse_api.drop_database(f'{self.replicator.target_database}_old')
                else:
                    self.replicator.clickhouse_api.execute_command(
                        f'RENAME DATABASE `{self.replicator.target_database_tmp}` TO `{self.replicator.target_database}`',
                    )
            self.replicator.clickhouse_api.database = self.replicator.target_database
            # Execute post-initial-replication commands
            self.execute_post_initial_replication_commands()
        logger.info(f'initial replication - done')

    def perform_initial_replication_table(self, table_name):
        logger.info(f'running initial replication for table {table_name}')

        if not self.replicator.config.is_table_matches(table_name):
            logger.info(f'skip table {table_name} - not matching any allowed table')
            return

        self.verify_table_structure_unchanged(table_name)

        max_primary_key = None

        mysql_table_structure, clickhouse_table_structure = self.replicator.state.tables_structure[table_name]

        logger.debug(f'mysql table structure: {mysql_table_structure}')
        logger.debug(f'clickhouse table structure: {clickhouse_table_structure}')

        field_types = [field.field_type for field in clickhouse_table_structure.fields]

        primary_keys = clickhouse_table_structure.primary_keys
        primary_key_ids = clickhouse_table_structure.primary_key_ids
        primary_key_types = [field_types[key_idx] for key_idx in primary_key_ids]
        mysql_field_ids = {
            field.name: idx for idx, field in enumerate(mysql_table_structure.fields)
        }
        mysql_field_types = {
            field.name: field.field_type for field in mysql_table_structure.fields
        }

        partition_columns, partition_names = self.get_mysql_partition_metadata(table_name)
        partition_columns = [column for column in partition_columns if column in mysql_field_ids]

        table_checkpoint = self.replicator.state.initial_replication_table_checkpoints.get(table_name)
        has_legacy_checkpoint = (
            self.replicator.state.initial_replication_table == table_name and
            self.replicator.state.initial_replication_max_primary_key is not None
        )
        should_restart_partitioned_table = (
            bool(partition_names) and
            has_legacy_checkpoint and
            table_checkpoint is None and
            self.replicator.config.initial_replication_threads > 1
        )
        if should_restart_partitioned_table:
            logger.info(
                f'restarting partitioned table {table_name} from scratch to enable partition-aware replication'
            )
            self.replicator.state.initial_replication_max_primary_key = None
            self.replicator.state.initial_replication_table_checkpoints.pop(table_name, None)
            has_legacy_checkpoint = False
            self.replicator.state.save()

        use_partition_scan = bool(partition_names)
        if use_partition_scan and has_legacy_checkpoint and table_checkpoint is None:
            logger.info(
                f'continuing legacy checkpoint for partitioned table {table_name} without partition-aware restart'
            )
            use_partition_scan = False
            partition_names = []
            partition_columns = []

        scan_order_by = []
        for column_name in partition_columns + primary_keys:
            if column_name not in scan_order_by:
                scan_order_by.append(column_name)

        scan_order_key_ids = [mysql_field_ids[column_name] for column_name in scan_order_by]
        scan_order_key_types = [mysql_field_types[column_name] for column_name in scan_order_by]

        current_partition_name = None
        max_order_by_value = None
        if use_partition_scan and table_checkpoint is not None:
            current_partition_name = table_checkpoint.get('partition_name')
            max_primary_key = table_checkpoint.get('max_primary_key')
            max_order_by_value = table_checkpoint.get('max_order_by_value')
            logger.info(f'continue from partition {current_partition_name}, primary key {max_primary_key}')
        elif self.replicator.state.initial_replication_table == table_name:
            max_primary_key = self.replicator.state.initial_replication_max_primary_key
            max_order_by_value = max_primary_key.copy() if max_primary_key is not None else None
            logger.info(f'continue from primary key {max_primary_key}')
        else:
            logger.info(f'replicating from scratch')
            self.replicator.state.initial_replication_table = table_name
            self.replicator.state.initial_replication_max_primary_key = None
            self.replicator.state.initial_replication_table_checkpoints.pop(table_name, None)
            self.replicator.state.save()

        stats_number_of_records = 0
        last_stats_dump_time = time.time()
        last_structure_check_time = 0
        partitions_to_process = partition_names if use_partition_scan else [None]
        partition_index = 0

        if current_partition_name and current_partition_name in partitions_to_process:
            partition_index = partitions_to_process.index(current_partition_name)

        while partition_index < len(partitions_to_process):
            current_partition_name = partitions_to_process[partition_index]

            while True:
                if time.time() - last_structure_check_time >= self.SAVE_STATE_INTERVAL:
                    self.verify_table_structure_unchanged(table_name)
                    last_structure_check_time = time.time()

                start_values = max_order_by_value if use_partition_scan else max_primary_key
                query_start_values = self.format_query_start_values(start_values, scan_order_key_types if use_partition_scan else primary_key_types)

                mysql_records = self.replicator.mysql_api.get_records(
                    table_name=table_name,
                    order_by=scan_order_by if use_partition_scan else primary_keys,
                    limit=self.replicator.config.initial_replication_batch_size,
                    start_value=query_start_values,
                    worker_id=self.replicator.worker_id,
                    total_workers=self.replicator.total_workers,
                    enable_worker_hashing=False,
                    partition_name=current_partition_name,
                )
                logger.debug(f'extracted {len(mysql_records)} records from mysql')

                records = self.replicator.converter.convert_records(mysql_records, mysql_table_structure, clickhouse_table_structure)

                if self.replicator.config.debug_log_level:
                    logger.debug(f'records: {records}')

                if not records:
                    break
                target_table_name = self.replicator.get_target_table_name(table_name)
                self.replicator.clickhouse_api.insert(target_table_name, records, table_structure=clickhouse_table_structure)
                for mysql_record in mysql_records:
                    record_primary_key = [mysql_record[key_idx] for key_idx in mysql_table_structure.primary_key_ids]
                    record_order_by_value = [mysql_record[key_idx] for key_idx in scan_order_key_ids]
                    if max_primary_key is None:
                        max_primary_key = record_primary_key
                    else:
                        max_primary_key = max(max_primary_key, record_primary_key)
                    if max_order_by_value is None:
                        max_order_by_value = record_order_by_value
                    else:
                        max_order_by_value = max(max_order_by_value, record_order_by_value)

                self.update_table_checkpoint(
                    table_name=table_name,
                    partition_name=current_partition_name if use_partition_scan else None,
                    max_primary_key=max_primary_key,
                    max_order_by_value=max_order_by_value if use_partition_scan else None,
                )
                self.save_state_if_required()
                self.prevent_binlog_removal()

                stats_number_of_records += len(records)
                
                # Test flag: Exit early if we've replicated enough records for testing
                if (self.replicator.initial_replication_test_fail_records is not None and 
                    stats_number_of_records >= self.replicator.initial_replication_test_fail_records):
                    logger.info(
                        f'TEST MODE: Exiting initial replication after {stats_number_of_records} records '
                        f'(limit: {self.replicator.initial_replication_test_fail_records})'
                    )
                    return
                
                curr_time = time.time()
                if curr_time - last_stats_dump_time >= 60.0:
                    last_stats_dump_time = curr_time
                    logger.info(
                        f'replicating {table_name}, '
                        f'replicated {stats_number_of_records} records, '
                        f'primary key: {max_primary_key}',
                    )

            partition_index += 1
            if partition_index < len(partitions_to_process):
                max_primary_key = None
                max_order_by_value = None
                self.update_table_checkpoint(
                    table_name=table_name,
                    partition_name=partitions_to_process[partition_index],
                    max_primary_key=None,
                    max_order_by_value=None,
                )
                self.save_state_if_required(force=True)

        logger.info(
            f'finish replicating {table_name}, '
            f'replicated {stats_number_of_records} records, '
            f'primary key: {max_primary_key}',
        )
        self.mark_table_completed(table_name)
        self.save_state_if_required(force=True)
    
    def _compare_table_structures(self, struct1, struct2):
        """
        Compare two TableStructure objects in a deterministic way.
        Returns True if the structures are equivalent, False otherwise.
        """
        # Compare basic attributes
        if struct1.table_name != struct2.table_name:
            logger.error(f"Table name mismatch: {struct1.table_name} vs {struct2.table_name}")
            return False
            
        if struct1.charset != struct2.charset:
            logger.error(f"Charset mismatch: {struct1.charset} vs {struct2.charset}")
            return False
            
        # Compare primary keys (order matters)
        if len(struct1.primary_keys) != len(struct2.primary_keys):
            logger.error(f"Primary key count mismatch: {len(struct1.primary_keys)} vs {len(struct2.primary_keys)}")
            return False
            
        for i, key in enumerate(struct1.primary_keys):
            if key != struct2.primary_keys[i]:
                logger.error(f"Primary key mismatch at position {i}: {key} vs {struct2.primary_keys[i]}")
                return False
                
        # Compare fields (count and attributes)
        if len(struct1.fields) != len(struct2.fields):
            logger.error(f"Field count mismatch: {len(struct1.fields)} vs {len(struct2.fields)}")
            return False
            
        for i, field1 in enumerate(struct1.fields):
            field2 = struct2.fields[i]
            
            if field1.name != field2.name:
                logger.error(f"Field name mismatch at position {i}: {field1.name} vs {field2.name}")
                return False
                
            if field1.field_type != field2.field_type:
                logger.error(f"Field type mismatch for {field1.name}: {field1.field_type} vs {field2.field_type}")
                return False
                
            # Compare parameters - normalize whitespace to avoid false positives
            params1 = ' '.join(field1.parameters.lower().split())
            params2 = ' '.join(field2.parameters.lower().split())
            if params1 != params2:
                logger.error(f"Field parameters mismatch for {field1.name}: {params1} vs {params2}")
                return False
                
        return True

    def perform_initial_replication_table_parallel(self, table_names):
        """
        Execute initial replication using multiple worker processes, one table per worker at a time.
        """
        if not table_names:
            return

        logger.info(
            f"Starting parallel replication for {len(table_names)} tables with {self.replicator.config.initial_replication_threads} workers"
        )

        active_processes = []
        next_table_index = 0
        last_progress_log_time = 0.0

        def launch_worker(table_name):
            worker_id, total_workers = self.get_parallel_worker_identity(table_name)
            cmd = [
                sys.executable, "-m", "mysql_ch_replicator.main",
                "db_replicator",
                "--config", self.replicator.settings_file,
                "--db", self.replicator.database,
                "--worker_id", str(worker_id),
                "--total_workers", str(total_workers),
                "--table", table_name,
                "--target_db", self.replicator.target_database_tmp,
                "--initial_only=True",
            ]

            logger.info(f"Launching worker for table {table_name}: {' '.join(cmd)}")
            process = subprocess.Popen(cmd)
            active_processes.append({
                'table_name': table_name,
                'process': process,
            })

        try:
            while next_table_index < len(table_names) or active_processes:
                while (
                    next_table_index < len(table_names) and
                    len(active_processes) < self.replicator.config.initial_replication_threads
                ):
                    launch_worker(table_names[next_table_index])
                    next_table_index += 1

                for active_process in active_processes[:]:
                    process = active_process['process']
                    if process.poll() is None:
                        continue

                    table_name = active_process['table_name']
                    exit_code = process.returncode
                    active_processes.remove(active_process)

                    if exit_code != 0:
                        logger.error(f"Worker process for table {table_name} failed with exit code {exit_code}")
                        for pending_process in active_processes:
                            pending_process['process'].terminate()
                        raise Exception(f"Worker process for table {table_name} failed with exit code {exit_code}")

                    logger.info(f"Worker process for table {table_name} completed successfully")
                    self.consolidate_worker_record_versions(table_name)
                    self.mark_table_completed(table_name)
                    self.save_state_if_required(force=True)

                if active_processes:
                    time.sleep(0.1)
                    if time.time() - last_progress_log_time >= 30.0:
                        last_progress_log_time = time.time()
                        logger.info(f"Still waiting for {len(active_processes)} workers to complete")
        except KeyboardInterrupt:
            logger.warning("Received interrupt, terminating worker processes")
            for active_process in active_processes:
                active_process['process'].terminate()
            raise

        logger.info("All workers completed initial replication")
        
    def consolidate_worker_record_versions(self, table_name):
        """
        Query ClickHouse directly to get the maximum record version for the specified table
        and update the main state with this version.
        """
        logger.info(f"Getting maximum record version from ClickHouse for table {table_name}")
        
        target_table_name = self.replicator.get_target_table_name(table_name)
        # Query ClickHouse for the maximum record version
        max_version = self.replicator.clickhouse_api.get_max_record_version(target_table_name)
        
        if max_version is not None and max_version > 0:
            current_version = self.replicator.state.tables_last_record_version.get(table_name, 0)
            if max_version > current_version:
                logger.info(f"Updating record version for table {table_name} from {current_version} to {max_version}")
                self.replicator.state.tables_last_record_version[table_name] = max_version
                self.replicator.state.save()
            else:
                logger.info(f"Current version {current_version} is already up-to-date with ClickHouse version {max_version}")
        else:
            logger.warning(f"No record version found in ClickHouse for table {table_name}")

    def execute_post_initial_replication_commands(self):
        """
        Execute custom commands after initial replication finishes.
        Commands are configured per database in the config file.
        """
        commands = self.replicator.config.get_post_initial_replication_commands(self.replicator.database)
        
        if not commands:
            logger.info(f'No post-initial-replication commands configured for database {self.replicator.database}')
            return
        
        logger.info(f'Executing {len(commands)} post-initial-replication commands for database {self.replicator.database}')
       
        # we should not execute USE <db> through clickhouse_api.execute_command instead we should have ch_api.set_database function
        # todo: https://github.com/bakwc/mysql_ch_replicator/issues/225
        self.replicator.clickhouse_api.execute_command(f'USE `{self.replicator.target_database}`')
        
        for i, command in enumerate(commands, 1):
            logger.info(f'Executing command {i}/{len(commands)}: {command[:100]}...')
            self.replicator.clickhouse_api.execute_command(command)
            logger.info(f'Command {i}/{len(commands)} executed successfully')
        
        logger.info(f'All post-initial-replication commands executed successfully')
