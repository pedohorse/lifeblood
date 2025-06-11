import sys
from . import enums


update_tasks_priority_on_group_trigger_template = '''
UPDATE "tasks" SET "priority" =
	(
		(SELECT COALESCE(MAX(task_group_attributes.priority), 0.0) AS priority FROM task_group_attributes
		INNER JOIN task_groups ON task_group_attributes."group"==task_groups."group"
		WHERE task_groups.task_id == tasks.id AND task_group_attributes.state == {group_not_archived_state})
		+
		tasks.priority_invocation_adjust
	)
	FROM task_groups
    WHERE task_groups.task_id == tasks.id AND task_groups."group" == {{group_source}}."group";
'''.format(group_not_archived_state=enums.TaskGroupArchivedState.NOT_ARCHIVED.value)


update_tasks_priority_on_invoc_trigger_template = '''
UPDATE "tasks" SET "priority" =
	(
		(SELECT COALESCE(MAX(task_group_attributes.priority), 0.0) AS priority FROM task_group_attributes
		INNER JOIN task_groups ON task_group_attributes."group"==task_groups."group"
		WHERE task_groups.task_id == {{id_source}} AND task_group_attributes.state == {group_not_archived_state})
		+
		tasks.priority_invocation_adjust
	)
	WHERE "id" == {{id_source}};
'''.format(group_not_archived_state=enums.TaskGroupArchivedState.NOT_ARCHIVED.value)


sql_init_script = '''
BEGIN TRANSACTION;
CREATE TABLE IF NOT EXISTS "lifeblood_metadata" (
	"version"	INTEGER NOT NULL DEFAULT 1,
	"component"	TEXT NOT NULL DEFAULT 'scheduler',
	"unique_db_id"	INTEGER DEFAULT NULL
);
CREATE TABLE IF NOT EXISTS "global_data" (
	"next_split_id"	INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS "worker_groups" (
	"worker_hwid"	INTEGER NOT NULL,
	"group"	TEXT NOT NULL,
	FOREIGN KEY("worker_hwid") REFERENCES "resources"("hwid") ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS "tasks" (
	"id"	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	"parent_id"	INTEGER,
	"children_count"	INTEGER NOT NULL DEFAULT 0,
	"active_children_count"	INTEGER NOT NULL DEFAULT 0,
	"state"	INTEGER NOT NULL,
	"state_details"	TEXT,
	"paused"	INTEGER DEFAULT 0,
	"node_id"	INTEGER NOT NULL,
	"node_input_name"	TEXT,
	"node_output_name"	TEXT,
	"work_data"	BLOB,
	"work_data_invocation_attempt"	INTEGER NOT NULL DEFAULT 0,
	"name"	TEXT,
	"attributes"	TEXT NOT NULL DEFAULT '{{}}',
	"split_level"	INTEGER NOT NULL DEFAULT 0,
	"priority"	REAL NOT NULL DEFAULT 50,
	"priority_invocation_adjust"	REAL NOT NULL DEFAULT 0,
	"priority_tie_order" REAL NOT NULL DEFAULT 0,
	"_invoc_requirement_clause"	TEXT,
	"environment_resolver_data"	BLOB,
	"dead"	INTEGER NOT NULL DEFAULT 0,
	FOREIGN KEY("node_id") REFERENCES "nodes"("id") ON UPDATE CASCADE ON DELETE RESTRICT,
	FOREIGN KEY("parent_id") REFERENCES "tasks"("id") ON UPDATE CASCADE ON DELETE SET NULL
);
CREATE TABLE IF NOT EXISTS "resources" (
	"hwid"	INTEGER NOT NULL UNIQUE,
	PRIMARY KEY("hwid")
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS "workers" (
	"id"	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	"last_address"	TEXT UNIQUE,
	"last_seen"	INTEGER,
	"last_checked"	INTEGER,
	"ping_state"	INTEGER NOT NULL,
	"state"	INTEGER NOT NULL,
	"worker_type"	INTEGER NOT NULL DEFAULT 0,
	"hwid"	INTEGER NOT NULL,
	"session_key"   INTEGER DEFAULT NULL,
	FOREIGN KEY("hwid") REFERENCES "resources"("hwid") ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS "task_splits" (
	"split_id"	INTEGER NOT NULL,
	"task_id"	INTEGER NOT NULL,
	"split_element"	INTEGER NOT NULL DEFAULT 0,
	"split_count"	INTEGER NOT NULL,
	"origin_task_id"	INTEGER NOT NULL,
	"split_sealed"	INTEGER NOT NULL DEFAULT 0,
	FOREIGN KEY("origin_task_id") REFERENCES "tasks"("id") ON UPDATE CASCADE ON DELETE RESTRICT,
	FOREIGN KEY("task_id") REFERENCES "tasks"("id") ON UPDATE CASCADE ON DELETE RESTRICT
);
CREATE TABLE IF NOT EXISTS "node_connections" (
	"id"	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	"node_id_in"	INTEGER NOT NULL,
	"node_id_out"	INTEGER NOT NULL,
	"in_name"	TEXT NOT NULL DEFAULT 'main',
	"out_name"	TEXT NOT NULL DEFAULT 'main',
	FOREIGN KEY("node_id_out") REFERENCES "nodes"("id") ON UPDATE CASCADE ON DELETE CASCADE,
	FOREIGN KEY("node_id_in") REFERENCES "nodes"("id") ON UPDATE CASCADE ON DELETE CASCADE,
	UNIQUE(node_id_in, node_id_out, in_name, out_name)
);
CREATE TABLE IF NOT EXISTS "nodes" (
	"id"	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	"type"	TEXT NOT NULL,
	"name"	TEXT,
	"node_object"	BLOB,
	"node_object_state"	BLOB
);
CREATE TABLE IF NOT EXISTS "task_groups" (
	"task_id"	INTEGER NOT NULL,
	"group"	TEXT NOT NULL,
	FOREIGN KEY("task_id") REFERENCES "tasks"("id") ON UPDATE CASCADE ON DELETE CASCADE
	FOREIGN KEY("group") REFERENCES "task_group_attributes"("group") ON UPDATE CASCADE ON DELETE CASCADE
	UNIQUE ("task_id", "group") ON CONFLICT IGNORE
);
CREATE TABLE IF NOT EXISTS "invocations" (
	"id"	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	"task_id"	INTEGER NOT NULL,
	"worker_id"	NUMERIC NOT NULL,
	"node_id"	INTEGER NOT NULL,
	"state"	INTEGER NOT NULL,
	"return_code"	INTEGER,
	"log_external"	INTEGER NOT NULL DEFAULT 0,
	"stdout"	TEXT,
	"stderr"	TEXT,
	"progress"	REAL,
	"runtime"	REAL,
	"inprog_time"	INTEGER DEFAULT NULL,
	"finish_time"	INTEGER DEFAULT NULL,
	FOREIGN KEY("worker_id") REFERENCES "workers"("id") ON UPDATE CASCADE ON DELETE RESTRICT,
	FOREIGN KEY("task_id") REFERENCES "tasks"("id") ON UPDATE CASCADE ON DELETE RESTRICT,
	FOREIGN KEY("node_id") REFERENCES "nodes"("id") ON UPDATE CASCADE ON DELETE RESTRICT
);
CREATE TABLE IF NOT EXISTS "task_group_attributes" (
	"group"	TEXT NOT NULL UNIQUE,
	"ctime"	INTEGER NOT NULL,
	"state"	INTEGER NOT NULL DEFAULT 0,
	"creator"	TEXT,
	"priority"	REAL NOT NULL DEFAULT 50,
	"user_data" BLOB,
	PRIMARY KEY("group")
);
CREATE INDEX IF NOT EXISTS "task_group_attrs_state_creator_idx" ON "task_group_attributes" (
	"state",
	"creator"
);
CREATE INDEX IF NOT EXISTS "task_dead" ON "tasks" (
	"dead"
);
CREATE INDEX IF NOT EXISTS "task_parent_id" ON "tasks" (
	"parent_id"
);
CREATE INDEX IF NOT EXISTS "invocation_worker_id_state" ON "invocations" (
	"worker_id",
	"state"
);
CREATE INDEX IF NOT EXISTS "task_groups_groups" ON "task_groups" (
	"group"
);
CREATE INDEX IF NOT EXISTS "invocation_worker_id" ON "invocations" (
	"worker_id"
);
CREATE INDEX IF NOT EXISTS "invocations_task_id" ON "invocations" (
	"task_id"
);
CREATE INDEX IF NOT EXISTS "task_groups_task_id" ON "task_groups" (
	"task_id"
);
CREATE INDEX IF NOT EXISTS "task_node_id" ON "tasks" (
	"node_id"
);
CREATE INDEX IF NOT EXISTS "task_splits_task_id" ON "task_splits" (
	"task_id"
);
CREATE INDEX IF NOT EXISTS "task_state" ON "tasks" (
	"state"
);
CREATE INDEX IF NOT EXISTS "task_state_paused_dead_idx" ON "tasks" (
	"state",
	"paused",
	"dead"
);
CREATE INDEX IF NOT EXISTS "task_dead_paused_state_idx" ON "tasks" (
	"dead",
	"paused",
	"state"
);
CREATE INDEX IF NOT EXISTS "invoc_state_idx" ON "invocations" (
	"state"
);
CREATE TRIGGER IF NOT EXISTS children_inserted
AFTER INSERT ON "tasks" WHEN new.parent_id IS NOT NULL
BEGIN
UPDATE "tasks" SET "children_count" = "children_count" + 1 WHERE "id" == new.parent_id;
END;
CREATE TRIGGER IF NOT EXISTS active_children_inserted
AFTER INSERT ON "tasks" WHEN new.state != {dead_state} AND new.parent_id IS NOT NULL
BEGIN
UPDATE "tasks" SET "active_children_count" = "active_children_count" + 1 WHERE "id" == new.parent_id;
END;
CREATE TRIGGER IF NOT EXISTS tasks_turning_dead
AFTER UPDATE OF "state" ON "tasks" WHEN old.state != {dead_state} AND new.state == {dead_state}
BEGIN
UPDATE "tasks" SET "active_children_count" = "active_children_count" - 1 WHERE "id" == new.parent_id;
UPDATE "tasks" SET "dead" = "dead" | 1 WHERE "id" == new."id";
END;
CREATE TRIGGER IF NOT EXISTS tasks_turning_undead
AFTER UPDATE OF "state" ON "tasks" WHEN old.state == {dead_state} AND new.state != {dead_state}
BEGIN
UPDATE "tasks" SET "active_children_count" = "active_children_count" + 1 WHERE "id" == new.parent_id;
UPDATE "tasks" SET "dead" = "dead" & ~1 WHERE "id" == new."id";
END;
CREATE TRIGGER IF NOT EXISTS task_group_turning_unarchived
AFTER UPDATE OF "state" ON "task_group_attributes" WHEN old.state == 1 AND new.state != 1
BEGIN
UPDATE "tasks" SET "dead" = "dead" & ~2 WHERE "id" IN (
	SELECT "task_id" FROM task_groups
	WHERE "group" == new."group"
);
END;
CREATE TRIGGER IF NOT EXISTS task_group_turning_archived
AFTER UPDATE OF "state" ON "task_group_attributes" WHEN old.state != 1 AND new.state == 1
BEGIN
UPDATE "tasks" SET "dead" = "dead" | 2 WHERE "id" IN (
	SELECT "task_id" FROM task_groups
	WHERE "group" == new."group"
);
END;
CREATE TRIGGER IF NOT EXISTS flush_task_state
BEFORE UPDATE OF "state" ON "tasks" WHEN old.state <> new.state
BEGIN
UPDATE "tasks" SET "state_details" = NULL WHERE "id" == new.id;
END;
CREATE TRIGGER IF NOT EXISTS flush_task_input_output_names
BEFORE UPDATE OF "node_id" ON "tasks" WHEN old.node_id <> new.node_id
BEGIN
UPDATE "tasks" SET "node_output_name" = NULL WHERE "id" == new.id;
END;

-- Triggers for invocations timings

CREATE TRIGGER IF NOT EXISTS update_invocations_inprog_time
AFTER UPDATE OF "state" ON "invocations" WHEN old.state != {invoc_inprog_state} AND new.state == {invoc_inprog_state}
BEGIN
	UPDATE "invocations" SET inprog_time = {unixepoch_func} WHERE "id" == new.id;
END;

CREATE TRIGGER IF NOT EXISTS update_invocations_finish_time
AFTER UPDATE OF "state" ON "invocations" WHEN old.state != {invoc_finish_state} AND new.state == {invoc_finish_state} 
BEGIN
	UPDATE "invocations" SET finish_time = {unixepoch_func} WHERE "id" == new.id;
END;

-- Triggers for PRIORITY update
-- update from invocation side

CREATE TRIGGER IF NOT EXISTS update_tasks_priority_from_invoc_update
AFTER UPDATE OF "priority_invocation_adjust" ON "tasks"
BEGIN
{update_tasks_invoc_priority_trigger_body}
END;

CREATE TRIGGER IF NOT EXISTS update_tasks_priority_from_invoc_update
AFTER INSERT ON "tasks"
BEGIN
-- note: task is inserted WITHOUT any group assignment, so we don't check them
UPDATE "tasks" SET "priority" = "priority_invocation_adjust" WHERE "id" == new.id;
END;

-- update from task group side

CREATE TRIGGER IF NOT EXISTS update_tasks_priority_from_group_update
AFTER UPDATE OF "priority" ON "task_group_attributes"
BEGIN
{update_tasks_priority_trigger_body}
END;

CREATE TRIGGER IF NOT EXISTS update_tasks_priority_from_group_insert
AFTER INSERT ON "task_groups"
BEGIN
{insert_task_groups_priority_trigger_body}
END;

CREATE TRIGGER IF NOT EXISTS update_tasks_priority_from_group_delete
AFTER DELETE ON "task_groups"
BEGIN
{delete_tasks_priority_trigger_body}
END;

-- Commit, set WAL and finish

COMMIT;
PRAGMA journal_mode=wal;
PRAGMA synchronous=NORMAL;
'''.format(
    dead_state=enums.TaskState.DEAD.value,
    invoc_inprog_state=enums.InvocationState.IN_PROGRESS.value,
    invoc_finish_state=enums.InvocationState.FINISHED.value,
    update_tasks_priority_trigger_body=update_tasks_priority_on_group_trigger_template.format(group_source='new'),
    delete_tasks_priority_trigger_body=update_tasks_priority_on_invoc_trigger_template.format(id_source='old.task_id'),  # update_tasks_priority_on_group_trigger_template.format(group_source='old'),
    insert_task_groups_priority_trigger_body=update_tasks_priority_on_invoc_trigger_template.format(id_source='new.task_id'),
    update_tasks_invoc_priority_trigger_body=update_tasks_priority_on_invoc_trigger_template.format(id_source='new.id'),
    # windows' python 3.9 and before use older sqlite versions with no unixepoch func
    unixepoch_func='unixepoch()' if sys.version_info.minor > 9 else "CAST(STRFTIME('%s', 'now') AS INT)"
)
# PRAGMA soft_heap_limit=100000000;
# PRAGMA mmap_size=100000000;
# TODO: add after delete triggers for children count


worker_resource_db_init_script = '''
BEGIN TRANSACTION;
CREATE TABLE IF NOT EXISTS "resources" (
	"pid"	INTEGER NOT NULL,
	"cpu_count"	INTEGER NOT NULL,
	"cpu_mem"	INTEGER NOT NULL,
	"gpu_count"	INTEGER NOT NULL,
	"gpu_mem"	INTEGER NOT NULL,
	PRIMARY KEY("pid")
);
COMMIT;
PRAGMA journal_mode=wal;
PRAGMA synchronous=NORMAL;
'''