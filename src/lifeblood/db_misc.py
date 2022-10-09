from . import enums
sql_init_script = '''
BEGIN TRANSACTION;
CREATE TABLE IF NOT EXISTS "lifeblood_metadata" (
	"version"	INTEGER NOT NULL DEFAULT 1,
	"component"	TEXT NOT NULL DEFAULT 'scheduler',
	"unique_db_id"	INTEGER DEFAULT NULL
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
	"_invoc_requirement_clause"	TEXT,
	"environment_resolver_data"	BLOB,
	"dead"	INTEGER NOT NULL DEFAULT 0,
	FOREIGN KEY("node_id") REFERENCES "nodes"("id") ON UPDATE CASCADE ON DELETE RESTRICT,
	FOREIGN KEY("parent_id") REFERENCES "tasks"("id") ON UPDATE CASCADE ON DELETE SET NULL
);
CREATE TABLE IF NOT EXISTS "resources" (
	"hwid"	TEXT NOT NULL UNIQUE,
	"cpu_count"	INTEGER NOT NULL,
	"total_cpu_count"	INTEGER NOT NULL,
	"cpu_mem"	INTEGER NOT NULL,
	"total_cpu_mem"	INTEGER NOT NULL,
	"gpu_count"	INTEGER NOT NULL,
	"total_gpu_count"	INTEGER NOT NULL,
	"gpu_mem"	INTEGER NOT NULL,
	"total_gpu_mem"	INTEGER NOT NULL,
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
	FOREIGN KEY("node_id_in") REFERENCES "nodes"("id") ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS "nodes" (
	"id"	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	"type"	TEXT NOT NULL,
	"name"	TEXT,
	"node_object"	BLOB
);
CREATE TABLE IF NOT EXISTS "task_groups" (
	"task_id"	INTEGER NOT NULL,
	"group"	TEXT NOT NULL,
	FOREIGN KEY("task_id") REFERENCES "tasks"("id") ON UPDATE CASCADE ON DELETE CASCADE
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
	PRIMARY KEY("group")
);
CREATE INDEX IF NOT EXISTS "task_group_attrs_state_creator_idx" ON "task_group_attributes" (
	"status",
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
COMMIT;
PRAGMA journal_mode=wal;
PRAGMA synchronous=NORMAL;
'''.format(dead_state=enums.TaskState.DEAD.value)
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