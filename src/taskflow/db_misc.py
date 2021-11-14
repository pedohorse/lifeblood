from . import enums
sql_init_script = '''
BEGIN TRANSACTION;
CREATE TABLE IF NOT EXISTS "worker_groups" (
	"worker_id"	INTEGER NOT NULL,
	"group"	TEXT NOT NULL,
	FOREIGN KEY("worker_id") REFERENCES "workers"("id") ON UPDATE CASCADE ON DELETE CASCADE
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
	"_invoc_requirement_clause"	TEXT,
	"environment_resolver_data"	BLOB,
	FOREIGN KEY("node_id") REFERENCES "nodes"("id") ON UPDATE CASCADE ON DELETE RESTRICT,
	FOREIGN KEY("parent_id") REFERENCES "tasks"("id") ON UPDATE CASCADE ON DELETE SET NULL
);
CREATE TABLE IF NOT EXISTS "workers" (
	"id"	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	"cpu_count"	TEXT NOT NULL,
	"mem_size"	NUMERIC NOT NULL,
	"gpu_count"	INTEGER NOT NULL,
	"gmem_size"	INTEGER NOT NULL,
	"last_address"	TEXT NOT NULL UNIQUE,
	"last_seen"	INTEGER,
	"last_checked"	INTEGER,
	"ping_state"	INTEGER NOT NULL,
	"state"	INTEGER NOT NULL,
	"worker_type"	INTEGER NOT NULL DEFAULT 0,
	"hwid"	INTEGER NOT NULL
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
	"stdout"	TEXT,
	"stderr"	TEXT,
	"progress"	REAL,
	FOREIGN KEY("worker_id") REFERENCES "workers"("id") ON UPDATE CASCADE ON DELETE RESTRICT,
	FOREIGN KEY("task_id") REFERENCES "tasks"("id") ON UPDATE CASCADE ON DELETE RESTRICT,
	FOREIGN KEY("node_id") REFERENCES "nodes"("id") ON UPDATE CASCADE ON DELETE RESTRICT
);
CREATE TABLE IF NOT EXISTS "task_group_attributes" (
	"id"	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	"group"	TEXT NOT NULL UNIQUE,
	"ctime"	INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS "task_state_paused_idx" ON "tasks" (
	"state",
	"paused"
);
CREATE INDEX IF NOT EXISTS "invoc_state_idx" ON "invocations" (
	"state"
);
CREATE TRIGGER IF NOT EXISTS count_dead_children
AFTER UPDATE OF "state" ON "tasks" WHEN old.state != {dead_state} AND new.state == {dead_state}
BEGIN
UPDATE "tasks" SET "active_children_count" = "active_children_count" - 1 WHERE "id" == new.parent_id;
END;
CREATE TRIGGER IF NOT EXISTS count_undead_children
AFTER UPDATE OF "state" ON "tasks" WHEN old.state == {dead_state} AND new.state != {dead_state}
BEGIN
UPDATE "tasks" SET "active_children_count" = "active_children_count" + 1 WHERE "id" == new.parent_id;
END;
CREATE TRIGGER IF NOT EXISTS flush_task_state
BEFORE UPDATE OF "state" ON "tasks" WHEN old.state <> new.state
BEGIN
UPDATE "tasks" SET "state_details" = NULL WHERE "id" == new.id;
END;
COMMIT;
PRAGMA journal_mode=wal;
'''.format(dead_state=enums.TaskState.DEAD.value)
