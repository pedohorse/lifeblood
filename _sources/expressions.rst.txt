.. _parameter expressions:

=====================
Parameter expressions
=====================

Overview
========

Almost any parameter on Lifeblood's nodes can have a simple value that will be just always constant,
or an expression.

A parameter expression is a piece of code that will be evaluated any time a task being processed tries
to get node's parameter value.

Currently only Python expressions are supported.

A Python Expression
+++++++++++++++++++

A python parameter expression can be any python expression. Returned value will be casted to appropriate
parameter value type if possible. If not possible - an exception will be thrown and task evaluating that expression
will be marked as Errored.

the following modules are imported into the context of expression evaluation:

* ``os``
* ``pathlib``
* contents of ``math`` (as if ``from math import *`` was performed)

The following names are defined in the context of evaluation:

* ``task`` - the task being processed
* ``node`` - the node that owns this parameter
* ``config`` - this node's configuration at scheduler's side

task
----

represents task being processed, the following methods available:

``__getitem__`` - retrieve any task's attribute
  | you can access task attributes with simple syntax: ``task['attr_name']``
  | for example,  ``task['file']`` ``task['frames'][0]``

``get(attr_name, default=None)`` - same, but with optional default on ``attr_name`` being not found
  | together with previous method they provide access interface similar to that of a dict
  | ``task.get('preferred_filename', 'untitled.mov')``

``id``
  task internal id

``task.parent_id``
  parent task id if this task has a parent, otherwise None

``task.name``
  name of this task (if task was created internally - name may ne None)

node
----

``__getitem__`` - retrieve any node's parameter value
  | you can access other node's parameters with simple syntax: ``node['param_name']``
  | for example,  ``node['hip path']`` ``task['driver path']``

``name``
  name of the node

``label``
  label of the node type, label is used in tab node creation menu

config
------

``__getitem__``/``get`` - retrieve any task's attribute
  | get value from this node type's config on scheduler side.
  | scheduler has a special config file for nodes' custom values
  | :ref:`config-dir` ``/scheduler/nodes/config.toml`` (read more here: :ref:`custom node type configuration`)
