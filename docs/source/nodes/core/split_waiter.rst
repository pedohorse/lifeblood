.. _nodes/core/split_waiter:

============
Split Waiter
============

Core task synchronization node.

:ref:`Split concept<concept_split>` is one of the core Lifeblood's task concepts.

A Task will be held here until all tasks from the same split this task belongs to have arrived at this task.
So tasks in a split will be synchronized in this node.

When all tasks of a split are in the node, the original task that was split is teleported to this node, attributes from split tasks
may be promoted to the original, and then original task is let through. Split tasks are killed after that.

Parameters
==========

Transfer Attributes

:Attribute:
    name of an attribute to promote from children to parent
:As:
    set promoted value to attribute of this name on parent
:Sort By:
    how to sort children task before gathering values for promotion
:Reversed:
    reverse the sort

Attributes Set
==============

Promoted attributes are set to the parent task
