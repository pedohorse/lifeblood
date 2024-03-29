.. _nodes/core/parent_children_waiter:

======================
Parent-Children Waiter
======================

Core task synchronization node.

Tasks' :ref:`parent-children hierarchy<task_parent_hierarchy>` is a core concept of Lifeblood.

Tasks coming from input 1 are treated as parents, tasks coming from input 2 are treated as children.

In standard mode, a parent task is not let through until all of it's **active** children have arrived in the same node.
In the same way children tasks are not let through until their parent and **all siblings** have arrived.

Again: a parent and all it's active children will be waiting in this node until all of them arrive.

After every required task has arrived - they may exchange attributes.

When tasks are let through - they leave through the output that corresponds to the input they came through

Parameters
==========

:Recursive:
    if this is set - tasks coming through second input are treated as both parents and children, therefore tasks coming from input 1 will be waiting
    for all their children, grandchildren and so on.

:Transfer Attributes:
    Number of attributes to transfer from children being synced to the parent
:Attribute:
    name of an attribute to promote from children to parent
:As:
    set promoted value to attribute of this name on parent
:Sort by:
    how to sort children task before gathering values for promotion
:Reversed:
    reverse the sort

Attributes Set
==============

Promoted attributes are set to the parent task
