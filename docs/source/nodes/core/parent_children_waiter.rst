.. _nodes/core/parent_children_waiter:

======================
Parent-Children Waiter
======================

Core task synchronization node.  # TODO: link to core split concept

Tasks coming from input 1 are treated as parents, tasks coming from input 2 are treated as children.

In standard mode, a parent task is not let through until all of it's **active** children have arrived in the same node.
In the same way children tasks are not let through until their parent and **all siblings** have arrived.

Again: a parent and all it's active children will be waiting in this node until all of them arrive.

After every required task has arrived - they may exchange attributes.

When tasks are let through - they leave through the output that corresponds to the input they came through

Parameters
==========

recursive
    if this is set - tasks coming through second input are treated as both parents and children, therefore tasks coming from input 1 will be waiting
    for all their children, grandchildren and so on.

Transfer Attributes

attribute
    name of an attribute to promote from children to parent
as
    set promoted value to attribute of this name on parent
sort by
    how to sort children task before gathering values for promotion
reversed
    reverse the sort

Attributes Set
==============

Promoted attributes are set to the parent task
