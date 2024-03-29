.. _nodes/core/wedge:

================
Wedge Attributes
================

Split incoming task into some number of split tasks, where each of split tasks will hold a variation of an attribute

:ref:`Split concept<concept_split>` is one of the core Lifeblood's task concepts.

Parameters
==========

:Wedge Type:
    Create split tasks based on:

    * by count
        create a set number of split tasks
    * by inc
        create number of split tasks based on a range of values

:Attribute:
    Name of the attribute to hold the wedged value
:From:
    Starting value for wedges
:To:
    For type "by count": ending value for wedges
:Count:
    For type "by count": number of values to split from-to range into
:Max:
    For type "by inc": the maximum value of the range
:Inc:
    For type "by inc": the increment. starting from value "from" a split task will be created
    adding "inc" to it until value is higher than "max"


Attributes Set
==============

Attributes being wedged will be set on task splits
