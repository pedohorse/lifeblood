.. _nodes/core/attribute_splitter:

==================
Attribute Splitter
==================

Splits task's attribute value into chunks of "chunk size" elements.

All created tasks are **splits** of the original task, so they need to be split-awaited later

:ref:`Split concept<concept_split>` is one of the core Lifeblood's task concepts.

It **List** more works ONLY on array/list attributes.

.. note::
    that task wil ALWAYS be splitted, even if just in 1 piece

For ex. attribute "something" of value ``[3, 4, 5, 6, 7, 8, 9, 10 , 11, 12, 13]`` with chunk size of 4
will split task into 3 tasks with attributes "something" equals to:

* ``[3, 4, 5, 6]``
* ``[7, 8, 9, 10]``
* ``[11, 12, 13]``

respectively

Parameters
==========

:Mode:
    Split mode:

    * list
        Will take the value of given attribute and if it is a list - split it into chunks of given size
        and create a task for each chunk. Each new task will have the value of the given attribute to be set
        to one of the chunks.

        So splitting a task with ``attr`` that has value ``[1, 2, 3, 4, "five", 6.6]`` with chunk size 3 will result
        in a split into 3 tasks with the value of ``attr`` being ``[1, 2]``, ``[3, 4]`` and ``["five", 6.6]`` respectively.
    * range
        Given float or int range start, end and increment will split this range into either given number of chunks, or some number of chunks
        with given number of elements

        For example, splitting range with start=15, end=29, increment=3 and number of chunks 4
        will result in incoming task being split into 4 tasks with values of given attributes
        ``out start name``, ``out end name`` or ``out size name`` being set to
        ``(15, 21, 2)``, ``(21, 24, 1)``, ``(24, 27, 1)`` and ``(27, 29, 1)`` respectively
:Attribute To Split:
    In List mode - the name of the attribute to split and set on split tasks
:Chunk Size:
    Size of the chunks to split list/range into
:Range Mode:
    For range mode - type of range to output to splitted tasks

    * **start-end** - set starting and ending elements
    * **start-size** - set starting element and the number of elements
    * **start-end-size** - set starting element and both end element and the number of elements
:Start:
    For range mode - start of the range to be split
:End:
    For range mode - end of the range to be split
:Inc:
    For range mode - increment of the range to be split
:Split By:
    How to split given list/range

    * chunk size
        Split by the desired number of elements in a single chunk

    * number of chunks
        Split by desired number of chunks
:Chunk Size:
    Desired number of elements in each chunk
:Chunks Count:
    Desired number of chunks
:Type:
    For range mode - type of output range
:Output Range Start Attribute Name:
    For range mode - name of the attribute to set on split tasks that holds the starting number of the range
:Output Range End Attribute Name:
    For range mode - name of the attribute to set on split tasks that holds the ending number of the range
:Output Range Size Attribute Name:
    For range mode - name of the attribute to set on split tasks that holds the number of elements in the range


Attributes Set
==============

In List mode attribute named as the value of ``Attribute To Split`` parameter is set

In Range mode - attributes named as the values of ``Output Range Start Attribute Name``, ``Output Range End Attribute Name`` and ``Output Range Size Attribute Name`` are set
