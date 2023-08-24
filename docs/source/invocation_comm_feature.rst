.. _invocation comm feature:

========================
Invocation Communication
========================

Task invocations in lifeblood can easily exchange data messages
through worker's network api

Overview
========

Any invocation may send arbitrary data to another invocation. for that it needs to know only 2 things:

* target invocation id
* target addressee name

since it requires to know target invocation id - this means that most likely it has to be passed from
parent task's invocation to child task, so that child later may report result or something else.

addressee name is just some string identifying the receiver. single invocation may be waiting for multiple
messages in parallel, so addressee name here acts as sort of a **port**

Message is sent through worker, then routed to scheduler, where scheduler routes it to the worker which is
running the target invocation, if any.

The worker's messaging API includes 2 methods so far:

* send a message to invocation id + addressee name
* wait for a message addressed to given addressee name

Some design details: (not implementation-specific)

* In ordinary situation send_message will return when message was delivered to **target invocation**'s **addressee name**
* Otherwise error text will be available from worker
* If target invocation is already running, but **addressee name** is not yet waiting for messages -
  this is considered a valid situation and **addressee name** will be waited for some time
  (default 90 seconds, which should be more than enough) to start receiving message.
  If not - timeout error will be produced
* If **target invocation** is NOT running - this is considered an **error** situation straight away, no waiting is done.
  The error will reflect if target invocation is already finished, or does not exist at all.
* if both send end receive commands may have timeouts

Python case
-----------

In case of python - worker's network API is wrapped with a small and simple python module ``lifeblood_connection``

When a task invocation is run by worker, an additional path is injected into ``PYTHONPATH``
environment variable. This makes it possible for any script running python to load
``lifeblood_connection`` module

``lifeblood_connection`` module has no non-standard dependencies, and is in fact python-2/3 compatible.
It is made this way to ensure no pollution of python environment is done.

``lifeblood_connection`` module presents a number of very handy methods to easily communicate with Scheduler,
here we will only talk about invocation messaging related functions

functions:

* ``message_to_invocation_send`` function does the sending

  * On error ``MessageSendError`` will be raised with error cause text

* ``message_to_invocation_receive`` function waits for receiving, optional ``timeout`` may be provided

  * On timeout ``MessageReceiveTimeout`` will be raised

Other languages
---------------

There is currently no support for other languages, but it can be easily added, as worker's command web API is very simple
