.. _nodes/stock/matrixnotifier:

===============
Matrix Notifier
===============

Matrix notifier node allows you to send text and files to a given room of a given
matrix homeserver.

Parameters
==========

:Backend:
    what to use as a matrix client. By default - a simplest python client
    is provided. It can only send unencrypted messages, which is good enough
    for personal/local homeservers, but not good for public homeservers.
    ``matrix-commander`` may be used as a client instead (also requires libolm).
    see below
:Room:
    room id to connect to. should have form of !ksdojqlfjalskfj:matrix.smth
:Retries:
    how many times to retry if connection fails
:Fail task on notification sending error:
    if after all retries message is not set - set the task into error state.
    if this is disabled - task will be considered done regardless of the
    success or failure of the message sending
:Matrix server:
    matrix homeserver to use
:Message:
    message to send
:Attach a file:
    optionally, a file may be attached to the message. it can be image, video
    or any other file. Be mindful if encryption is used when sending
    sensitive files to public homeservers

Installing Matrix Commander as a back end
=========================================

By default matrix notifier comes with a simple client implementation that does **NOT
implement** E2E encryption.

This is is generally enough if a homeserver is local, but for public homeservers,
such as default matrix.org, it's better to enable proper end-to-end encryption.

To enable end-to-end encryption you need to set up another backend, such as matrix-commander

Installing Matrix-Commander
---------------------------

First of all, you need to ensure you have `olm lib installed <https://gitlab.matrix.org/matrix-org/olm>`_

on linux machines it's available in packages repos under something like ``libolm`` name

matrix-commander is an open source command line `matrix client <https://github.com/8go/matrix-commander>`_

You can install it, keeping it completely isolated from your system libraries,
handy installation script is provided with lifeblood.

1. Open terminal, set it up so that ``python`` is available
2. Navigate into Lifeblood installation location ``lifeblood/stock_nodes/matrixclient/data``
3. There you should see script named ``install_matrix_commander.py``

   This file will use currently available python to create a small virtual environment with matrix-commander in it.
   All in the current directory
4. Run the script with ``python install_matrix_commander.py``
5. If the script reports ``success`` - you should be good, even if there were some errors in the installation process' log

(keep your terminal open if you want to initialize matrix-commander right after installation)

After this you should be able to switch ``matrixnotifier``'s ``backend`` parameter to ``matrix-commander``
(also that will be the default value from now on for new nodes).

But before you can use matrix-commander - you need to initialize it and log in

Initializing Matrix-Commander
-----------------------------

To initialize matrix-commander you need to log in, and to join all the rooms to where your bot is supposed to report to.

1. From the terminal left open from the installation process first enter virtual environment
    * on windows run ``venv/bin/Activate``
    * on any normal operating system do ``. venv/bin/activate`` or ``source venv/bin/activate``
2. Now ``matrix-commander`` command should be available. If it's not - something went wrong while activating vitrual env
3. Depending on your operating system:
    a. Run ``matrix-commander --login password`` to start interactive login process using password
    b. Run ``matrix-commander --login sso`` to start interactive login process using Single Sign-On
4. matrix-commander should prompt you for homeserver, user, password and optionally default room. The room does not
   matter too much as it will be overriden by the node, but if you have one room for notifications - why not add it here.
5. now you need to join any rooms you would want to send notifications to.

   use ``--room-join`` command to join all rooms one by one, like: ``matrix-commander --room-join !roomcode@homeserver.smth``
6. after this you will find file ``credentials.json`` and a directory ``store`` in your current directory -
   these are authentication and e2e info needed to communicate to the server. We need to move it elsewhere
7. Find where you lifeblood config lives (by default
   it's your user dir), there navigate to ``<lifeblood config location>/scheduler/nodes/matrixnotifier/matrixcommander``,
   create missing directories.
8. There you need to create a directory based on a homeserver url you registered at.
    * ``://`` turns into ``..``
    * ``/`` thrns int ``_``

   For example, ``https://matrix.org`` turns into ``https..matrix.org``

   therefore you need to create directory ``<lifeblood config location>/scheduler/nodes/matrixnotifier/matrixcommander/https..matrix.org``
9. Move ``credentials.json`` and ``store`` from before to this new directory
10. That's it!

    at this point you should have ``credentials.json`` and ``store`` in your

    ``<lifeblood config location>/scheduler/nodes/matrixnotifier/matrixcommander/<encoded_homeserver>/``

Now matrix-commander is initialized and you are ready to start getting some notifications