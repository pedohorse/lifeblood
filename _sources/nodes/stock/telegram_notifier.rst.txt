.. _nodes/stock/telegram_notifier:

=================
Telegram Notifier
=================

Telegram notifier node allows you to send text and files to a given chat through a given bot.

Bot must be created beforehand and added to all chats where it needs
to send messages to.

it is recommended to set up bot_id and chat_id once in then config,
not to expose them in node parameters.

To do that - keep the default expressions in node's parameters, and
set values of ``bot_id`` and room in your ``<home>/lifeblood/nodes/config.toml``

``chat_id`` is not that secret, but it may also be kept in config for the ease of future changes

::

    telegram_notifier.bot_id = '<secret_bot_id here>'
    telegram_notifier.chat_id = '<chat id here>'


Parameters
==========

:Bot Secret ID:
    Bot's secret, the one telegram's `@BotFather <https://t.me/BotFather>`_ gives you.

    Read the `official tutorial <https://core.telegram.org/bots/tutorial#obtain-your-bot-token>`_ on
    how to create your personal notification bot. (Don't be afraid to give your bot complicated and even unreadable name,
    it's just for your team to use after all.)

    .. note::
        Remember, keep your bot_id is a secret, it allows anyone to control the bot.
:Chat ID:
    Chat ID where to send messages.

    You can create a new group for you and the bot (and your team members maybe), add your bot there too.

    To read messages in groups, the bot needs admin rights, but to write messages it does not. But to write messages in **channels** bot
    needs admin privileges in that telegram channel.

    To find out the proper ``chat id``:

    * Add bot to the group or channel

      * If it is channel - give bot admin rights
      * If it is a group - write some random command to the bot, like ``/test @my_bot_name_here``
        (where ``my_bot_name_here`` should be your actual bot's name)

    * Now in your browser open a **new private tab** and go to ``https://api.telegram.org/<bot_id>/getUpdates``, where ``<bot_id>``
      is that secret thing you got when creating the bot.
      It will look something like this: ``4839574812:AAFD39kkdpWt3ywyRZergyOLMaJhac60qc``

      .. note::
        it's recommented to use a **private** tab to prevent that url with bot_id in it from being saved into browser history
    * There you will see a json dump of a reply, look for something that looks like ``"chat": {"id": -123123123123 ....``
    * That number after ``"id"`` is the chat id (yes, it can be a negative number, so do not forget to preserve the minus sign)
    * If you see more than one chat id in that json dump - means that you have sent more than one message to your bot, so it's up to you to figure out
      what chat id is what. (it should not be hard to do if you look through that whole json data)
:Fail on error:
    If message is not sent due to network, authentication or really any other error - set the task into error state.
    If this is disabled - task will be considered done regardless of the
    success or failure of the message sending.
:Formatting:
    How to parse the message.

    Read about supported modes in `telegram API docs <https://core.telegram.org/bots/api#formatting-options>`_

    .. warning::
        If message formatting syntax is wrong (not closed html tag, something not escaped, backticks opened and not closed)
        telegram server will return ERROR, it's not like in telegram client that just ignores those errors.

        So it is your responsibility to ensure correct syntax if formatting is set to anything but ``Plain``
:Message:
    Message to send. Message's formatting must be correct.
:Attach a file:
    Optionally, a file may be attached to the message. it can be image, video
    or any other file.
:Use worker:
    By default notification is sent by scheduler's child process,
    but you may want to use workers instead, for example if there is a LOT of
    simultaneous uploads of many files, that may clog scheduler's processing pool.

    With this checkbox set, notification will be scheduled to be sent from workers.
    However it's up to you to ensure you don't hit any rate limits with telegram servers.

    .. warning::
        **BEWARE:** when executing on workers - ``bot_id`` **WILL BE SAVED TO SCHEDULER'S DATABASE**
        as part of Invocation Job description. So anyone with direct access to the database will be
        able to find your ``bot_id`` in it
