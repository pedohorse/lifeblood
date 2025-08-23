from enum import Enum
from datetime import datetime
from lifeblood.net_messages.address import AddressChain

from typing import Iterable, Optional


class PingEntityIdleness(Enum):
    ACTIVE = 0
    WORKING_IDLE = 1
    SLEEPING_IDLE = 2


class PingEntity:
    def address(self) -> AddressChain:
        raise NotImplementedError()

    def idleness(self) -> PingEntityIdleness:
        raise NotImplementedError()

    def last_checked(self) -> datetime:
        """
        should return UTC time when ping for the corresponding entity was last accepted
        """
        raise NotImplementedError()

    def ping_data(self) -> dict:
        raise NotImplementedError()


class PingReply:
    def __init__(self, entity: PingEntity, reply_data: Optional[dict], exception: Optional[Exception] = None):
        self.__entity = entity
        self.__data = reply_data
        self.__exception = exception

    def entity(self) -> PingEntity:
        return self.__entity

    def reply_data(self) -> Optional[dict]:
        return self.__data

    def exception(self) -> Optional[Exception]:
        return self.__exception

    def is_success(self) -> bool:
        return self.__exception is None


class PingProducerBase:
    """
    this must return all non-pruned entities to ping.
    missing entities are treated as "offline"

    all entities returned by select_entities()
    will eventually either be passed to
    entity_accepted and then eventually to entity_reply_received, or
    entity_discarded
    """
    async def select_entities(self) -> Iterable[PingEntity]:
        raise NotImplementedError()

    async def entity_reply_received(self, reply: PingReply):
        pass

    async def entity_accepted(self, entity: PingEntity):
        pass

    async def entity_discarded(self, entity: PingEntity):
        pass
