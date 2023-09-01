from typing import Optional


class AddressTypeNotSupportedError(RuntimeError):
    """
    should be raised by abstract factory implementations if given specific DirectAddress is not supported
    """
    pass


class MessageTransferError(RuntimeError):
    def __init__(self, message: str = '', *, wrapped_exception: Optional[Exception]):
        super().__init__(message)
        self.__wrapped_exception = wrapped_exception
        
    def wrapped_exception(self) -> Optional[Exception]:
        return self.__wrapped_exception

    def __repr__(self):
        return f'<{self.__class__}: {repr(self.wrapped_exception())}>'

    def __str__(self):
        return f'{self.__class__}: {str(self.wrapped_exception())}'


class StreamOpeningError(MessageTransferError):
    pass


class MessageSendingError(MessageTransferError):
    pass


class MessageReceivingError(MessageTransferError):
    pass


class MessageTimeoutError(MessageTransferError):
    pass


class MessageTransferTimeoutError(MessageTimeoutError):
    pass


class NoMessageError(MessageTransferTimeoutError):
    pass


class MessageReceiveTimeoutError(MessageTimeoutError):
    """
    Note, this is different from MessageTransferTimeoutError,
      as this exception does not imply transfer error on THIS side of comm
    """
    pass


class MessageRoutingError(RuntimeError):
    pass
