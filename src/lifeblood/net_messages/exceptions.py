class MessageTransferError(RuntimeError):
    def __init__(self, message: str = '', *, wrapped_exception: Exception):
        super().__init__(message)
        self.__wrapped_exception = wrapped_exception
        
    def wrapped_exception(self):
        return self.__wrapped_exception

    def __repr__(self):
        return f'<{self.__class__}: {repr(self.wrapped_exception())}>'

    def __str__(self):
        return f'{self.__class__}: {str(self.wrapped_exception())}'


class MessageSendingError(MessageTransferError):
    pass


class MessageReceivingError(MessageTransferError):
    pass


class MessageRoutingError(RuntimeError):
    pass
