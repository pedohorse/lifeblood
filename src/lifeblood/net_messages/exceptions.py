class MessageTransferError(RuntimeError):
    def __init__(self, message: str = '', *, wrapped_exception: Exception):
        super().__init__(message)
        self.__wrapped_exception = wrapped_exception
        
    def wrapped_exception(self):
        return self.__wrapped_exception


class MessageSendingError(MessageTransferError):
    pass


class MessageReceivingError(MessageTransferError):
    pass
