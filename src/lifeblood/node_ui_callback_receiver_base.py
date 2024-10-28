class NodeUiCallbackReceiverBase:
    def _ui_changed(self, definition_changed: bool = False):
        raise NotImplementedError()
