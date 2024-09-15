from .graphics_items import NetworkItem


class GraphicsSceneViewingWidgetBase:
    def request_ui_focus(self, item: NetworkItem):
        raise NotImplementedError()

    def release_ui_focus(self, item: NetworkItem):
        raise NotImplementedError()

    def item_requests_context_menu(self, item):
        raise NotImplementedError()
