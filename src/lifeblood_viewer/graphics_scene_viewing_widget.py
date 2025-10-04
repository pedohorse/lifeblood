from .graphics_items import NetworkItem


class GraphicsSceneViewingWidgetBase:
    def __init__(self, **kwargs):
        # explicit constructor needed to work with pyside6 and multiple inheritance
        super().__init__()

    def request_ui_focus(self, item: NetworkItem):
        raise NotImplementedError()

    def release_ui_focus(self, item: NetworkItem):
        raise NotImplementedError()

    def item_requests_context_menu(self, item):
        raise NotImplementedError()
