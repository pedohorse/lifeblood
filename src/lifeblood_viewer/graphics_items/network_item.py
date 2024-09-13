
class NetworkItem:
    def __init__(self, id):
        super().__init__()
        self.__id = id

    def get_id(self):
        return self.__id


class NetworkItemWithUI(NetworkItem):
    def item_updated(self, *, redraw: bool = False, ui: bool = False):
        """
        should be called when item's state is changed
        :param redraw: True if item itself redraw is needed
        :param ui: True if item's parameter ui redraw is needed
        """
        if redraw:
            self.update()
        if ui:
            self.update()  # currently contents and UI are drawn always together, so this will do
            # but in future TODO: invalidate only UI layer

    def draw_imgui_elements(self, drawing_widget):
        """
        this should only be called from active opengl context!
        :return:
        """
        pass
