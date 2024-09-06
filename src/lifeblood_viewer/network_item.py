from PySide2.QtWidgets import QGraphicsItem


class NetworkItem(QGraphicsItem):
    def __init__(self, id):
        super().__init__()

        # cheat cuz Shiboken.Object does not respect mro
        mro = self.__class__.mro()
        cur_mro_i = mro.index(NetworkItem)
        if len(mro) > cur_mro_i + 2:
            super(mro[cur_mro_i+2], self).__init__()

        self.__id = id

    def get_id(self):
        return self.__id


class NetworkItemWithUI(NetworkItem):
    def update_ui(self):
        self.update()  # currently contents and UI are drawn always together, so this will do
        # but in future TODO: invalidate only UI layer

    def draw_imgui_elements(self, drawing_widget):
        """
        this should only be called from active opengl context!
        :return:
        """
        pass
