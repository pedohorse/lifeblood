
class NetworkItem:
    def __init__(self, id):
        super().__init__()
        self.__id = id

    def get_id(self):
        return self.__id


class NetworkItemWithUI(NetworkItem):
    def draw_imgui_elements(self, drawing_widget):
        """
        this should only be called from active opengl context!
        :return:
        """
        pass
