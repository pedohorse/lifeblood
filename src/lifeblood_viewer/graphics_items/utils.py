from PySide6.QtCore import QPointF, QTimer


def call_later(callable, *args, **kwargs):  #TODO: this repeats here and in nodeeditor
    if len(args) == 0 and len(kwargs) == 0:
        QTimer.singleShot(0, callable)
    else:
        QTimer.singleShot(0, lambda: callable(*args, **kwargs))


def length2(v: QPointF):
    return QPointF.dotProduct(v, v)

