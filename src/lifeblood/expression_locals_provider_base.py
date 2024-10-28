from typing import Any, Dict


class ExpressionLocalsProviderBase:
    def locals(self) -> Dict[str, Any]:
        raise NotImplementedError()
