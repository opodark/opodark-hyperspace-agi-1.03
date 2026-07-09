import importlib
import pkgutil
import os
from .base import BaseConnector

class ConnectorManager:
    def __init__(self):
        self.connectors: list[BaseConnector] = []
        self._load_connectors()

    def _load_connectors(self):
        package = __package__
        for _, name, _ in pkgutil.iter_modules([os.path.dirname(__file__)]):
            if name.startswith("_") or name in ("base", "manager"):
                continue
            try:
                module = importlib.import_module(f".{name}", package=package)
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if isinstance(attr, type) and issubclass(attr, BaseConnector) and attr != BaseConnector:
                        connector = attr()
                        if connector.enabled:
                            self.connectors.append(connector)
                            print(f"[ConnectorManager] Loaded: {connector.name}")
            except Exception as e:
                print(f"[ConnectorManager] Failed to load {name}: {e}")

    def get_all_tools(self) -> list:
        tools = []
        for conn in self.connectors:
            tools.extend(conn.get_tools())
        return tools

    def execute(self, tool_name: str, args: dict) -> str:
        for conn in self.connectors:
            try:
                result = conn.execute(tool_name, args)
                if result is not None:
                    return result
            except Exception:
                continue
        return f"Tool '{tool_name}' non gestito da nessun connector attivo."
