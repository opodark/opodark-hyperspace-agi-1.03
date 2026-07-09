from abc import ABC, abstractmethod
import os

class BaseConnector(ABC):
    name: str = "base"

    def __init__(self):
        self.enabled = os.getenv(f"CONNECTOR_{self.name.upper()}_ENABLED", "true").lower() == "true"

    @abstractmethod
    def get_tools(self) -> list:
        """Ritorna lista di tool in formato OpenAI"""
        pass

    @abstractmethod
    def execute(self, tool_name: str, args: dict) -> str:
        """Esegue il tool e ritorna stringa (risultato per l'LLM)"""
        pass
