"""
connectors/base.py — BaseConnector per HyperSpace-AGI v1.03

Ogni connettore deve:
  - Definire `name` (stringa univoca, es. "github", "office365", "google")
  - Implementare `get_tools()` → lista tool in formato OpenAI function-calling
  - Implementare `execute()` → dispatcher che ritorna str o None
  - Opzionalmente sovrascrivere `enabled` per il check delle credenziali

Il ConnectorManager carica automaticamente tutti i connector con enabled=True.
Override di `enabled` consigliato per connettori che richiedono env var specifiche
(vedi office365.py, github.py, google.py).

Override via env: CONNECTOR_<NAME>_ENABLED=false  per disabilitare forzatamente.
"""
from abc import ABC, abstractmethod
from typing import Any
import os


class BaseConnector(ABC):
    """Interfaccia base per tutti i connettori esterni di HyperSpace-AGI."""

    # Nome univoco del connettore — usato come prefisso dei tool e nei log
    name: str = "base"

    @property
    def enabled(self) -> bool:
        """
        Ritorna True se il connettore è abilitato e le credenziali sono disponibili.

        Logica:
          1. Se CONNECTOR_<NAME>_ENABLED=false  → sempre disabilitato (override manuale)
          2. Altrimenti chiama is_available()   → controlla le credenziali specifiche

        I connettori derivati possono sovrascrivere questo metodo oppure
        sovrascrivere solo is_available() per semplicità.
        """
        force_off = os.getenv(f"CONNECTOR_{self.name.upper()}_ENABLED", "").lower()
        if force_off == "false":
            return False
        return self.is_available()

    def is_available(self) -> bool:
        """
        Controlla se le credenziali/env var necessarie sono presenti.
        Sovrascrivere nei connettori che richiedono configurazione specifica.
        Default: True (sempre disponibile, utile per connettori senza auth).
        """
        return True

    @abstractmethod
    def get_tools(self) -> list[dict]:
        """
        Ritorna la lista di tool in formato OpenAI function-calling.
        Ogni elemento deve avere la struttura:
          {
            "type": "function",
            "function": {
              "name": str,           # univoco in tutta la mesh
              "description": str,    # descrizione chiara per l'LLM
              "parameters": { ... }  # JSON Schema
            }
          }
        """
        ...

    @abstractmethod
    def execute(self, tool_name: str, args: dict) -> str | None:
        """
        Esegue il tool richiesto e ritorna una stringa leggibile dall'LLM.
        Ritorna None se il tool_name non appartiene a questo connettore
        (il ConnectorManager passerà al connettore successivo).
        """
        ...
