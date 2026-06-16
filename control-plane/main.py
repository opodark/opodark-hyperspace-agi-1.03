# control-plane/main.py
# HyperSpace AGI v0.2 — Control Plane + Dashboard v2

from flask import Flask, request, jsonify, render_template_string, Response
import os, threading, time, requests, json, uuid, random
from datetime import datetime

app = Flask(__name__)

# ── CONFIG ──────────────────────────────────────────────────
NODE_ENDPOINTS = [e.strip() for e in os.getenv("NODE_ENDPOINTS","node:8084").split(",") if e.strip()]
OLLAMA_URL    = os.getenv("OLLAMA_URL","http://host.docker.internal:11434")
DEFAULT_MODEL = os.getenv("OLLAMA_MODEL","phi3")
_AUTHORITY_URL     = os.getenv("AUTHORITY_URL","http://authority:8080")
_AUTHORITY_ENABLED = os.getenv("AUTHORITY_ENABLED","false").lower()=="true"
LOG_LIMIT = 500

# ── STATE ───────────────────────────────────────────────────
tasks: dict = {}
event_logs  = []
_nodes_by_id: dict  = {}
_known_endpoints: set = set(NODE_ENDPOINTS)

hb_state = {"cycle":0,"last_tick":None,"last_conn":None,
            "last_dream":None,"last_chat":None,
            "nodes_seen":[],"running":False}

advanced_config = {
    "ollama":     {"url":OLLAMA_URL,"defaultModel":DEFAULT_MODEL},
    "mesh":       {"nodeEndpoints":NODE_ENDPOINTS,"heartbeatEvery":15},
    "_authority": {"serverUrl":_AUTHORITY_URL,"enabled":_AUTHORITY_ENABLED},
    "security":   {"sharedSecret":"","secretRotatedAt":None},
}

# ── HELPERS ─────────────────────────────────────────────────
def _ep_to_url(ep):
    return ep.rstrip("/") if ep.startswith("http") else f"http://{ep}"

def _is_public_ep(ep):
    if ep.startswith("https://"): return True
    if ep.startswith("http://"):
        host=ep.split("//")[1].split(":"