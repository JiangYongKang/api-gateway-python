"""Local entrypoint: ``uv run uvicorn main:app``"""
from __future__ import annotations

import logging

from gateway.app import create_app
from gateway.demo import build_demo_gateway
from gateway.logging_setup import configure_logging

configure_logging(logging.INFO)

gateway = build_demo_gateway()
app = create_app(gateway)
