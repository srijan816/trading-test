#!/bin/bash
cd /Volumes/SrijanExt/Code/finance/trading-test
export PYTHONPATH=src
echo "Starting Arena Dashboard on http://localhost:8050"
.venv/bin/python -m arena.dashboard.app
