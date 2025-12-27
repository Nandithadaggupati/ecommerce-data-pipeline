#!/bin/bash
python -m pytest tests/ -v --cov=scripts --cov-report=html
