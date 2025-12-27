#!/usr/bin/env bash
set -e

echo "=========================================="
echo "E-Commerce Data Pipeline Setup"
echo "=========================================="
echo ""

# Check if Python 3.9+ is installed
echo "[1/5] Checking Python version..."
if ! command -v python3 &> /dev/null; then
    echo "ERROR: Python 3 is not installed"
    exit 1
fi
PYTHON_VERSION=$(python3 --version | awk '{print $2}')
echo "✓ Python $PYTHON_VERSION found"
echo ""

# Create virtual environment
echo "[2/5] Creating virtual environment..."
if [ -d ".venv" ]; then
    echo "Virtual environment already exists; skipping creation"
else
    python3 -m venv .venv
    echo "✓ Virtual environment created"
fi
echo ""

# Activate virtual environment
echo "[3/5] Activating virtual environment..."
source .venv/bin/activate
echo "✓ Virtual environment activated"
echo ""

# Upgrade pip and install dependencies
echo "[4/5] Installing dependencies..."
pip install --upgrade pip setuptools wheel > /dev/null 2>&1
pip install -r requirements.txt
echo "✓ Dependencies installed"
echo ""

# Setup environment file
echo "[5/5] Setting up environment configuration..."
if [ ! -f ".env" ]; then
    cp .env.example .env
    echo "✓ .env file created from .env.example"
    echo "  ⚠️  IMPORTANT: Update .env with your actual database credentials"
else
    echo "✓ .env file already exists"
fi
echo ""

# Create necessary directories
mkdir -p data/raw data/staging data/processed
mkdir -p logs
mkdir -p dashboards/screenshots

echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Update .env with your database credentials"
echo "2. Start Postgres: docker-compose -f docker/docker-compose.yml up -d"
echo "3. Run the pipeline: python scripts/pipeline_orchestrator.py"
echo ""
echo "For more info, see README.md"
echo ""
    