#!/bin/bash
set -e

echo "Starting V9 Architecture Update..."
git checkout v9

# We will use python scripts to safely patch files
