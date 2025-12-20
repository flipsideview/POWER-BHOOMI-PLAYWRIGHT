#!/bin/bash
# POWER-BHOOMI v4.0 - Repository Setup Script
# This script prepares files for pushing to GitHub

set -e

echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║           POWER-BHOOMI v4.0 - Repository Setup                               ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"
echo ""

# Check if we're in the right directory
if [ ! -f "app_playwright.py" ]; then
    echo "❌ Error: app_playwright.py not found. Run this script from POWER-BHOOMI directory."
    exit 1
fi

# Create new directory for repository
REPO_DIR="POWER-BHOOMI-PLAYWRIGHT"
echo "📁 Creating repository directory: $REPO_DIR"
mkdir -p "$REPO_DIR"

# Copy core files
echo "📋 Copying core implementation files..."
cp portal_health.py "$REPO_DIR/"
cp task_models.py "$REPO_DIR/"
cp playwright_worker.py "$REPO_DIR/"
cp process_supervisor.py "$REPO_DIR/"
cp database_playwright.py "$REPO_DIR/"
cp app_playwright.py "$REPO_DIR/"

# Copy configuration
echo "⚙️  Copying configuration files..."
cp requirements_playwright.txt "$REPO_DIR/"
cp config.yaml "$REPO_DIR/"
cp config_loader.py "$REPO_DIR/"

# Copy documentation
echo "📚 Copying documentation..."
cp ARCHITECTURE.md "$REPO_DIR/"
cp INSTALL_PLAYWRIGHT.md "$REPO_DIR/"
cp TESTING_GUIDE.md "$REPO_DIR/"
cp OPERATIONS_RUNBOOK.md "$REPO_DIR/"
cp IMPLEMENTATION_SUMMARY.md "$REPO_DIR/"
cp MANIFEST.md "$REPO_DIR/"

# Rename README
cp README_PLAYWRIGHT.md "$REPO_DIR/README.md"

# Create .gitignore
echo "🔒 Creating .gitignore..."
cat > "$REPO_DIR/.gitignore" << 'EOF'
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
venv/
env/
ENV/
.venv

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# Application
*.log
*.db
*.db-shm
*.db-wal
*.csv
*.pid
*.lock

# Secrets
config_local.yaml
.env
secrets.yaml
EOF

# Create tests directory
echo "🧪 Creating tests directory..."
mkdir -p "$REPO_DIR/tests"
touch "$REPO_DIR/tests/__init__.py"

# Create scripts directory
echo "🔧 Creating scripts directory..."
mkdir -p "$REPO_DIR/scripts"

# Create LICENSE (MIT)
echo "📜 Creating LICENSE..."
cat > "$REPO_DIR/LICENSE" << 'EOF'
MIT License

Copyright (c) 2025 POWER-BHOOMI Team

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
EOF

# Initialize git repository
echo "🌱 Initializing git repository..."
cd "$REPO_DIR"
git init
git branch -M main

# Create initial commits
echo "💾 Creating initial commits..."

git add portal_health.py task_models.py playwright_worker.py process_supervisor.py database_playwright.py app_playwright.py
git commit -m "feat: Core Playwright+Process implementation

- portal_health.py: HTTP-based health monitoring
- task_models.py: Task dataclasses and serialization
- playwright_worker.py: Worker process with bounded browser
- process_supervisor.py: Process lifecycle management
- database_playwright.py: SQLite manager with WAL mode
- app_playwright.py: Flask control plane

Guarantees:
- Hard browser budget (≤ MAX_WORKERS)
- No orphan processes
- Weeks-long stability"

git add requirements_playwright.txt config.yaml config_loader.py
git commit -m "feat: Configuration and dependencies

- requirements_playwright.txt: Python packages
- config.yaml: Application configuration
- config_loader.py: Configuration loader utility"

git add ARCHITECTURE.md INSTALL_PLAYWRIGHT.md TESTING_GUIDE.md OPERATIONS_RUNBOOK.md IMPLEMENTATION_SUMMARY.md
git commit -m "docs: Complete architecture and operations documentation

- ARCHITECTURE.md: System design (15 pages)
- INSTALL_PLAYWRIGHT.md: Installation guide (6 pages)
- TESTING_GUIDE.md: Testing procedures (12 pages)
- OPERATIONS_RUNBOOK.md: Operations guide (14 pages)
- IMPLEMENTATION_SUMMARY.md: Implementation overview (10 pages)

Total: 65 pages of comprehensive documentation"

git add README.md MANIFEST.md .gitignore LICENSE tests/ scripts/
git commit -m "docs: Add README, manifest, and project metadata

- README.md: Project overview
- MANIFEST.md: File listing and checklist
- .gitignore: Git ignore rules
- LICENSE: MIT License
- tests/: Test directory structure
- scripts/: Utility scripts directory"

echo ""
echo "═══════════════════════════════════════════════════════════════════════════════"
echo "✅ Repository setup complete!"
echo "═══════════════════════════════════════════════════════════════════════════════"
echo ""
echo "📂 Repository location: $REPO_DIR"
echo "📊 Commits created: 4"
echo "📄 Files ready: $(find . -type f | wc -l | tr -d ' ')"
echo ""
echo "Next steps:"
echo "  1. Review the repository: cd $REPO_DIR && ls -la"
echo "  2. Create GitHub repository at: https://github.com/flipsideview/POWER-BHOOMI-PLAYWRIGHT"
echo "  3. Add remote: git remote add origin https://github.com/flipsideview/POWER-BHOOMI-PLAYWRIGHT.git"
echo "  4. Push: git push -u origin main"
echo ""
echo "🚀 Ready to deploy!"
echo ""





