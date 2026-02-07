#!/usr/bin/env bash
set -euo pipefail

if ! command -v brew >/dev/null 2>&1; then
  echo "Homebrew nicht gefunden. Installiere Homebrew zuerst: https://brew.sh"
  exit 1
fi

brew update
brew install ripgrep fd tree

echo "✅ Tools installiert: rg, fd, tree"
