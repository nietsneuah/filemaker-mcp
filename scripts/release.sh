#!/usr/bin/env bash
# release.sh — Bump version, update CHANGELOG, tag, push, create GitHub release.
#
# Usage:
#   scripts/release.sh patch          # 0.1.1 → 0.1.2
#   scripts/release.sh minor          # 0.1.1 → 0.2.0
#   scripts/release.sh major          # 0.1.1 → 1.0.0
#   scripts/release.sh 0.2.0          # explicit version
#   scripts/release.sh patch --dry-run # show what would happen
#
# Prerequisites: gh CLI authenticated, clean working tree, on main branch.

set -euo pipefail

export PATH="$HOME/.local/bin:$PATH"

INIT_FILE="src/filemaker_mcp/__init__.py"
CHANGELOG="CHANGELOG.md"

# --- Parse arguments ---
DRY_RUN=false
VERSION_ARG=""
for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN=true ;;
        *)
            if [[ -n "$VERSION_ARG" ]]; then
                echo "ERROR: Multiple version arguments provided: '$VERSION_ARG' and '$arg'. Use exactly one."
                exit 1
            fi
            VERSION_ARG="$arg"
            ;;
    esac
done

if [[ -z "$VERSION_ARG" ]]; then
    echo "Usage: scripts/release.sh <patch|minor|major|X.Y.Z> [--dry-run]"
    exit 1
fi

# --- Read current version ---
CURRENT=$(sed -n 's/^__version__ = "\(.*\)"/\1/p' "$INIT_FILE")
if [[ -z "$CURRENT" ]]; then
    echo "ERROR: Could not read version from $INIT_FILE"
    exit 1
fi

if ! [[ "$CURRENT" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "ERROR: Current version '$CURRENT' in $INIT_FILE is not a simple X.Y.Z version."
    exit 1
fi

IFS='.' read -r CUR_MAJOR CUR_MINOR CUR_PATCH <<< "$CURRENT"

# --- Compute new version ---
case "$VERSION_ARG" in
    patch) NEW_VERSION="$CUR_MAJOR.$CUR_MINOR.$((CUR_PATCH + 1))" ;;
    minor) NEW_VERSION="$CUR_MAJOR.$((CUR_MINOR + 1)).0" ;;
    major) NEW_VERSION="$((CUR_MAJOR + 1)).0.0" ;;
    *)
        if [[ "$VERSION_ARG" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
            NEW_VERSION="$VERSION_ARG"
        else
            echo "ERROR: Invalid version '$VERSION_ARG'. Use patch, minor, major, or X.Y.Z."
            exit 1
        fi
        ;;
esac

echo "Version: $CURRENT → $NEW_VERSION"

# --- Preflight checks ---
if ! command -v gh >/dev/null 2>&1; then
    echo "ERROR: gh CLI not found. Install from https://cli.github.com/"
    exit 1
fi
if ! gh auth status >/dev/null 2>&1; then
    echo "ERROR: gh CLI not authenticated. Run 'gh auth login'."
    exit 1
fi

if [[ "$(git branch --show-current)" != "main" ]]; then
    echo "ERROR: Must be on main branch (currently on $(git branch --show-current))"
    exit 1
fi

if [[ -n "$(git status --porcelain)" ]]; then
    echo "ERROR: Working tree is not clean. Commit or stash changes first."
    exit 1
fi

# Check CHANGELOG has unreleased content
UNRELEASED_CONTENT=$(sed -n '/^## \[Unreleased\]/,/^## /p' "$CHANGELOG" | sed '1d;$d' | grep -v '^$' || true)
if [[ -z "$UNRELEASED_CONTENT" ]]; then
    echo "WARNING: No content under [Unreleased] in CHANGELOG.md"
    echo "  The release will have an empty changelog section."
    if [[ "$DRY_RUN" == false ]]; then
        read -rp "Continue anyway? [y/N] " confirm
        if [[ "$confirm" != [yY] ]]; then
            echo "Release cancelled by user."
            exit 1
        fi
    fi
fi

if [[ "$DRY_RUN" == true ]]; then
    echo ""
    echo "=== DRY RUN ==="
    echo "Would update $INIT_FILE: __version__ = \"$NEW_VERSION\""
    echo "Would update $CHANGELOG: [Unreleased] → [$NEW_VERSION] — $(date +%Y-%m-%d)"
    echo "Would commit: release: v$NEW_VERSION"
    echo "Would tag: v$NEW_VERSION"
    echo "Would push: main + tags"
    echo "Would create GitHub release: v$NEW_VERSION"
    if [[ -n "$UNRELEASED_CONTENT" ]]; then
        echo ""
        echo "Changelog content:"
        echo "$UNRELEASED_CONTENT"
    fi
    exit 0
fi

# --- Run tests ---
echo ""
echo "Running tests..."
uv run pytest tests/ -x -q
echo ""

# --- Update version in __init__.py ---
sed -i '' "s/__version__ = \"$CURRENT\"/__version__ = \"$NEW_VERSION\"/" "$INIT_FILE"
echo "Updated $INIT_FILE"

# --- Update CHANGELOG ---
TODAY=$(date +%Y-%m-%d)
python3 -c "
import pathlib
p = pathlib.Path('$CHANGELOG')
text = p.read_text()
text = text.replace(
    '## [Unreleased]',
    '## [Unreleased]\n\n## [$NEW_VERSION] — $TODAY',
    1,
)
p.write_text(text)
"
echo "Updated $CHANGELOG"

# --- Commit, tag, push ---
git add "$INIT_FILE" "$CHANGELOG"
git commit -m "release: v$NEW_VERSION"
git tag -a "v$NEW_VERSION" -m "v$NEW_VERSION"
git push origin main "v$NEW_VERSION"
echo ""

# --- Create GitHub release ---
RELEASE_NOTES=$(sed -n "/^## \[$NEW_VERSION\]/,/^## /p" "$CHANGELOG" | sed '1d;$d')
gh release create "v$NEW_VERSION" --title "v$NEW_VERSION" --notes "$RELEASE_NOTES"

echo ""
echo "Released v$NEW_VERSION"
