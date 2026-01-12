#!/bin/bash

# Python WASM Processor Build Script
# Compiles Python to Component Model WASM using the official componentize-py tool

# Note: componentize-py only packages modules that are actually imported in the code
# If you need to include dependency libraries in the WASM, you must explicitly import them in the code

set -e

# Function to get millisecond timestamp (compatible with macOS and Linux)
get_timestamp_ms() {
    # Try using date +%s%3N (Linux)
    if date +%s%3N 2>/dev/null | grep -qE '^[0-9]+$'; then
        date +%s%3N
    # Try using gdate (GNU date, macOS via brew install coreutils)
    elif command -v gdate &> /dev/null && gdate +%s%3N 2>/dev/null | grep -qE '^[0-9]+$'; then
        gdate +%s%3N
    # Use Python to get millisecond timestamp (most compatible method)
    elif command -v python3 &> /dev/null; then
        python3 -c "import time; print(int(time.time() * 1000))"
    # Use Perl to get millisecond timestamp
    elif command -v perl &> /dev/null; then
        perl -MTime::HiRes=time -e 'print int(time * 1000)'
    # Fallback to second precision
    else
        echo "$(date +%s)000"
    fi
}

# Record build start time
BUILD_START_TIME=$(date +%s)
BUILD_START_TIME_MS=$(get_timestamp_ms)

# Color definitions
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
WIT_DIR="$PROJECT_ROOT/wit"
BUILD_DIR="$SCRIPT_DIR/build"
TARGET_DIR="$BUILD_DIR"

echo -e "${GREEN}=== Python WASM Processor Build Script ===${NC}"
echo "Using official componentize-py tool"
echo ""

# Check Python
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Error: python3 not found${NC}"
    echo "Please install Python 3.9+: https://www.python.org/downloads/"
    exit 1
fi

PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
echo -e "${GREEN}✓ Python version: $PYTHON_VERSION${NC}"

# Check and install componentize-py
echo ""
echo -e "${YELLOW}=== Checking componentize-py ===${NC}"

# Prefer checking user bin directory (--user installed commands are there)
COMPONENTIZE_CMD=""
USER_BIN_DIR="$HOME/Library/Python/3.9/bin"

if command -v componentize-py &> /dev/null; then
    COMPONENTIZE_CMD="componentize-py"
elif [ -f "$USER_BIN_DIR/componentize-py" ] && [ -x "$USER_BIN_DIR/componentize-py" ]; then
    COMPONENTIZE_CMD="$USER_BIN_DIR/componentize-py"
    echo "Found componentize-py at: $COMPONENTIZE_CMD"
elif python3 -c "import componentize_py" 2>/dev/null; then
    # Package is installed but command is not in PATH, try to find executable
    # Check other possible Python version paths
    for py_version in "3.9" "3.10" "3.11" "3.12"; do
        alt_bin_dir="$HOME/Library/Python/$py_version/bin"
        if [ -f "$alt_bin_dir/componentize-py" ] && [ -x "$alt_bin_dir/componentize-py" ]; then
            COMPONENTIZE_CMD="$alt_bin_dir/componentize-py"
            echo "Found componentize-py at: $COMPONENTIZE_CMD"
            break
        fi
    done
else
    echo "Installing componentize-py..."
    echo "Preferring PyPI installation (recommended, faster and simpler)..."
    echo "Note: Using user installation mode (--user), no sudo required"
    echo "      Package will be installed to: ~/Library/Python/3.9/lib/python/site-packages"
    python3 -m pip install --upgrade pip --user 2>&1 | grep -v "Defaulting to user installation" || true
    
    # Initialize variables
    INSTALL_FAILED=true
    INSTALL_LOG=$(mktemp)
    
    # Method 1: Install from PyPI (recommended, fastest and simplest)
    echo "Method 1: Installing componentize-py from PyPI..."
    
    # Try installing from PyPI (user installation mode, no sudo required)
    if python3 -m pip install --user componentize-py > "$INSTALL_LOG" 2>&1; then
        # pip command succeeded, check if installation actually succeeded
        if python3 -c "import componentize_py" 2>/dev/null || command -v componentize-py &> /dev/null; then
            echo -e "${GREEN}✓ componentize-py installed successfully${NC}"
            INSTALL_FAILED=false
        else
            echo -e "${YELLOW}Install command succeeded but componentize-py not found, trying method 2${NC}"
            cat "$INSTALL_LOG" | tail -10
        fi
    else
        # pip command failed
        INSTALL_FAILED=true
        echo -e "${YELLOW}Method 1 failed, trying method 2: clone and install manually${NC}"
        
        # Check if it's a network issue
        if grep -q "Failed to connect\|Couldn't connect\|Connection refused\|unable to access\|No matching distribution" "$INSTALL_LOG" 2>/dev/null; then
            echo -e "${YELLOW}PyPI installation failed, trying method 2: install from GitHub${NC}"
            echo "Error details:"
            grep -E "Failed to connect|Couldn't connect|unable to access|No matching distribution" "$INSTALL_LOG" | head -3
            echo ""
            
            # Method 2: Install from GitHub (if PyPI fails)
            echo "Method 2: Installing from GitHub (using SSH)..."
            INSTALL_LOG2=$(mktemp)
            if python3 -m pip install --user git+ssh://git@github.com/bytecodealliance/componentize-py.git > "$INSTALL_LOG2" 2>&1; then
                if python3 -c "import componentize_py" 2>/dev/null || command -v componentize-py &> /dev/null; then
                    echo -e "${GREEN}✓ componentize-py installed successfully (method 2: GitHub)${NC}"
                    INSTALL_FAILED=false
                else
                    INSTALL_FAILED=true
                fi
            else
                echo -e "${RED}Method 2 also failed${NC}"
                if grep -q "Permission denied\|Host key verification failed" "$INSTALL_LOG2" 2>/dev/null; then
                    echo "SSH configuration issue, please check: ssh -T git@github.com"
                fi
                INSTALL_FAILED=true
            fi
            rm -f "$INSTALL_LOG2"
        else
            echo "Installation error details:"
            cat "$INSTALL_LOG" | tail -10
        fi
    fi
    rm -f "$INSTALL_LOG"
    
    # Check again (including user bin directory)
    # Note: componentize-py cannot be executed via python3 -m componentize_py
    # Must find the actual executable file
    if command -v componentize-py &> /dev/null; then
        COMPONENTIZE_CMD="componentize-py"
    elif [ -f "$USER_BIN_DIR/componentize-py" ] && [ -x "$USER_BIN_DIR/componentize-py" ]; then
        COMPONENTIZE_CMD="$USER_BIN_DIR/componentize-py"
        echo "Found componentize-py at: $COMPONENTIZE_CMD"
    elif python3 -c "import componentize_py" 2>/dev/null; then
        # Package is installed, try to find executable
        # Check other possible Python version paths
        for py_version in "3.9" "3.10" "3.11" "3.12"; do
            alt_bin_dir="$HOME/Library/Python/$py_version/bin"
            if [ -f "$alt_bin_dir/componentize-py" ] && [ -x "$alt_bin_dir/componentize-py" ]; then
                COMPONENTIZE_CMD="$alt_bin_dir/componentize-py"
                echo "Found componentize-py at: $COMPONENTIZE_CMD"
                break
            fi
        done
        
        # If still not found, try to find installation location via pip show
        if [ -z "$COMPONENTIZE_CMD" ]; then
            PIP_BIN=$(python3 -m pip show -f componentize-py 2>/dev/null | grep "bin/componentize-py" | head -1 | sed 's/.*\.\.\/\.\.\/\.\.\///' | sed 's|^|'"$HOME/Library/Python/3.9/lib/python/site-packages/../..'"'|')
            if [ -n "$PIP_BIN" ] && [ -f "$PIP_BIN" ] && [ -x "$PIP_BIN" ]; then
                COMPONENTIZE_CMD="$PIP_BIN"
                echo "Found componentize-py at: $COMPONENTIZE_CMD"
            fi
        fi
    fi
fi

# Final check
if [ -z "$COMPONENTIZE_CMD" ]; then
    echo -e "${RED}Error: Unable to find or install componentize-py${NC}"
    echo ""
    echo "Possible reasons for installation failure:"
    echo "1. Network connection issue (cannot access GitHub)"
    echo "2. Git configuration issue"
    echo "3. Submodule clone failure"
    echo ""
    echo "Please try manual installation, see detailed guide:"
    echo "  cat $SCRIPT_DIR/INSTALL.md"
    echo ""
    echo "Or directly view: examples/python-processor/INSTALL.md"
    echo ""
    echo "Quick manual installation steps:"
    echo "  1. Install from PyPI (recommended):"
    echo "     pip install --user componentize-py"
    echo ""
    echo "  2. If PyPI is unavailable, install from GitHub (requires SSH key):"
    echo "     pip install --user git+ssh://git@github.com/bytecodealliance/componentize-py.git"
    echo ""
    echo "  3. Or manually clone and install:"
    echo "     git clone --recursive git@github.com:bytecodealliance/componentize-py.git"
    echo "     cd componentize-py"
    echo "     pip install --user ."
    echo ""
    echo "See INSTALL.md for detailed guide"
    exit 1
fi

COMPONENTIZE_VERSION=$($COMPONENTIZE_CMD --version 2>&1 | head -1 || echo "unknown")
echo -e "${GREEN}✓ componentize-py: $COMPONENTIZE_VERSION${NC}"

# Create build directory
mkdir -p "$BUILD_DIR"

# Calculate relative path (from python-processor directory to wit directory, script uses relative path)
cd "$SCRIPT_DIR"
WIT_RELATIVE="../../wit"
if [ ! -d "$WIT_RELATIVE" ]; then
    # If relative path doesn't exist, use absolute path
    WIT_RELATIVE="$WIT_DIR"
fi

# Clean up old bindings directory if it exists (to avoid duplicate items error)
# Note: componentize command will automatically generate bindings, no need to run bindings command separately
if [ -d "wit_world" ]; then
    echo "Cleaning old bindings directory..."
    rm -rf wit_world
fi

# Check and install Python dependencies (if requirements.txt exists)
if [ -f "requirements.txt" ]; then
    echo ""
    echo -e "${YELLOW}=== Installing Python Dependencies ===${NC}"
    echo "Detected requirements.txt, installing dependencies to local directory..."
    
    # Count dependencies
    DEP_COUNT=$(cat requirements.txt | grep -v '^#' | grep -v '^$' | grep -v '^=' | wc -l | tr -d ' ')
    echo "Found $DEP_COUNT dependency packages"
    
    # Check if it's a full dependency set (file size will be very large)
    if [ "$DEP_COUNT" -gt 10 ]; then
        echo -e "${YELLOW}⚠️  Warning: Detected large number of dependencies ($DEP_COUNT packages)${NC}"
        echo "  This will result in:"
        echo "  - Long build time (may take 10-30 minutes)"
        echo "  - Very large WASM file (100-200MB+)"
        echo "  - High memory usage"
        echo ""
        read -p "Continue? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "Build cancelled"
            exit 0
        fi
    fi
    
    # Create dependency directory
    DEPS_DIR="$SCRIPT_DIR/dependencies"
    mkdir -p "$DEPS_DIR"
    
    # Install dependencies to local directory (componentize-py will automatically scan)
    echo "Starting dependency installation..."
    INSTALL_START=$(date +%s)
    if python3 -m pip install --target "$DEPS_DIR" -r requirements.txt 2>&1 | tee /tmp/pip_install.log | grep -v "Defaulting to user installation" | grep -v "already satisfied" | grep -v "Requirement already satisfied"; then
        INSTALL_END=$(date +%s)
        INSTALL_TIME=$((INSTALL_END - INSTALL_START))
        echo -e "${GREEN}✓ Dependencies installed successfully (took ${INSTALL_TIME} seconds)${NC}"
        
        # Calculate dependency directory size
        DEPS_SIZE=$(du -sh "$DEPS_DIR" 2>/dev/null | cut -f1 || echo "unknown")
        echo "Dependency directory size: $DEPS_SIZE"
        echo "Dependency directory: $DEPS_DIR"
        echo "Top 10 largest packages:"
        du -sh "$DEPS_DIR"/* 2>/dev/null | sort -hr | head -10 || echo "Unable to calculate"
    else
        # Check for errors
        if grep -q "ERROR\|error\|Error\|Failed\|failed" /tmp/pip_install.log 2>/dev/null; then
            echo -e "${YELLOW}Warning: Some dependencies may have failed to install, but continuing build${NC}"
            echo "Error details:"
            grep -i "ERROR\|error\|Error\|Failed\|failed" /tmp/pip_install.log | head -5
        else
            echo -e "${GREEN}✓ Dependencies installed successfully${NC}"
        fi
        rm -f /tmp/pip_install.log
    fi
    echo ""
fi

# Compile to WASM component
echo ""
echo "Compiling Python to WASM component..."
# Fix notes (wasmtime 17.0 compatibility):
# - componentize-py 0.19.3 generated Component may have "constant expression required" error in some cases
# - Using --stub-wasi option can reduce WASI dependencies and avoid parsing errors
# - Current Python code (main.py) already avoids using WASI CLI features (no sys, print)
# - Components generated with --stub-wasi have better compatibility with wasmtime 17.0

# Ensure we're in the correct directory (reference script approach)
cd "$SCRIPT_DIR"

# Directly specify WIT file processor.wit (only use user-defined WIT file)
WIT_FILE="$WIT_RELATIVE/processor.wit"
if [ ! -f "$WIT_FILE" ]; then
    WIT_FILE="$WIT_DIR/processor.wit"
fi

if [ ! -f "$WIT_FILE" ]; then
    echo -e "${RED}Error: WIT file not found: processor.wit${NC}"
    exit 1
fi

echo "Using WIT file: $WIT_FILE"

# Use relative path (reference script uses -d wit, we use relative path)
# Reference format: componentize-py -d wit -w logger-client componentize processor --stub-wasi -o output.wasm
# Our format: componentize-py -d <wit-file> -w processor componentize main --stub-wasi -o output.wasm
# Note: Directly specify WIT file instead of directory to avoid scanning other files
# 
# Known issue: componentize-py 0.19.3 will produce "duplicate items detected" error when processing
# WIT files containing static func returning resource type. This is a known bug in componentize-py.
# 
# If you encounter this error, possible solutions:
# 1. Update componentize-py: pip install --user --upgrade componentize-py
# 2. Check GitHub issues: https://github.com/bytecodealliance/componentize-py/issues
# 3. Temporarily use Go or Rust processor (verified working)
#
        # Execute compilation
        # According to official documentation, --stub-wasi should be after componentize, before module name
        # Format: componentize-py -d <wit-file> -w <world> componentize --stub-wasi <module-name> -o <output>
        # Note: componentize-py 0.19.3 already generates Component Model format (includes component keyword)
        # First generate temporary file, then use wasm-tools strip to optimize
        TEMP_WASM="$TARGET_DIR/processor-temp.wasm"

        echo ""
        echo -e "${GREEN}=== Starting Python to WASM Compilation ===${NC}"
        COMPILE_START_TIME=$(date +%s)
        COMPILE_START_TIME_MS=$(get_timestamp_ms)
        
        $COMPONENTIZE_CMD -d "$WIT_FILE" -w processor componentize --stub-wasi main -o "$TEMP_WASM" 2>&1
        COMPILE_RESULT=$?
        
        COMPILE_END_TIME=$(date +%s)
        COMPILE_END_TIME_MS=$(get_timestamp_ms)
        
        # Check if it's a valid number (avoid non-numeric characters)
        if echo "$COMPILE_START_TIME_MS" | grep -qE '^[0-9]+$' && echo "$COMPILE_END_TIME_MS" | grep -qE '^[0-9]+$'; then
            # If millisecond precision is supported
            COMPILE_DURATION_MS=$((COMPILE_END_TIME_MS - COMPILE_START_TIME_MS))
            COMPILE_DURATION_SEC=$((COMPILE_DURATION_MS / 1000))
            COMPILE_DURATION_MS_REMAINDER=$((COMPILE_DURATION_MS % 1000))
            echo -e "${GREEN}✓ Compilation completed: ${COMPILE_DURATION_SEC}.${COMPILE_DURATION_MS_REMAINDER}s${NC}"
        else
            # Only second precision supported
            COMPILE_DURATION=$((COMPILE_END_TIME - COMPILE_START_TIME))
            echo -e "${GREEN}✓ Compilation completed: ${COMPILE_DURATION}s${NC}"
        fi

# If compilation succeeds, use wasm-tools strip to optimize file size
if [ $COMPILE_RESULT -eq 0 ] && [ -f "$TEMP_WASM" ]; then
    echo ""
    echo -e "${GREEN}=== Optimizing WASM File Size ===${NC}"
    STRIP_START_TIME=$(date +%s)
    STRIP_START_TIME_MS=$(get_timestamp_ms)
    
    ORIGINAL_SIZE=$(stat -f%z "$TEMP_WASM" 2>/dev/null || stat -c%s "$TEMP_WASM" 2>/dev/null || echo "0")
    echo "Original size: $(numfmt --to=iec-i --suffix=B $ORIGINAL_SIZE 2>/dev/null || echo "${ORIGINAL_SIZE}B")"
    
    # Check if wasm-tools is available
    if command -v wasm-tools &> /dev/null; then
        # Use wasm-tools strip --all to remove all debug info and unused sections
        if wasm-tools strip --all "$TEMP_WASM" -o "$TARGET_DIR/processor.wasm" 2>&1; then
            STRIP_END_TIME=$(date +%s)
            STRIP_END_TIME_MS=$(get_timestamp_ms)
            
            STRIPPED_SIZE=$(stat -f%z "$TARGET_DIR/processor.wasm" 2>/dev/null || stat -c%s "$TARGET_DIR/processor.wasm" 2>/dev/null || echo "0")
            REDUCTION=$((ORIGINAL_SIZE - STRIPPED_SIZE))
            REDUCTION_PERCENT=$((REDUCTION * 100 / ORIGINAL_SIZE))
            echo "Optimized size: $(numfmt --to=iec-i --suffix=B $STRIPPED_SIZE 2>/dev/null || echo "${STRIPPED_SIZE}B")"
            echo "Reduction: $(numfmt --to=iec-i --suffix=B $REDUCTION 2>/dev/null || echo "${REDUCTION}B") (${REDUCTION_PERCENT}%)"
            
            # Check if it's a valid number
            if echo "$STRIP_START_TIME_MS" | grep -qE '^[0-9]+$' && echo "$STRIP_END_TIME_MS" | grep -qE '^[0-9]+$'; then
                STRIP_DURATION_MS=$((STRIP_END_TIME_MS - STRIP_START_TIME_MS))
                STRIP_DURATION_SEC=$((STRIP_DURATION_MS / 1000))
                STRIP_DURATION_MS_REMAINDER=$((STRIP_DURATION_MS % 1000))
                echo -e "${GREEN}✓ Optimization completed: ${STRIP_DURATION_SEC}.${STRIP_DURATION_MS_REMAINDER}s${NC}"
            else
                STRIP_DURATION=$((STRIP_END_TIME - STRIP_START_TIME))
                echo -e "${GREEN}✓ Optimization completed: ${STRIP_DURATION}s${NC}"
            fi
            
            rm -f "$TEMP_WASM"
        else
            echo -e "${YELLOW}Warning: wasm-tools strip failed, using original file${NC}"
            mv "$TEMP_WASM" "$TARGET_DIR/processor.wasm"
        fi
    else
        echo -e "${YELLOW}Warning: wasm-tools not installed, skipping optimization step${NC}"
        echo "  Install: cargo install wasm-tools"
        mv "$TEMP_WASM" "$TARGET_DIR/processor.wasm"
    fi
fi

# Clean up intermediate files (regardless of success or failure)
echo ""
echo "Cleaning up intermediate files..."
rm -rf wit_world
rm -f componentize-py.toml
rm -rf __pycache__ *.pyc .componentize-py-*
# Clean up intermediate WASM files from previous builds, keep only the final processor.wasm
rm -f "$TARGET_DIR/processor-core.wasm"
rm -f "$TARGET_DIR/processor-embedded.wasm"
rm -f "$TARGET_DIR/processor-temp.wasm"

        # Calculate total build time
        BUILD_END_TIME=$(date +%s)
        BUILD_END_TIME_MS=$(get_timestamp_ms)
        
        if [ $COMPILE_RESULT -eq 0 ] && [ -f "$TARGET_DIR/processor.wasm" ]; then
            echo ""
            echo -e "${GREEN}=== Build Completed ===${NC}"
            echo -e "${GREEN}✓ Python Processor WASM built successfully: $TARGET_DIR/processor.wasm${NC}"
            ls -lh "$TARGET_DIR/processor.wasm"
            
            # Display total build time
            # Check if it's a valid number
            if echo "$BUILD_START_TIME_MS" | grep -qE '^[0-9]+$' && echo "$BUILD_END_TIME_MS" | grep -qE '^[0-9]+$'; then
                TOTAL_DURATION_MS=$((BUILD_END_TIME_MS - BUILD_START_TIME_MS))
                TOTAL_DURATION_SEC=$((TOTAL_DURATION_MS / 1000))
                TOTAL_DURATION_MIN=$((TOTAL_DURATION_SEC / 60))
                TOTAL_DURATION_SEC_REMAINDER=$((TOTAL_DURATION_SEC % 60))
                TOTAL_DURATION_MS_REMAINDER=$((TOTAL_DURATION_MS % 1000))
                
                if [ $TOTAL_DURATION_MIN -gt 0 ]; then
                    echo ""
                    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
                    echo -e "${GREEN}📊 Total build time: ${TOTAL_DURATION_MIN}m ${TOTAL_DURATION_SEC_REMAINDER}.${TOTAL_DURATION_MS_REMAINDER}s${NC}"
                    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
                else
                    echo ""
                    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
                    echo -e "${GREEN}📊 Total build time: ${TOTAL_DURATION_SEC}.${TOTAL_DURATION_MS_REMAINDER}s${NC}"
                    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
                fi
            else
                TOTAL_DURATION=$((BUILD_END_TIME - BUILD_START_TIME))
                TOTAL_DURATION_MIN=$((TOTAL_DURATION / 60))
                TOTAL_DURATION_SEC=$((TOTAL_DURATION % 60))
                
                if [ $TOTAL_DURATION_MIN -gt 0 ]; then
                    echo ""
                    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
                    echo -e "${GREEN}📊 Total build time: ${TOTAL_DURATION_MIN}m ${TOTAL_DURATION_SEC}s${NC}"
                    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
                else
                    echo ""
                    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
                    echo -e "${GREEN}📊 Total build time: ${TOTAL_DURATION}s${NC}"
                    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
                fi
            fi
        elif [ -f "$TARGET_DIR/processor.wasm" ]; then
            # Even if there's an error, if the file was generated, consider it successful
            echo ""
            echo -e "${GREEN}✓ Python Processor WASM built successfully: $TARGET_DIR/processor.wasm${NC}"
            ls -lh "$TARGET_DIR/processor.wasm"
        else
            echo -e "${RED}Error: WASM file not generated${NC}"
            echo ""
            echo "If you encounter 'duplicate items detected' error:"
            echo "  This is a known bug in componentize-py (affects all versions)"
            echo "  See fix guide: cat FIX_DUPLICATE_ERROR.md"
            echo "  Or view: examples/python-processor/FIX_DUPLICATE_ERROR.md"
            echo ""
            echo "Suggestions:"
            echo "  1. Try updating componentize-py: pip install --user --upgrade componentize-py"
            echo "  2. Check GitHub issues: https://github.com/bytecodealliance/componentize-py/issues"
            echo "  3. Temporarily use Go or Rust processor (verified working)"
            exit 1
        fi
