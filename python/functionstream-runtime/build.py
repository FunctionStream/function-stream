#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
构建脚本：将 functionstream-runtime 打包成 WASM 组件

使用 componentize-py 将 Python 代码编译成 WebAssembly 组件
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path

# 获取脚本所在目录
SCRIPT_DIR = Path(__file__).parent.absolute()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
WIT_DIR = PROJECT_ROOT / "wit"
OUTPUT_DIR = SCRIPT_DIR / "target"
WASM_OUTPUT = OUTPUT_DIR / "functionstream-runtime.wasm"


def check_dependencies():
    """检查必要的依赖"""
    # 检查 componentize-py（尝试多种方式）
    found = False
    componentize_cmd = None
    
    # 方法1: 直接调用 componentize-py（如果在 PATH 中）
    try:
        result = subprocess.run(
            ["componentize-py", "--version"],
            capture_output=True,
            check=True,
            timeout=5
        )
        componentize_cmd = ["componentize-py"]
        found = True
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        pass
    
    # 方法2: 查找用户 Python bin 目录中的 componentize-py
    if not found:
        try:
            import os
            home = os.path.expanduser("~")
            # 检查常见的 Python 版本路径
            for py_version in ["3.9", "3.10", "3.11", "3.12"]:
                bin_path = Path(home) / "Library" / "Python" / py_version / "bin" / "componentize-py"
                if bin_path.exists() and bin_path.is_file():
                    # 测试是否可执行
                    try:
                        result = subprocess.run(
                            [str(bin_path), "--version"],
                            capture_output=True,
                            check=True,
                            timeout=5
                        )
                        componentize_cmd = [str(bin_path)]
                        found = True
                        print(f"Found componentize-py at: {bin_path}")
                        break
                    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
                        continue
        except Exception:
            pass
    
    # 方法3: 如果安装了包但找不到命令，提示用户
    if not found:
        try:
            result = subprocess.run(
                ["python3", "-m", "pip", "show", "componentize-py"],
                capture_output=True,
                check=True,
                timeout=5
            )
            # 包已安装但命令找不到
            print("Warning: componentize-py is installed but command not found in PATH")
            print("Please add Python bin directory to PATH:")
            print("  export PATH=\"$HOME/Library/Python/3.9/bin:$PATH\"")
            print("Or run the build script with the full path to componentize-py")
        except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
            pass
    
    if not found:
        print("Error: componentize-py not found")
        print("Please install it:")
        print("  python3 -m pip install componentize-py")
        print("Then ensure it's in your PATH or add:")
        print("  export PATH=\"$HOME/Library/Python/3.9/bin:$PATH\"")
        sys.exit(1)
    
    return componentize_cmd


def ensure_dependencies():
    """确保依赖包已安装到 dependencies 目录（供 componentize-py 使用）"""
    # 创建 dependencies 目录
    deps_dir = SCRIPT_DIR / "dependencies"
    if deps_dir.exists():
        shutil.rmtree(deps_dir)
    deps_dir.mkdir(parents=True, exist_ok=True)
    
    # 将 functionstream-api 安装到 dependencies 目录（而不是系统 site-packages）
    # 这样 componentize-py 可以通过 -p 参数找到它
    fs_api_dir = SCRIPT_DIR.parent / "functionstream-api"
    if not fs_api_dir.exists():
        print(f"Error: functionstream-api directory not found: {fs_api_dir}")
        print("Please ensure functionstream-api exists at the expected location")
        sys.exit(1)
    
    print(f"Installing functionstream-api to dependencies directory...")
    try:
        # 使用 --target 将 functionstream-api 安装到 dependencies 目录
        result = subprocess.run(
            ["python3", "-m", "pip", "install", "--target", str(deps_dir), str(fs_api_dir)],
            check=True,
            capture_output=True,
            text=True
        )
        print("✓ functionstream-api installed to dependencies directory")
        return deps_dir
    except subprocess.CalledProcessError as e:
        print(f"Error: Failed to install functionstream-api: {e}")
        if e.stderr:
            print("STDERR:", e.stderr)
        sys.exit(1)


def generate_bindings(componentize_cmd):
    """生成 WIT 绑定代码并保存到 bindings 目录（作为参考保留）"""
    print("Generating WIT bindings (for reference)...")
    
    # WIT 文件路径
    wit_file = WIT_DIR / "processor.wit"
    if not wit_file.exists():
        print(f"Error: WIT file not found: {wit_file}")
        sys.exit(1)
    
    # 绑定输出目录（保留作为参考，不删除）
    bindings_dir = SCRIPT_DIR / "bindings"
    # 注意：不删除现有目录，保留之前的绑定代码作为参考
    # 如果需要重新生成，可以手动删除 bindings 目录
    if not bindings_dir.exists():
        bindings_dir.mkdir(parents=True, exist_ok=True)
        print(f"Creating bindings directory: {bindings_dir}")
    else:
        print(f"Bindings directory already exists: {bindings_dir}")
        print("  (Keeping existing bindings as reference. Delete manually to regenerate.)")
    
    # 生成绑定命令
    # 格式：componentize-py -d <wit-file> -w <world> bindings <output-dir>
    cmd = componentize_cmd + [
        "-d", str(wit_file),      # WIT 文件路径
        "-w", "processor",        # world 名称
        "bindings",               # bindings 子命令
        str(bindings_dir),        # 输出目录
    ]
    
    print(f"Running: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            cwd=SCRIPT_DIR,
            check=True,
            capture_output=True,
            text=True,
        )
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        
        if bindings_dir.exists() and any(bindings_dir.iterdir()):
            print(f"✓ WIT bindings generated successfully!")
            print(f"  Output: {bindings_dir}")
            print(f"  You can now import from: bindings.wit_world.imports")
            return bindings_dir
        else:
            print("⚠ Warning: Bindings directory is empty")
            return None
            
    except subprocess.CalledProcessError as e:
        print(f"⚠ Warning: Failed to generate bindings (exit code {e.returncode})")
        if e.stdout:
            print("STDOUT:", e.stdout)
        if e.stderr:
            print("STDERR:", e.stderr, file=sys.stderr)
        print("Continuing with WASM build (bindings will be generated during componentize)...")
        return None
    except FileNotFoundError:
        print("⚠ Warning: componentize-py not found for bindings generation")
        print("Continuing with WASM build...")
        return None


def build_wasm(componentize_cmd, deps_dir):
    """构建 WASM 组件"""
    print("Building WASM component...")
    
    # 清理输出目录
    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # WIT 文件路径
    wit_file = WIT_DIR / "processor.wit"
    if not wit_file.exists():
        print(f"Error: WIT file not found: {wit_file}")
        sys.exit(1)
    
    # 检查主模块文件（functionstream-runtime.runner 作为入口点）
    main_module_file = SCRIPT_DIR / "src" / "functionstream-runtime" / "runner.py"
    if not main_module_file.exists():
        print(f"Error: Main module not found: {main_module_file}")
        sys.exit(1)
    
    # 构建命令
    # 格式：componentize-py -d <wit-file> -w <world> componentize --stub-wasi -p <python-path> <module-name> -o <output>
    # 使用 functionstream-runtime.runner 作为主模块（包含 WitWorld 类）
    main_module_name = "functionstream-runtime.runner"
    
    # 构建完整命令
    # 使用 --python-path (-p) 指定多个路径：
    # 1. dependencies 目录（包含 fs_api）
    # 2. 当前目录（包含 main.py 和 functionstream-runtime）
    cmd = componentize_cmd + [
        "-d", str(wit_file),      # WIT 文件路径
        "-w", "processor",        # world 名称
        "componentize",
        "--stub-wasi",
        "-p", str(deps_dir),      # Python 路径 1：dependencies 目录（包含 fs_api）
        "-p", str(SCRIPT_DIR / "src"),    # Python 路径 2：src 目录（包含 functionstream-runtime）
        main_module_name,         # 主模块名称
        "-o", str(WASM_OUTPUT),   # 输出文件
    ]
    
    print(f"Running: {' '.join(cmd)}")
    
    # 不需要设置 PYTHONPATH，因为 fs_api 已经在当前目录中
    env = os.environ.copy()
    
    try:
        result = subprocess.run(
            cmd,
            cwd=SCRIPT_DIR,
            check=True,
            capture_output=True,
            text=True,
            env=env
        )
        print(result.stdout)
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        
        if WASM_OUTPUT.exists():
            size = WASM_OUTPUT.stat().st_size / 1024
            print(f"\n✓ Build successful!")
            print(f"  Output: {WASM_OUTPUT}")
            print(f"  Size: {size:.2f} KB")
        else:
            print("\n✗ Build failed: Output file not found")
            sys.exit(1)
            
    except subprocess.CalledProcessError as e:
        print(f"\n✗ Build failed with exit code {e.returncode}")
        if e.stdout:
            print("STDOUT:", e.stdout)
        if e.stderr:
            print("STDERR:", e.stderr, file=sys.stderr)
        sys.exit(1)
    except FileNotFoundError:
        print("Error: componentize-py not found. Please install it:")
        print("  pip install componentize-py")
        sys.exit(1)
    finally:
        # 清理临时目录（可选，保留以便调试）
        pass


def main():
    """主函数"""
    print("=" * 60)
    print("Function Stream Runtime - WASM Build Script")
    print("=" * 60)
    print()
    
    # 检查 componentize-py
    print("Checking componentize-py...")
    componentize_cmd = check_dependencies()
    print("✓ componentize-py found")
    print()
    
    # 安装依赖到 dependencies 目录
    print("Installing dependencies...")
    deps_dir = ensure_dependencies()
    print()
    
    # 生成 WIT 绑定代码（保存到 bindings 目录，供开发时查看）
    print("Generating WIT bindings (for development reference)...")
    bindings_dir = generate_bindings(componentize_cmd)
    if bindings_dir:
        print(f"✓ Bindings saved to: {bindings_dir}")
        print("  You can now inspect the generated WIT bindings structure")
    print()
    
    # 构建 WASM
    build_wasm(componentize_cmd, deps_dir)
    
    print()
    print("=" * 60)
    print("Build completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()

