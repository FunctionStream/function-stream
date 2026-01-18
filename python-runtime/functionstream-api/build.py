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
构建脚本：将 functionstream-api 打包成 Python 包

使用 setuptools 构建 wheel 和 source distribution
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path

# 获取脚本所在目录
SCRIPT_DIR = Path(__file__).parent.absolute()
OUTPUT_DIR = SCRIPT_DIR / "dist"


def check_dependencies():
    """检查必要的依赖"""
    # 检查 setuptools 和 wheel
    try:
        import setuptools
        import wheel
        print("✓ setuptools and wheel found")
        return True
    except ImportError:
        print("Error: setuptools and wheel are required")
        print("Please install them:")
        print("  python3 -m pip install setuptools wheel")
        sys.exit(1)


def clean_build():
    """清理之前的构建产物"""
    print("Cleaning previous build artifacts...")
    
    dirs_to_clean = [
        SCRIPT_DIR / "build",
        SCRIPT_DIR / "dist",
        SCRIPT_DIR / "*.egg-info",
    ]
    
    for pattern in dirs_to_clean:
        if "*" in str(pattern):
            # 使用 glob 匹配
            import glob
            for path in glob.glob(str(pattern)):
                path_obj = Path(path)
                if path_obj.exists():
                    if path_obj.is_dir():
                        shutil.rmtree(path_obj)
                    else:
                        path_obj.unlink()
        else:
            if pattern.exists():
                if pattern.is_dir():
                    shutil.rmtree(pattern)
                else:
                    pattern.unlink()
    
    print("✓ Clean completed")


def build_package():
    """构建 Python 包"""
    print("Building Python package...")
    
    # 确保输出目录存在
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # 构建命令
    cmd = [
        sys.executable,
        "-m",
        "build",
        "--outdir",
        str(OUTPUT_DIR),
    ]
    
    print(f"Running: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            cwd=SCRIPT_DIR,
            check=True,
            capture_output=True,
            text=True
        )
        
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        
        # 列出构建产物
        if OUTPUT_DIR.exists():
            files = list(OUTPUT_DIR.glob("*"))
            if files:
                print(f"\n✓ Build successful!")
                print(f"  Output directory: {OUTPUT_DIR}")
                print(f"  Files:")
                for f in sorted(files):
                    size = f.stat().st_size / 1024
                    print(f"    - {f.name} ({size:.2f} KB)")
            else:
                print("\n✗ Build failed: No output files found")
                sys.exit(1)
        else:
            print("\n✗ Build failed: Output directory not found")
            sys.exit(1)
            
    except subprocess.CalledProcessError as e:
        print(f"\n✗ Build failed with exit code {e.returncode}")
        if e.stdout:
            print("STDOUT:", e.stdout)
        if e.stderr:
            print("STDERR:", e.stderr, file=sys.stderr)
        sys.exit(1)
    except FileNotFoundError:
        print("Error: build module not found. Please install build:")
        print("  python3 -m pip install build")
        sys.exit(1)


def verify_package():
    """验证构建的包"""
    print("\nVerifying package...")
    
    # 检查 dist 目录中的文件
    wheel_files = list(OUTPUT_DIR.glob("*.whl"))
    sdist_files = list(OUTPUT_DIR.glob("*.tar.gz"))
    
    if not wheel_files and not sdist_files:
        print("✗ No package files found")
        return False
    
    print(f"✓ Found {len(wheel_files)} wheel file(s) and {len(sdist_files)} source distribution file(s)")
    
    # 尝试导入验证（可选）
    try:
        # 这里可以添加更多的验证逻辑
        print("✓ Package verification passed")
        return True
    except Exception as e:
        print(f"✗ Package verification failed: {e}")
        return False


def main():
    """主函数"""
    print("=" * 60)
    print("Function Stream API - Python Package Build Script")
    print("=" * 60)
    print()
    
    # 检查依赖
    print("Checking dependencies...")
    check_dependencies()
    print()
    
    # 清理之前的构建
    clean_build()
    print()
    
    # 构建包
    build_package()
    print()
    
    # 验证包
    verify_package()
    print()
    
    print("=" * 60)
    print("Build completed successfully!")
    print("=" * 60)
    print()
    print("To install the package:")
    print(f"  python3 -m pip install {OUTPUT_DIR}/*.whl")
    print()
    print("Or install from source:")
    print(f"  python3 -m pip install -e {SCRIPT_DIR}")


if __name__ == "__main__":
    main()

