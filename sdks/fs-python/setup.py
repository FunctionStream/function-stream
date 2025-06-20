from setuptools import setup, find_packages
import os

# Read version from __init__.py
def get_version():
    with open(os.path.join("function_stream", "__init__.py"), "r") as f:
        for line in f:
            if line.startswith("__version__"):
                return line.split("=")[1].strip().strip('"').strip("'")
    return "0.1.0"

# Read README for long description
def get_long_description():
    with open("README.md", "r", encoding="utf-8") as f:
        return f.read()

setup(
    name="function-stream",
    version=get_version(),
    packages=find_packages(),
    install_requires=[
        "pulsar-client>=3.0.0",
        "pyyaml>=6.0",
        "aiohttp>=3.8.0",
        "pydantic>=2.0.0"
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-asyncio>=0.21.0",
            "black>=22.0.0",
            "flake8>=5.0.0",
        ]
    },
    author="FunctionStream Org",
    author_email="",
    description="FunctionStream SDK is a powerful Python library for building and deploying serverless streaming functions that runs on Function Stream platform.",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    url="https://github.com/functionstream/function-stream",
    project_urls={
        "Bug Tracker": "https://github.com/functionstream/function-stream/issues",
        "Documentation": "https://github.com/functionstream/function-stream/tree/main/sdks/fs-python",
        "Source Code": "https://github.com/functionstream/function-stream",
    },
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Distributed Computing",
    ],
    python_requires=">=3.9",
    keywords="serverless, functions, pulsar, event-driven, streaming",
    license="Apache License 2.0",
    zip_safe=False,
)
