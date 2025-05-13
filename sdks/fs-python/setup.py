from setuptools import setup, find_packages

setup(
    name="fs-sdk",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pulsar-client>=3.0.0",
        "pyyaml>=6.0",
        "aiohttp>=3.8.0",
    ],
    author="FuncionStream Org",
    author_email="your.email@example.com",
    description="A simple RPC service SDK that allows users to focus on their core business logic",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/fs-sdk",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
) 