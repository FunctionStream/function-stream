from setuptools import setup, find_packages

setup(
    name="fs-sdk",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pulsar-client>=3.0.0",
        "pyyaml>=6.0",
        "aiohttp>=3.8.0",
        "pydantic>=2.0.0"
    ],
    author="FunctionStream Org",
    author_email="",
    description="FunctionStream SDK is a powerful Python library for building and deploying serverless functions that process messages from Apache Pulsar.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/functionstream/function-stream/sdks/fs-python",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
)
