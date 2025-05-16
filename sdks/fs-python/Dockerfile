FROM python:3.13-slim

WORKDIR /function

# Copy the SDK files
COPY . /function/

# Install the SDK
RUN pip install .

# Set the default command
CMD ["python", "/function/function.py"] 