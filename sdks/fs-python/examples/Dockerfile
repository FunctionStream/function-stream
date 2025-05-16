FROM functionstream/fs-python-base:latest

WORKDIR /function

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY string_function.py .
COPY config.yaml .
COPY package.yaml .

CMD ["python", "string_function.py"] 