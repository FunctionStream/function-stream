FROM functionstream/fs-python-base:latest

WORKDIR /function

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY main.py .

RUN chmod +x main.py

CMD ["python", "main.py"]