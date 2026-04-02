FROM python:3.11-slim
RUN pip install pandas pyarrow sqlalchemy pg8000 tqdm numpy