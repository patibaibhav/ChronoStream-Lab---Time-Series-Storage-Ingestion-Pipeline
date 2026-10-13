FROM python:3.13-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.lock ./requirements.lock
RUN python -m pip install --no-cache-dir -r requirements.lock

COPY pyproject.toml README.md ./
COPY app ./app
RUN python -m pip install --no-deps .

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
