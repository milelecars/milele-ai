FROM python:3.11-slim

WORKDIR /app

# Bot doesn't need playwright/chromium/xvfb — keep the image lean.
RUN pip install --no-cache-dir \
        requests==2.31.0 \
        supabase==2.28.3 \
        tenacity==8.2.3 \
        python-dotenv==1.0.1 \
        google-generativeai==0.8.3

COPY bot.py ./
COPY utils/ ./utils/

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

CMD ["python", "bot.py"]
