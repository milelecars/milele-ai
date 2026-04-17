FROM mcr.microsoft.com/playwright/python:v1.58.0-jammy

WORKDIR /app

# Xvfb provides a virtual display so Chromium can run non-headless on Linux.
# Imperva flags headless Chrome; visible-mode (even under Xvfb) passes.
RUN apt-get update \
 && apt-get install -y --no-install-recommends xvfb \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV DUBIZZLE_HEADLESS=0 \
    PYTHONUNBUFFERED=1

CMD ["xvfb-run", "-a", "--server-args=-screen 0 1920x1080x24", \
     "python", "main.py", "--source", "dubizzle"]
