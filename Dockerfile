FROM python:3.11

# Set the working directory
WORKDIR /app

# Install necessary packages
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    gnupg \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt /app/
RUN pip install -r requirements.txt

# Copy application code
COPY . /app/
RUN chmod +x ./resources/log-deleter.sh
# `COPY . /app/` copies whatever __pycache__ directories exist in the build host's local
# checkout (they're gitignored, not tracked, but COPY doesn't care) -- stale .pyc files from an
# old source tree can then get built into the image alongside the real, current .py sources.
# Confirmed live 2026-10-27: a rebuild picked up the new commit's source but PROVIDER_CATALOG
# kept reading from a stale compiled constants.pyc, so a real code change silently didn't take
# effect until the cache was cleared by hand. Strip every __pycache__ at build time so the image
# always compiles fresh from the source it was just given.
RUN find /app -name "__pycache__" -type d -exec rm -rf {} +

# Set environment variables
ENV PYTHONUNBUFFERED 1
ENV DJANGO_SETTINGS_MODULE=settings

# Command to run the application
CMD ["gunicorn", "--workers", "3", "--bind", "0.0.0.0:8000", "wsgi:application"]
