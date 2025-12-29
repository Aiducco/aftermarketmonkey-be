FROM python:3.11

# Set the working directory
WORKDIR /app

# Install necessary packages including cron
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    gnupg \
    cron \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt /app/
RUN pip install -r requirements.txt

# Copy application code
COPY . /app/
RUN chmod +x ./resources/log-deleter.sh

# Copy and set up crontab
COPY crontab /etc/cron.d/crontab
RUN chmod 0644 /etc/cron.d/crontab
RUN crontab /etc/cron.d/crontab

# Create log directory for cron
RUN mkdir -p /var/log && touch /var/log/cron.log

# Copy entrypoint script
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

# Set environment variables
ENV PYTHONUNBUFFERED 1
ENV DJANGO_SETTINGS_MODULE=settings

# Use entrypoint script to start cron and then the application
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["gunicorn", "--workers", "3", "--bind", "0.0.0.0:8000", "wsgi:application"]
