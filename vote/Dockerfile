# base defines a base stage that uses the official python runtime base image
FROM python:3.11-slim AS base

COPY --from=ghcr.io/fivexl/lprobe:0.1.5 /lprobe /bin/lprobe

VOLUME /tmp
# 1777 means: drwxrwxrwt (anyone can write, but files are protected from deletion by other users)
# RUN mkdir -p /tmp && chmod 1777 /tmp

# Set the application directory
WORKDIR /usr/local/app

# Install our requirements.txt
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# dev defines a stage for development, where it'll watch for filesystem changes
FROM base AS dev
RUN pip install watchdog
ENV FLASK_ENV=development
CMD ["python", "app.py"]

# final defines the stage that will bundle the application for production
FROM base AS final

# Copy our code from the current folder to the working directory inside the container
COPY . .

# Make port 80 available for links and/or publish
EXPOSE 8080

# Define our command to be run when launching the container
CMD ["gunicorn", "app:app", "-b", "0.0.0.0:8080", "--log-file", "-", "--access-logfile", "-", "--workers", "4", "--keep-alive", "0"]
