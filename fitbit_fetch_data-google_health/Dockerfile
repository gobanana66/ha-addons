# Base image per architecture. BUILD_ARCH is provided by the Supervisor builder
# (defaults to amd64 for a plain `docker build`). This replaces the deprecated
# build.yaml build_from mapping, which pinned the same tag for every arch.
ARG BUILD_ARCH=amd64
FROM ghcr.io/home-assistant/${BUILD_ARCH}-base-python:3.10-alpine3.18

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Copy all required files
COPY requirements.txt /app/
COPY Google_Health_Fetch.py /app/
COPY run.sh /app/

# Install build dependencies for packages that need to compile
RUN apk add --no-cache libxml2-dev libxslt-dev

# Install requirements
RUN pip install -r requirements.txt

# Setup permissions
RUN chmod +x /app/run.sh

# Do not switch users
# USER appuser

CMD [ "/app/run.sh" ]
