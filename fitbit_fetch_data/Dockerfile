ARG BUILD_FROM
FROM $BUILD_FROM

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Copy all required files
COPY requirements.txt /app/
COPY Fitbit_Fetch.py /app/
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
