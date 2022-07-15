FROM python:3.10

RUN apt-get update; \
    apt-get upgrade -y; \
    apt-get install -y \
        gdal-bin \
        sqlite3 \
        curl \
        python3-pip \
        git \
        build-essential \
        nodejs \
        npm

WORKDIR /app
COPY . /app

RUN python -m pip install --upgrade pip
RUN python -m pip install pip-tools
RUN python -m piptools sync requirements/requirements.txt requirements/dev-requirements.txt
RUN	npm install


ENV FLASK_CONFIG=config.DevelopmentConfig
ENV FLASK_APP=application.wsgi:app
ENV FLASK_ENV=development

CMD flask db upgrade && flask run -h 0.0.0.0 -p 5000
