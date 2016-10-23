FROM python:3.5-alpine
RUN mkdir /app
WORKDIR /app
COPY requirements.txt /app
RUN apk add --no-cache zlib zlib-dev git vim curl bash && \
    pip install -r requirements.txt
COPY trafficdumper.py /app
