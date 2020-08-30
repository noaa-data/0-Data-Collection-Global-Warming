# Name the image when running the build:
# docker build -t global-warming-data:0.1 .

FROM python:3.7-buster
ENV WEBSCRAPE_DB "/home/app"
ENV PYTHONPATH "${PYTHONPATH}:/home/app"
WORKDIR /home/app
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

ENV TEST_PREFECT "True"
CMD ["python"]