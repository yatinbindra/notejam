FROM python:2.7

WORKDIR /notejam

COPY . /notejam
EXPOSE 9000
RUN pip install -r requirements.txt 
WORKDIR /notejam/notejam
RUN python2 manage.py migrate && python2 manage.py runserver 0.0.0.0:9000
