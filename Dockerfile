FROM python:3.7.9

ADD weather_reader.py requirements.txt /

RUN pip install -r requirements.txt

CMD python weather_reader.py