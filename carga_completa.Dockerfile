FROM python:3.9

WORKDIR /app

COPY ./requirements.txt .

COPY ./executa_carga_completa.py .

COPY ./carga_completa/ .

RUN pip install -r requirements.txt

CMD [ "python", "executa_carga_completa.py" ]

