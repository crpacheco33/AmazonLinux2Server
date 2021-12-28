FROM 369963911650.dkr.ecr.us-west-1.amazonaws.com/ecr-python-base:latest

WORKDIR /var/www/server
USER visibly

COPY server.requirements.txt ./

RUN pip install --upgrade pip
RUN pip install -r server.requirements.txt

COPY . ./
ENV PYTHONPATH /var/www/server/server/
ENTRYPOINT ["python", "-m", "server.main", "main.py"]
