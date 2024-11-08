FROM python:3.12-slim

WORKDIR /data

COPY ../requirements.txt /data/

RUN pip install --no-cache-dir -r requirements.txt

COPY ./Data/*.py /data/

# copy .env 
COPY ../.env /app/.env

# Ensure all scripts are executable (optional)
RUN chmod +x /data/*.py

# Run all Python scripts in the /app directory
CMD ["bash", "-c", "for file in /data/*.py; do python $file; done"]