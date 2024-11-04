# Use Python as base image
FROM python:3.9

# Install the Google Cloud BigQuery library
RUN pip install -r requirements.txt

# Copy any scripts needed for querying
COPY run_query.py /app/run_query.py

# Set the working directory
WORKDIR /app

# Command to run when the container starts
CMD ["python", "run_query.py"]
