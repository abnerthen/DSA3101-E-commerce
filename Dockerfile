# Use Python as base image
FROM python:3.9

# Install jupyter and nbconvert for running notebooks
RUN pip install jupyter nbconvert

# Install the Google Cloud BigQuery library
RUN pip install -r "Data Cleaning/requirements.txt"

# Copy any scripts needed for querying
COPY run_query.py /app/run_query.py

# Set the working directory
WORKDIR /app

# Command to run when the container starts
CMD ["python", "run_query.py"]
