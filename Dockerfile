# Use Python as base image
FROM python:3.9

WORKDIR /app

COPY requirements.txt .

# Install jupyter and nbconvert for running notebooks
RUN pip install jupyter nbconvert

# Install the Google Cloud BigQuery library
RUN pip install --no-cache-dir -r requirements.txt 

# Copy any scripts needed for querying
COPY "Data Cleaning/" .


# Set the working directory
WORKDIR /app

# Command to run when the container starts
CMD ["python", "run_query.py"]
