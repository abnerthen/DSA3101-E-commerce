# Base image with Jupyter
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /project

# Copy requirements.txt and install dependencies
COPY requirements.txt /project/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project directory into the container
COPY . /project

# Install jupyter nbconvert for running notebooks
RUN pip install --no-cache-dir nbconvert

# Command to run all notebooks in Subgroup_A and Subgroup_B
CMD find ./Subgroup_A ./Subgroup_B -name "*.ipynb" -exec jupyter nbconvert --to notebook --execute --inplace {} \;
