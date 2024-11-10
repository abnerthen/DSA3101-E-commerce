# Base image with Jupyter
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /project

# Create the wait-for-data script
RUN echo '#!/bin/bash\n\
while [ ! -f /data/expected_output_file ]; do\n\
  echo "Waiting for data...";\n\
  sleep 5;\n\
done' > /wait-for-data.sh

# Make the script executable
RUN chmod +x /wait-for-data.sh

# Copy requirements.txt and install dependencies
COPY requirements.txt /project/
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project directory into the container
COPY . /project

# Command to run all notebooks in Subgroup_A and Subgroup_B
CMD ["/bin/bash", "-c", "/wait-for-data.sh && find ./Subgroup_A ./Subgroup_B -name "*.ipynb" -exec jupyter nbconvert --to notebook --execute --inplace {} \;"]
