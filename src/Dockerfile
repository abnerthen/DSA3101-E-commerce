# Use Python image
FROM python:3.12-slim

# Set working directory to the app folder
WORKDIR /app

COPY requirements.txt /app/

# Install app dependencies (adjust the requirements file if needed)
RUN pip install -r requirements.txt

# Copy app files into the container
COPY src/ .

# copy .env 
COPY ../.env /app/.env

# RUN python app_data.py

# Create the wait-for-data.sh script
RUN echo '#!/bin/bash\n\
while [ ! -f /data/expected_output_file ]; do\n\
  echo "Waiting for data...";\n\
  sleep 5;\n\
done' > /wait-for-data.sh

# Make the script executable
RUN chmod +x /wait-for-data.sh

# Expose app port (adjust based on your app's requirement)
EXPOSE 8050

# Run the app (adjust the command for your specific app)
CMD ["/bin/bash", "-c", "/wait-for-data.sh && python app.py"]