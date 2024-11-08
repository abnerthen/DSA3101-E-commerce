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

# Expose app port (adjust based on your app's requirement)
EXPOSE 8050

# Run the app (adjust the command for your specific app)
CMD ["python", "app.py"]
