# Use Python image
FROM python:3.9-slim

# Set working directory to the app folder
WORKDIR /usr/src/app

# Copy app files into the container
COPY ./app /usr/src/app

# Install app dependencies (adjust the requirements file if needed)
RUN pip install -r requirements.txt

# Expose app port (adjust based on your app's requirement)
EXPOSE 8050

# Run the app (adjust the command for your specific app)
CMD ["python", "app.py"]
