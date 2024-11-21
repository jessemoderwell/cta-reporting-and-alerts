FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the Python service code
COPY src/ .

# Set the default command to run your Python application
CMD ["python", "-u", "main.py"]