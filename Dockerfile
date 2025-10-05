 # Use a modern, slim version of Python as the base image
FROM python:3.11-slim-bookworm

# Set environment variables for Python to run smoothly
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Set the working directory inside the container
WORKDIR /app

# Install system packages like ffmpeg (useful for getting video metadata later)
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy and install Python packages efficiently
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application's code into the container
COPY . .

# Create a non-root user for better security
RUN useradd --create-home appuser
RUN chown -R appuser:appuser /app

# Switch to the non-root user
USER appuser

# The command that will be run when the container starts
CMD ["python", "main.py"]
