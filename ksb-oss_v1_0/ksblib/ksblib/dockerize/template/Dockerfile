FROM python:3.5-slim

# Set working directory.
WORKDIR /app

# Copy the current directory contents into the working directory of container.
ADD . /app

# Copy user-provided libraries and files to modules directory.
COPY /modules /app/modules

# Install needed packages.
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Make port 80 available to outside.
EXPOSE 8080

# Run run.py when the container is loaded.
CMD ["python", "run.py"]