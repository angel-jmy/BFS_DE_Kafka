FROM python:3.11

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy Python scripts
COPY src/*.py /app/
RUN echo "After copy:" && ls -la

# Keep container running
CMD ["tail", "-f", "/dev/null"]
