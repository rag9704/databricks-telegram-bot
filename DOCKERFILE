FROM python:3.10-slim

# Set environment variables
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Create and use a virtual environment
RUN python -m venv $VIRTUAL_ENV

# Install dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy bot code
COPY . .

# Run the bot
CMD ["python", "app.py"]
