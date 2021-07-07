FROM python:3.8

# working directory Path
WORKDIR /app

# Copying the local code to container image.
COPY Publisher.py .

COPY requirements.txt .

# install dependencies
RUN pip3 install -r requirements.txt

# Runs container start
CMD ["python", ".publisher.py"]



 