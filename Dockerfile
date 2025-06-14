FROM python:3.12-slim

WORKDIR /app

# Copy and install requirements
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy the application code
COPY app/ .

# Install cron
RUN apt-get update && apt-get install -y cron

COPY crontab /etc/cron.d/app-cron

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/app-cron

# Create the log file to be able to run tail
RUN touch /var/log/cron.log

# Copy the model training script
COPY trainingScript.sh /trainingScript.sh
RUN chmod +x /trainingScript.sh

# Creating log file
RUN touch /var/log/training.log
RUN touch /var/log/trainingSchedule.log

# Copy entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 5001

# Start cron and uvicorn using the entrypoint
CMD ["/entrypoint.sh"]
