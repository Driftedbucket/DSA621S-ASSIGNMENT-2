# Use the official Ballerina base image
FROM ballerina/ballerina:2201.12.7

# Set working directory
WORKDIR /home/ballerina

# Copy project files
COPY ./notification_service/Ballerina.toml .
COPY ./notification_service/Config.toml* ./
COPY ./notification_service/main.bal .
COPY ./libs ./libs

# Pull all dependencies (MySQL + Kafka connectors)
RUN bal pull ballerinax/mysql && \
    bal pull ballerinax/kafka

# # Add environment variables for database connection
# # These values are the ones your Ballerina config expects
# ENV HOST=mysql
# ENV USER=root
# ENV PASSWORD=muddysituation
# ENV PORT=3306
# ENV DATABASE=ticketingdb

# # Build the project (after dependencies are available)
# RUN bal build --offline=false

#RUN bal build 


# Expose the service port (change based on your service)
EXPOSE 8085

# Run the service
CMD ["bal", "run"]
