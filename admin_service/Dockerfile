# Use the official Ballerina base image
FROM ballerina/ballerina:2201.12.7

# Set working directory
WORKDIR /home/ballerina

# Copy project files
COPY ./admin_service/Ballerina.toml .
COPY ./admin_service/Config.toml* ./
COPY ./admin_service/main.bal .

COPY ./libs ./libs

# Pull all dependencies (this ensures MySQL + Kafka connectors are installed)
RUN bal pull ballerinax/mysql && \
    bal pull ballerinax/kafka


# #Setting the docker environment variables. If you expect docker to set them(?) it wont
# ENV HOST=mysql
# ENV USER=root
# ENV PASSWORD=muddysituation
# ENV PORT=3306
# ENV DATABASE=ticketingdb 

# # Build the project
# RUN bal build --offline=false

#RUN bal build 


# Expose your service port
EXPOSE 8086

# Run the service
CMD ["bal", "run"]
