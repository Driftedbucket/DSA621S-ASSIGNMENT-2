FROM ballerina/ballerina:2201.12.7

WORKDIR /home/ballerina

COPY Ballerina.toml .
COPY main.bal .
COPY Config.toml* ./

RUN bal build

EXPOSE 8086

CMD ["bal", "run"]
