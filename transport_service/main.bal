import ballerina/http;
import ballerina/io;
import ballerina/sql;
import ballerinax/mysql;
import ballerinax/kafka as k;

configurable string USER = "root";
configurable string PASSWORD = "muddysituation";
configurable string HOST = "localhost";
configurable int PORT = 3306;
configurable string DATABASE = "ticketingdb";

configurable string KAFKA_BOOTSTRAP = "localhost:9092";

final k:Producer scheduleProducer = check new(KAFKA_BOOTSTRAP);

final mysql:Client db = check new(HOST, DATABASE, USER, PASSWORD, PORT);

// Data types
type Route record {
    int? routeID;
    string routeName;
    string? routeDescription;
};

type Trip record {
    int? tripID;
    int routeID;
    string departureTime;
    string arrivalTime;
    string status;
};

type RouteRequest record {
    string routeName;
    string? routeDescription;
};

type TripRequest record {
    int routeID;
    string departureTime;
    string arrivalTime;
    string? status;
};

type ScheduleUpdateRequest record {
    int? route_id;
    string? description;
    string? start_time;
    string? end_time;
};

// HTTP Listener
listener http:Listener transportListener = new(8082);
