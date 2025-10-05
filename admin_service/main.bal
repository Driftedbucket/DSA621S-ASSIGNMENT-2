import ballerina/http;
import ballerina/sql;
import ballerina/io;
import ballerinax/mysql;
import ballerinax/kafka as k;

configurable string HOST = "localhost";
configurable string DATABASE = "ticketingdb";
configurable string USER = "root";
configurable string PASSWORD = "muddysituation";
configurable int PORT = 3306;

configurable string KAFKA_BOOTSTRAP = "localhost:9092";

final mysql:Client db = check new(HOST, DATABASE, USER, PASSWORD, PORT);
final k:Producer scheduleProducer = check new(KAFKA_BOOTSTRAP);

listener http:Listener adminListener = new(8086);

// Record types
type Admin record {
    int? adminID;
    string firstName;
    string lastName;
    string email;
    string passwordHash;
};

type LoginRequest record {
    string email;
    string password;
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

type DisruptionRequest record {
    int route_id;
    string description;
    string start_time;
    string end_time;
};

type TicketSalesReport record {
    int routeID;
    string routeName;
    int totalTickets;
    int createdTickets;
    int paidTickets;
    int validatedTickets;
    int expiredTickets;
};

type CountRecord record {
    int count;
};
type Route record {
    int routeID;
    string routeName;
    string? routeDescription;
};

type TripDetail record {
    int tripID;
    int routeID;
    string routeName;
    string departureTime;
    string arrivalTime;
    string status;
        };
type TripStatusUpdate record {
    string status;
};
