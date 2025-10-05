import ballerina/http;
import ballerina/sql;
// import ballerina/io;
import ballerinax/mysql;

// Configuration
configurable string USER = "root";
configurable string PASSWORD = "muddysituation";
configurable string HOST = "localhost";
configurable int PORT = 3306;
configurable string DATABASE = "ticketingdb";

// Database client
final mysql:Client db = check new(
    host = HOST,
    user = USER,
    password = PASSWORD,
    port = PORT,
    database = DATABASE
);

// Data types
type Passenger record {
    int? passengerID;
    string firstName;
    string lastName;
    string email;
    string passwordHash;
};

type RegisterRequest record {
    string firstName;
    string lastName;
    string email;
    string password;
};

type LoginRequest record {
    string email;
    string password;
};

type Ticket record {
    int ticketID;
    string routeName;
    string routeDescription;
    string status;
};

type CountRecord record {
    int count;
};
