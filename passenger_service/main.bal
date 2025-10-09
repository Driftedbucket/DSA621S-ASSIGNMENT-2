import ballerina/http;
import ballerina/sql;
// import ballerina/io;
import ballerinax/mysql;

// Configuration
configurable string USER = "root";
configurable string PASSWORD = "muddysituation";
configurable string HOST = "mysql";
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

// HTTP Listener
listener http:Listener passengerListener = new(8081);

service /passenger on passengerListener {

    // Register a new passenger
    resource function post register(http:Request req) returns http:Response|error {
        json jsonPayload = check req.getJsonPayload();
        RegisterRequest body = check jsonPayload.cloneWithType(RegisterRequest);

        sql:ParameterizedQuery q = `
            INSERT INTO Passenger (firstName, lastName, email, passwordHash) 
            VALUES (${body.firstName}, ${body.lastName}, ${body.email}, ${body.password})
        `;
        
        sql:ExecutionResult result = check db->execute(q);
        
        http:Response response = new;
        if result.affectedRowCount > 0 {
            int|string? lastId = result.lastInsertId;
            json payload = { 
                message: "Passenger registered successfully!", 
                passengerID: lastId is int ? lastId : 0 
            };
            check response.setJsonPayload(payload);
            response.statusCode = 201;
        } else {
            json payload = { message: "Failed to register passenger" };
            check response.setJsonPayload(payload);
            response.statusCode = 500;
        }
        return response;
    }

    // Login passenger
    resource function post login(http:Request req) returns http:Response|error {
        json jsonPayload = check req.getJsonPayload();
        LoginRequest body = check jsonPayload.cloneWithType(LoginRequest);

        sql:ParameterizedQuery q = `
            SELECT COUNT(*) AS count 
            FROM Passenger 
            WHERE email = ${body.email} AND passwordHash = ${body.password}
        `;
        
        CountRecord result = check db->queryRow(q);
        
        http:Response response = new;
        if result.count > 0 {
            json payload = { 
                message: "Login successful", 
                authenticated: true 
            };
            check response.setJsonPayload(payload);
            response.statusCode = 200;
        } else {
            json payload = { 
                message: "Invalid credentials", 
                authenticated: false 
            };
            check response.setJsonPayload(payload);
            response.statusCode = 401;
        }
        return response;
    }

    // View tickets for a passenger

resource function get tickets/[int passengerID]() returns http:Response|error {
    sql:ParameterizedQuery q = `
        SELECT t.ticketID, tr.routeName, tr.routeDescription, t.status
        FROM Ticket t
        JOIN Trip tp ON t.tripID = tp.tripID
        JOIN Route tr ON tp.routeID = tr.routeID
        WHERE t.passengerID = ${passengerID}
    `;

    // Execute the query and collect results
    stream<Ticket, error?> resultStream = check db->query(q);
    Ticket[] tickets = check from Ticket ticket in resultStream select ticket;
    check resultStream.close();

    // Prepare HTTP response
    http:Response response = new;

    if tickets.length() > 0 {
        json payload = {
            message: "Tickets retrieved successfully",
            tickets: <json>tickets 
        };
        check response.setJsonPayload(payload);
        response.statusCode = 200;
    } else {
        json payload = {
            message: "No tickets found for this passenger",
            tickets: []
        };
        check response.setJsonPayload(payload);
        response.statusCode = 200;
    }

    return response;
}

}
