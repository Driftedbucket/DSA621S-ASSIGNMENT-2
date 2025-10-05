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

service /admin on adminListener {
    
    // Admin login
    resource function post login(http:Request req) returns http:Response|error {
        json jsonPayload = check req.getJsonPayload();
        LoginRequest body = check jsonPayload.cloneWithType(LoginRequest);
        
        sql:ParameterizedQuery q = `
            SELECT COUNT(*) AS count 
            FROM Admin 
            WHERE email = ${body.email} AND passwordHash = ${body.password}
        `;
        
        CountRecord result = check db->queryRow(q);
        
        http:Response response = new;
        if result.count > 0 {
            json payload = {
                message: "Admin login successful",
                authenticated: true
            };
            check response.setJsonPayload(payload);
            response.statusCode = 200;
        } else {
            json payload = {
                message: "Invalid admin credentials",
                authenticated: false
            };
            check response.setJsonPayload(payload);
            response.statusCode = 401;
        }
        
        return response;
    }
    
    // Create route
    resource function post routes(http:Request req) returns http:Response|error {
        json jsonPayload = check req.getJsonPayload();
        RouteRequest body = check jsonPayload.cloneWithType(RouteRequest);
        
        string routeName = body.routeName;
        string routeDescription = body.routeDescription ?: "";
        
        sql:ParameterizedQuery q = `
            INSERT INTO Route (routeName, routeDescription)
            VALUES (${routeName}, ${routeDescription})
        `;
        
        sql:ExecutionResult result = check db->execute(q);
        
        http:Response response = new;
        if result.affectedRowCount > 0 {
            int|string? routeID = result.lastInsertId;
            json payload = {
                message: "Route created successfully",
                routeID: routeID is int ? routeID : 0
            };
            check response.setJsonPayload(payload);
            response.statusCode = 201;
        } else {
            json payload = { message: "Failed to create route" };
            check response.setJsonPayload(payload);
            response.statusCode = 500;
        }
        
        return response;
    }
    
    // Get all routes
    resource function get routes() returns http:Response|error {
        sql:ParameterizedQuery q = `SELECT * FROM Route`;
        
        
        stream<Route, sql:Error?> resultStream = check db->query(q);
        Route[] routes = check from Route route in resultStream select route;
        check resultStream.close();
        
        http:Response response = new;
        json payload = {
            message: "Routes retrieved successfully",
            routes: <json>routes
        };
        check response.setJsonPayload(payload);
        response.statusCode = 200;
        
        return response;
    }
    
    // Update route
    resource function put routes/[int routeID](http:Request req) returns http:Response|error {
        json jsonPayload = check req.getJsonPayload();
        RouteRequest body = check jsonPayload.cloneWithType(RouteRequest);
        
        sql:ParameterizedQuery q = `
            UPDATE Route 
            SET routeName = ${body.routeName}, 
                routeDescription = ${body.routeDescription ?: ""}
            WHERE routeID = ${routeID}
        `;
        
        sql:ExecutionResult result = check db->execute(q);
        
        http:Response response = new;
        if result.affectedRowCount > 0 {
            json payload = { message: "Route updated successfully" };
            check response.setJsonPayload(payload);
            response.statusCode = 200;
        } else {
            json payload = { message: "Route not found or no changes made" };
            check response.setJsonPayload(payload);
            response.statusCode = 404;
        }
        
        return response;
    }
    
    // Delete route
    resource function delete routes/[int routeID]() returns http:Response|error {
        sql:ParameterizedQuery q = `DELETE FROM Route WHERE routeID = ${routeID}`;
        sql:ExecutionResult result = check db->execute(q);
        
        http:Response response = new;
        if result.affectedRowCount > 0 {
            json payload = { message: "Route deleted successfully" };
            check response.setJsonPayload(payload);
            response.statusCode = 200;
        } else {
            json payload = { message: "Route not found" };
            check response.setJsonPayload(payload);
            response.statusCode = 404;
        }
        
        return response;
    }
    




