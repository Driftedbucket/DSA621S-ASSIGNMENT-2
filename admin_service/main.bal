import ballerina/http;
import ballerina/sql;
import ballerina/io;
import ballerinax/mysql;
import ballerinax/kafka as k;

configurable string HOST = "mysql";
configurable string DATABASE = "ticketingdb";
configurable string USER = "root";
configurable string PASSWORD = "muddysituation";
configurable int PORT = 3306;

configurable string BALLERINA_KAFKA_BOOTSTRAP = "kafka:9092";

final mysql:Client db = check new(
    host = HOST,
    user = USER,
    password = PASSWORD,
    port = PORT,
    database = DATABASE
);
final k:Producer scheduleProducer = check new(BALLERINA_KAFKA_BOOTSTRAP);

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
    

  // Create trip
    resource function post trips(http:Request req) returns http:Response|error {
        json jsonPayload = check req.getJsonPayload();
        TripRequest body = check jsonPayload.cloneWithType(TripRequest);
        
        string status = body.status ?: "scheduled";
        
        sql:ParameterizedQuery q = `
            INSERT INTO Trip (routeID, departureTime, arrivalTime, status)
            VALUES (${body.routeID}, ${body.departureTime}, ${body.arrivalTime}, ${status})
        `;
        
        sql:ExecutionResult result = check db->execute(q);
        
        http:Response response = new;
        if result.affectedRowCount > 0 {
            int|string? tripID = result.lastInsertId;
            json payload = {
                message: "Trip created successfully",
                tripID: tripID is int ? tripID : 0
            };
            check response.setJsonPayload(payload);
            response.statusCode = 201;
        } else {
            json payload = { message: "Failed to create trip" };
            check response.setJsonPayload(payload);
            response.statusCode = 500;
        }
        
        return response;
    }
    
    // Get all trips
    resource function get trips() returns http:Response|error {
        sql:ParameterizedQuery q = `
            SELECT t.*, r.routeName 
            FROM Trip t 
            JOIN Route r ON t.routeID = r.routeID
        `;
        
        stream<TripDetail, sql:Error?> resultStream = check db->query(q);
        TripDetail[] trips = check from TripDetail trip in resultStream select trip;
        check resultStream.close();
        
        http:Response response = new;
        json payload = {
            message: "Trips retrieved successfully",
            trips: <json>trips
        };
        check response.setJsonPayload(payload);
        response.statusCode = 200;
        
        return response;
    }
    
    // Update trip status
    resource function put trips/[int tripID](http:Request req) returns http:Response|error {
        json jsonPayload = check req.getJsonPayload();
        
        
        TripStatusUpdate body = check jsonPayload.cloneWithType(TripStatusUpdate);
        
        sql:ParameterizedQuery q = `
            UPDATE Trip 
            SET status = ${body.status}
            WHERE tripID = ${tripID}
        `;
        
        sql:ExecutionResult result = check db->execute(q);
        
        http:Response response = new;
        if result.affectedRowCount > 0 {
            json payload = { message: "Trip status updated successfully" };
            check response.setJsonPayload(payload);
            response.statusCode = 200;
        } else {
            json payload = { message: "Trip not found" };
            check response.setJsonPayload(payload);
            response.statusCode = 404;
        }

        return response;
    }

    // Publish service disruption
    resource function post disruptions(http:Request req) returns http:Response|error {
        json jsonPayload = check req.getJsonPayload();
        DisruptionRequest body = check jsonPayload.cloneWithType(DisruptionRequest);
        
        // Publish to Kafka
        json event = {
            route_id: body.route_id,
            description: body.description,
            start_time: body.start_time,
            end_time: body.end_time
        };
        
        byte[] eventBytes = event.toJsonString().toBytes();
        var sendResult = scheduleProducer->send({
            topic: "schedule.updates",
            value: eventBytes
        });
        
        http:Response response = new;
        if sendResult is error {
            io:println("Failed to publish disruption: ", sendResult.message());
            check response.setJsonPayload({
                message: "Failed to publish disruption"
            });
            response.statusCode = 500;
        } else {
            io:println("Disruption published for route: ", body.route_id);
            check response.setJsonPayload({
                message: "Disruption published successfully"
            });
            response.statusCode = 200;
        }
        
        return response;
    }
    
    // Get ticket sales report
    resource function get reports/ticketsales() returns http:Response|error {
        sql:ParameterizedQuery q = `
            SELECT 
                r.routeID,
                r.routeName,
                COUNT(t.ticketID) as totalTickets,
                SUM(CASE WHEN t.status = 'created' THEN 1 ELSE 0 END) as createdTickets,
                SUM(CASE WHEN t.status = 'paid' THEN 1 ELSE 0 END) as paidTickets,
                SUM(CASE WHEN t.status = 'validated' THEN 1 ELSE 0 END) as validatedTickets,
                SUM(CASE WHEN t.status = 'expired' THEN 1 ELSE 0 END) as expiredTickets
            FROM Route r
            LEFT JOIN Trip tr ON r.routeID = tr.routeID
            LEFT JOIN Ticket t ON tr.tripID = t.tripID
            GROUP BY r.routeID, r.routeName
        `;
        
        stream<TicketSalesReport, sql:Error?> resultStream = check db->query(q);
        TicketSalesReport[] reports = check from TicketSalesReport report in resultStream 
            select report;
        check resultStream.close();
        
        http:Response response = new;
        json payload = {
            message: "Ticket sales report generated",
            reports: <json>reports
        };
        check response.setJsonPayload(payload);
        response.statusCode = 200;
        
        return response;
    }
    
    // Health check
    resource function get health() returns json {
        return { status: "Admin Service running" };
    }
}


