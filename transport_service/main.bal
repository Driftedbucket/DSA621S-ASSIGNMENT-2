import ballerina/http;
import ballerina/io;
import ballerina/sql;
import ballerinax/mysql;
import ballerinax/kafka as k;

configurable string USER = "root";
configurable string PASSWORD = "muddysituation";
configurable string HOST = ?;
configurable int PORT = 3306;
configurable string DATABASE = "ticketingdb";

configurable string KAFKA_BOOTSTRAP = ?;

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

service /transport on transportListener {

    // Add a new route
    resource function post addRoute(http:Request req) returns http:Response|error {
        //RouteRequest body = check req.getjsonPayload(); 
        json jsonPayload = check req.getJsonPayload();
        RouteRequest body = check jsonPayload.cloneWithType(RouteRequest);
    
        string routeName = body.routeName;
        string routeDescription = body.routeDescription ?: "";

        sql:ParameterizedQuery q = `
            INSERT INTO Route (routeName, routeDescription)
            VALUES (${routeName}, ${routeDescription})
        `;
        sql:ExecutionResult res = check db->execute(q);
        int|string? lastId = res.lastInsertId;

        http:Response response = new;
        if lastId is int {
            json payload = { message: "Route added", routeID: lastId };
            check response.setJsonPayload(payload);
            response.statusCode = 201;
            return response;
        } else {
            json payload = { message: "Failed to add route" };
            check response.setJsonPayload(payload);
            response.statusCode = 500;
            return response;
        }
    }

    // Add a new trip
    resource function post addTrip(http:Request req) returns http:Response|error {
        json jsonPayload = check req.getJsonPayload();
        TripRequest body = check jsonPayload.cloneWithType(TripRequest);
        
        int routeID = body.routeID;
        string departure = body.departureTime;
        string arrival = body.arrivalTime;
        string status = body.status ?: "scheduled";

        sql:ParameterizedQuery q = `
            INSERT INTO Trip (routeID, departureTime, arrivalTime, status)
            VALUES (${routeID}, ${departure}, ${arrival}, ${status})
        `;
        sql:ExecutionResult res = check db->execute(q);
        int|string? lastId = res.lastInsertId;

        http:Response response = new;
        if lastId is int {
            json payload = { message: "Trip added", tripID: lastId };
            check response.setJsonPayload(payload);
            response.statusCode = 201;
            return response;
        } else {
            json payload = { message: "Failed to add trip" };
            check response.setJsonPayload(payload);
            response.statusCode = 500;
            return response;
        }
    }

    // Publish a schedule update via Kafka
    resource function post publishSchedule(http:Request req) returns http:Response|error {
        json jsonPayload = check req.getJsonPayload();
        ScheduleUpdateRequest body = check jsonPayload.cloneWithType(ScheduleUpdateRequest);

        // Store disruption in database (optional)
        int routeId = body.route_id ?: 0;
        string description = body.description ?: "";
        string startTime = body.start_time ?: "";
        string endTime = body.end_time ?: "";

        sql:ParameterizedQuery q = `
            INSERT INTO disruptions (route_id, description, start_time, end_time)
            VALUES (${routeId}, ${description}, ${startTime}, ${endTime})
        `;
        sql:ExecutionResult res=check db->execute(q);

        // Produce Kafka event
        json eventPayload = body.toJson();
        string payloadString = eventPayload.toJsonString();
        
        byte[] payloadBytes = payloadString.toBytes();
    
        // Send directly with topic and value
        var sendRes = scheduleProducer->send({
        topic: "schedule.updates",
        value: payloadBytes
    });

    http:Response response = new;
    if sendRes is error {
        io:println("Kafka send failed: ", sendRes.message());
        json errPayload = { message: "Failed to publish schedule update" };
        check response.setJsonPayload(errPayload);
        response.statusCode = 500;
        return response;
    }

    json successPayload = { message: "Schedule update published" };
    check response.setJsonPayload(successPayload);
    response.statusCode = 200;
    return response;
}
}
