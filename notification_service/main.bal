import ballerina/http;
import ballerina/sql;
import ballerina/io;
import ballerinax/mysql;
import ballerinax/kafka as k;



// Config
configurable string HOST = "localhost";
configurable string DATABASE = "ticketingdb";
configurable string USER = "root";
configurable string PASSWORD = "muddysituation";
configurable int PORT = 3306;

configurable string KAFKA_BOOTSTRAP = "localhost:9092";

// DB client
final mysql:Client db = check new(HOST, DATABASE, USER, PASSWORD, PORT);

// Kafka consumers (use BytesConsumerRecord because producers send bytes)
final k:Consumer scheduleConsumer = check new(KAFKA_BOOTSTRAP, {
    groupId: "notification-schedule-group",
    topics: ["schedule.updates"]
});

final k:Consumer ticketConsumer = check new(KAFKA_BOOTSTRAP, {
    groupId: "notification-ticket-group",
    topics: ["tickets.confirmed"]
});

// HTTP listener
listener http:Listener notificationListener = new(8085);

// Types (module level)
type NotificationRecord record {
    int? notificationID;
    int passengerID;
    string message;
    string dateCreated;
};

type ScheduleUpdate record {
    int? route_id;
    string? description;
    string? start_time;
    string? end_time;
};

type TicketConfirmed record {
    int ticketID;
    string status;
    string timestamp;
};

type NotificationRequest record {
    int passengerID;
    string message;
};
type TicketOwner record { 
    int passengerID; 
};

// Create notification helper
function createNotification(int passengerID, string message) returns error? {
    sql:ParameterizedQuery q = `
        INSERT INTO Notification (passengerID, message)
        VALUES (${passengerID}, ${message})
    `;
    sql:ExecutionResult res = check db->execute(q);
    if res.affectedRowCount > 0 {
        io:println("📨 Notification created for passenger: ", passengerID);
    }
    return;
}

// HTTP endpoints
service /notification on notificationListener {

    // Get notifications for a passenger
    resource function get notifications/[int passengerID]() returns http:Response|error {
        sql:ParameterizedQuery q = `
            SELECT notificationID, passengerID, message, dateCreated
            FROM Notification
            WHERE passengerID = ${passengerID}
            ORDER BY dateCreated DESC
        `;

        // Query returns generic records, map to NotificationRecord
        stream<record { int notificationID; int passengerID; string message; string dateCreated; }, sql:Error?> s =
            check db->query(q);

        NotificationRecord[] notifications = [];
        check from var r in s do {
            notifications.push({
                notificationID: r.notificationID,
                passengerID: r.passengerID,
                message: r.message,
                dateCreated: r.dateCreated
            });
        };
        check s.close();

        http:Response response = new;
        json payload = {
            message: "Notifications retrieved successfully",
            notifications: <json>notifications
        };
        check response.setJsonPayload(payload);
        response.statusCode = 200;
        return response;
    }

  resource function post sendNotification(http:Request req) returns http:Response|error {
    json payload = check req.getJsonPayload();
    http:Response res = new;

    if !(payload is map<json>) {
        res.setJsonPayload({ message: "Invalid JSON payload (expected object)" });
        res.statusCode = 400;
        return res;
    }
    
    map<json> body = payload;

    // Extract and validate passengerID
    if !(body["passengerID"] is int) {
        if body["passengerID"] is string {
            int|error conv = int:fromString(<string>body["passengerID"]);
            if conv is error {
                res.setJsonPayload({ message: "passengerID must be an integer" });
                res.statusCode = 400;
                return res;
            }
        } else {
            res.setJsonPayload({ message: "passengerID is required" });
            res.statusCode = 400;
            return res;
        }
    }
    
    int pid = body["passengerID"] is int ? <int>body["passengerID"] : check int:fromString(<string>body["passengerID"]);

    // Extract and validate message
    if !(body["message"] is string) {
        res.setJsonPayload({ message: "message is required and must be a string" });
        res.statusCode = 400;
        return res;
    }
    
    string msg = <string>body["message"];

    // Insert notification directly
    sql:ParameterizedQuery insertQ = `INSERT INTO Notification (passengerID, message) VALUES (${pid}, ${msg})`;
    
    _ = check db->execute(insertQ);
    
    io:println("Notification created for passenger: ", pid);
    res.setJsonPayload({ message: "Notification sent successfully" });
    res.statusCode = 201;
    return res;
}

    // Health check
    resource function get health() returns json {
        return { status: "Notification Service running" };
    }
}

