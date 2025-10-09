import ballerina/http;
import ballerina/sql;
import ballerina/io;
import ballerinax/mysql;
import ballerinax/kafka as k;

// Config
configurable string HOST = ?;
configurable string DATABASE = "ticketingdb";
configurable string USER = "root";
configurable string PASSWORD = "muddysituation";
configurable int PORT = 3306;

configurable string BALLERINA_KAFKA_BOOTSTRAP = ?;

// DB client
final mysql:Client db = check new(HOST, DATABASE, USER, PASSWORD, PORT);

// Kafka consumers (use BytesConsumerRecord because producers send bytes)
final k:Consumer scheduleConsumer = check new(BALLERINA_KAFKA_BOOTSTRAP, {
    groupId: "notification-schedule-group",
    topics: ["schedule.updates"]
});

final k:Consumer ticketConsumer = check new(BALLERINA_KAFKA_BOOTSTRAP, {
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
// Init: spawn two workers for consumers
function init() returns error? {
    io:println("🔔 Notification Service starting...");
    io:println("Listening for schedule updates and ticket confirmations");

    // worker for schedule updates
    worker ScheduleProcessor {
        error? res = consumeScheduleUpdates();
        if res is error {
            io:println("❌ Schedule consumer error: ", res.message());
        }
    }

    // worker for ticket confirmations
    worker TicketProcessor {
        error? res = consumeTicketConfirmations();
        if res is error {
            io:println("❌ Ticket consumer error: ", res.message());
        }
    }
}

// Consume schedule updates from Kafka
function consumeScheduleUpdates() returns error? {
    while true {
        // explicit typed poll (avoids inference issues)
        k:BytesConsumerRecord[]|k:Error records = scheduleConsumer->poll(1000.0);
        if records is k:Error {
            io:println("⚠️ Kafka poll error (schedule): ", records.message());
            continue;
        }

        if records.length() == 0 {
            continue;
        }

        foreach k:BytesConsumerRecord rec in records {
            // decode bytes -> string
            byte[] val = rec.value;
            string payloadStr = check string:fromBytes(val);

            // ---------------------------
            // <-- use string.fromJsonString() here
            // ---------------------------
            var jsonParseRes = payloadStr.fromJsonString();
            if jsonParseRes is error {
                io:println("⚠️ Failed to parse schedule JSON: ", jsonParseRes.message());
                continue;
            }
            json payloadJson = jsonParseRes;
            // ---------------------------

            // ensure it's a map so we can pull fields
            if !(payloadJson is map<json>) {
                io:println("⚠️ Unexpected schedule payload shape, skipping");
                continue;
            }
            map<json> m = payloadJson;

            // Extract route_id (optional)
            int routeId = 0;
            if m["route_id"] is int {
                routeId = <int> m["route_id"];
            } else if m["route_id"] is string {
                var conv = int:fromString(<string> m["route_id"]);
                if conv is int {
                    routeId = conv;
                } else {
                    // leave as 0 (no route filter)
                }
            }

            string description = "Route schedule has been updated";
            if m["description"] is string {
                description = <string> m["description"];
            }

            io:println("📣 Received schedule update for route: ", routeId, " - ", description);

            // find affected passengers (distinct)
            sql:ParameterizedQuery passengerQuery = `
                SELECT DISTINCT p.passengerID
                FROM Passenger p
                JOIN Ticket t ON p.passengerID = t.passengerID
                JOIN Trip tr ON t.tripID = tr.tripID
                WHERE tr.routeID = ${routeId}
            `;

            stream<record { int passengerID; }, sql:Error?> passengerStream = check db->query(passengerQuery);

            check from var passenger in passengerStream
                do {
                    int pid = passenger.passengerID;
                    string message = "Schedule Update: " + description;
                    error? notifyErr = createNotification(pid, message);
                    if notifyErr is error {
                        io:println("⚠️ Failed to create notification for passenger ", pid, ": ", notifyErr.message());
                    }
                };
            check passengerStream.close();
        }
    }
}

// Consume ticket.confirmed events from Kafka
function consumeTicketConfirmations() returns error? {
    while true {
        k:BytesConsumerRecord[]|k:Error records = ticketConsumer->poll(1000.0);
        if records is k:Error {
            io:println("⚠️ Kafka poll error (tickets): ", records.message());
            continue;
        }

        if records.length() == 0 {
            continue;
        }

        foreach k:BytesConsumerRecord rec in records {
        byte[] val = rec.value;
        string payloadStr = check string:fromBytes(val);

        // ---------- Replace json:fromString(...) with string.fromJsonString() ----------
        var jsonParseRes = payloadStr.fromJsonString();
        if jsonParseRes is error {
            io:println("⚠️ Failed to parse ticket JSON: ", jsonParseRes.message());
            continue;
        }
        json payloadJson = jsonParseRes;
        // ---------------------------------------------------------------------------

        if !(payloadJson is map<json>) {
            io:println("⚠️ Unexpected ticket payload shape, skipping");
            continue;
        }
        map<json> m = payloadJson;

            // Extract ticketID safely
            int ticketID;
            if m["ticketID"] is int {
                ticketID = <int> m["ticketID"];
            } else if m["ticketID"] is string {
                var conv = int:fromString(<string> m["ticketID"]);
                if conv is int {
                    ticketID = conv;
                } else {
                    io:println("⚠️ Invalid ticketID in ticket.confirmed message: ", m["ticketID"].toString());
                    continue;
                }
            } else {
                io:println("⚠️ Missing ticketID in ticket.confirmed message");
                continue;
            }

            io:println("✅ Received ticket confirmation for ticket: ", ticketID);

            // Lookup passenger for the ticket
            TicketOwner? owner = check db->queryRow(`
                SELECT passengerID FROM Ticket WHERE ticketID = ${ticketID}
            `, TicketOwner);

            if owner is TicketOwner {
                string message = "Your ticket #" + ticketID.toString() + " has been confirmed and paid successfully!";
                error? nerr = createNotification(owner.passengerID, message);
                if nerr is error {
                    io:println("⚠️ Failed to create notification: ", nerr.message());
                }
            } else {
                io:println("⚠️ No owner found for ticket ", ticketID);
            }
        }
    }
}
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

