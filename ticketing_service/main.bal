import ballerina/http;
import ballerina/sql;
import ballerina/io;
import ballerinax/mysql;
import ballerinax/kafka as k;

configurable string HOST = ?;
configurable string DATABASE = "ticketingdb";
configurable string USER = "root";
configurable string PASSWORD = "muddysituation";
configurable int PORT = 3306;

configurable string BALLERINA_KAFKA_BOOTSTRAP = ?;

final mysql:Client db = check new(HOST, DATABASE, USER, PASSWORD, PORT);
final k:Producer ticketProducer = check new(BALLERINA_KAFKA_BOOTSTRAP);

// Consumer for payments.processed
final k:Consumer paymentConsumer = check new(BALLERINA_KAFKA_BOOTSTRAP, {
    groupId: "ticketing-group",
    topics: ["payments.processed"]
});

listener http:Listener ticketListener = new(8083);

// Simple ticket record used locally
type TicketRecord record {
    int ticketID;
    int passengerID;
    int tripID;
    string status;
    string dateCreated;
};

type PaymentEvent record {
    int ticketID;
    string status;  // "SUCCESS" or "FAILED"
    decimal? amount;
    string? paymentMethod;
};
// Initialize Kafka consumer listener
function init() returns error? {
    io:println("🎫 Ticketing Service starting...");
    io:println("📡 Listening for payment events on Kafka topic: payments.processed");
    
    // Start consuming payment events in a separate worker
    worker PaymentProcessor {
        error? result = consumePaymentEvents();
        if result is error {
            io:println("❌ Payment consumer error: ", result.message());
        }
    }
}

// Consume payment events from Kafka
function consumePaymentEvents() returns error? {
    while true {
        // Fixed: Use the correct return type from poll
        k:BytesConsumerRecord[]|k:Error records = paymentConsumer->poll(1000.0);
        
        if records is k:Error {
            io:println("⚠ Kafka poll error: ", records.message());
            continue;
        }

        if records.length() == 0 {
            continue;
        }

        foreach k:BytesConsumerRecord rec in records {
            // Extract value field (byte[])
            byte[] val = rec.value;
            string payloadStr = check string:fromBytes(val);

            // Parse JSON safely - Fixed: proper json parsing
            json|error jsonRes = payloadStr.fromJsonString();
            if jsonRes is error {
                io:println("⚠ Failed to parse message JSON: ", jsonRes.message());
                continue;
            }
            json payloadJson = jsonRes;

            // Validate/convert to the typed record
            PaymentEvent|error evtRes = payloadJson.cloneWithType(PaymentEvent);
            if evtRes is error {
                io:println("⚠ Invalid payment event shape: ", evtRes.message());
                continue;
            }
            PaymentEvent event = evtRes;

            io:println("💳 Received payment event for ticket: ", event.ticketID, " Status: ", event.status);

            string newStatus = event.status == "SUCCESS" ? "CONFIRMED" : "PAYMENT_FAILED";

            sql:ParameterizedQuery updateQuery = `
                UPDATE Ticket 
                SET status = ${newStatus}
                WHERE ticketID = ${event.ticketID}
            `;

            sql:ExecutionResult result = check db->execute(updateQuery);

            if result.affectedRowCount > 0 {
                io:println("✅ Ticket ", event.ticketID, " updated to status: ", newStatus);

                if newStatus == "CONFIRMED" {
                    json confirmEvent = {
                        ticketID: event.ticketID,
                        status: "CONFIRMED",
                        timestamp: getCurrentTimestamp()
                    };

                    byte[] confirmBytes = confirmEvent.toJsonString().toBytes();

                    error? sendResult = ticketProducer->send({
                        topic: "tickets.confirmed",
                        value: confirmBytes
                    });

                    if sendResult is error {
                        io:println("⚠ Failed to publish ticket.confirmed: ", sendResult.message());
                    } else {
                        io:println("📤 Published ticket.confirmed event for ticket: ", event.ticketID);
                    }
                }
            } else {
                io:println("⚠ Failed to update ticket ", event.ticketID);
            }
        }
    }
}
function getCurrentTimestamp() returns string {
    return "2025-10-05T12:00:00Z"; // Placeholder - use ballerina/time for actual timestamp
}

service /ticketing on ticketListener {

    // Create a ticket (status = created) and produce ticket.requests
    resource function post createTicket(http:Request req) returns http:Response|error {
        // Get JSON payload
        json payload = check req.getJsonPayload();
        map<json> body = check payload.cloneWithType();

        // Extract integers safely
        int passengerID = check body["passengerID"].cloneWithType();
        int tripID = check body["tripID"].cloneWithType();

        // Default status
        string status = "CREATED";

        // Insert into Ticket table
        sql:ParameterizedQuery q = `
            INSERT INTO Ticket (passengerID, tripID, status)
            VALUES (${passengerID}, ${tripID}, ${status})
        `;
        sql:ExecutionResult result = check db->execute(q);

        // Build response
        http:Response res = new;
        if result.affectedRowCount > 0 {
            int|string? ticketID = result.lastInsertId;
            
            // Produce ticket.requests event to Kafka
            if ticketID is int {
                json ticketEvent = {
                    ticketID: ticketID,
                    passengerID: passengerID,
                    tripID: tripID,
                    status: status
                };
                
                byte[] eventBytes = ticketEvent.toJsonString().toBytes();
                error? sendResult = ticketProducer->send({
                    topic: "ticket.requests",
                    value: eventBytes
                });
                
                if sendResult is error {
                    io:println("⚠️ Failed to publish ticket.requests event: ", sendResult.message());
                }
            }
            
            json respPayload = {
                message: "Ticket created successfully",
                ticketID: ticketID is int ? ticketID : 0,
                status: status
            };
            check res.setJsonPayload(respPayload);
            res.statusCode = 201;
        } else {
            json respPayload = { message: "Failed to create ticket" };
            check res.setJsonPayload(respPayload);
            res.statusCode = 500;
        }

        return res;
    }
    
    // Query ticket status
    resource function get ticketStatus(http:Request req) returns http:Response|error {
        // Get all query params as a map
        map<string|string[]> queryParams = req.getQueryParams();

        // Extract ticketID safely
        string|string[]? ticketParam = queryParams["ticketID"];
        string ticketIDStr = "";

        if ticketParam is string {
            ticketIDStr = ticketParam;
        } else if ticketParam is string[] {
            if ticketParam.length() > 0 {
                ticketIDStr = ticketParam[0];
            } else {
                http:Response res = new;
                check res.setJsonPayload({ message: "Missing ticketID query parameter" });
                res.statusCode = 400;
                return res;
            }
        } else {
            http:Response res = new;
            check res.setJsonPayload({ message: "Missing ticketID query parameter" });
            res.statusCode = 400;
            return res;
        }

        // Parse to int
        int ticketID = check int:fromString(ticketIDStr);

        // Query DB
        sql:ParameterizedQuery q = `SELECT status FROM Ticket WHERE ticketID = ${ticketID}`;
        stream<record { string status; }, sql:Error?> resultStream = check db->query(q);

        string? status = ();
        check from var row in resultStream
            do {
                status = row.status;
            };
        check resultStream.close();

        http:Response res = new;
        json payload = { ticketID: ticketID, status: status ?: "not found" };
        check res.setJsonPayload(payload);
        res.statusCode = 200;
        return res;
    }
}
