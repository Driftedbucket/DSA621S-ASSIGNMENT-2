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
final k:Producer ticketProducer = check new(KAFKA_BOOTSTRAP);

// Consumer for payments.processed
final k:Consumer paymentConsumer = check new(KAFKA_BOOTSTRAP, {
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

// Helper function to get current timestamp
function getCurrentTimestamp() returns string {
    return "2025-10-05T12:00:00Z"; // Placeholder - use ballerina/time for actual timestamp
}
