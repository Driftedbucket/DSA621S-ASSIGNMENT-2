import ballerina/http;
import ballerina/sql;
import ballerina/io;
import ballerinax/mysql;
import ballerinax/kafka as k;
import ballerina/time;

configurable string HOST = "localhost";
configurable string DATABASE = "ticketingdb";
configurable string USER = "root";
configurable string PASSWORD = "muddysituation";
configurable int PORT = 3306;

configurable string KAFKA_BOOTSTRAP = "localhost:9092";

final mysql:Client db = check new(HOST, DATABASE, USER, PASSWORD, PORT);

// Kafka producer for payments.processed events
final k:Producer paymentProducer = check new(KAFKA_BOOTSTRAP);

// Kafka consumer for ticket.requests
final k:Consumer ticketConsumer = check new(KAFKA_BOOTSTRAP, {
    groupId: "payment-group",
    topics: ["ticket.requests"]
});

listener http:Listener paymentListener = new(8084);

// Record types
type TicketRequest record {
    int ticketID;
    int passengerID;
    int tripID;
    string status;
};

type PaymentEvent record {
    int ticketID;
    string status;  // "SUCCESS" or "FAILED"
    string timestamp;
};

type PaymentRecord record {
    int? paymentID;
    int ticketID;
    string status;
    string dateCreated;
};

function processPayment(TicketRequest ticketReq) returns error? {
    // Create payment record in database
    sql:ParameterizedQuery insertQuery = `
        INSERT INTO Payment (ticketID, status)
        VALUES (${ticketReq.ticketID}, 'pending')
    `;
    
    sql:ExecutionResult insertResult = check db->execute(insertQuery);
    int|string? paymentID = insertResult.lastInsertId;
    
    if !(paymentID is int) {
        io:println("Failed to create payment record");
        return;
    }

    // Simulate payment processing
    boolean paymentSuccess = simulatePayment(ticketReq.ticketID);
    string paymentStatus = paymentSuccess ? "completed" : "failed";
    string eventStatus = paymentSuccess ? "SUCCESS" : "FAILED";

    // Update payment record
    sql:ParameterizedQuery updateQuery = `
        UPDATE Payment 
        SET status = ${paymentStatus}
        WHERE paymentID = ${paymentID}
    `;
    
    sql:ExecutionResult updateResult = check db->execute(updateQuery);
    
    if updateResult.affectedRowCount > 0 {
        io:println("Payment ", paymentID, " processed: ", paymentStatus);
    }

    // Create payment event
    PaymentEvent event = {
        ticketID: ticketReq.ticketID,
        status: eventStatus,
        timestamp: getCurrentTimestamp()
    };

    // Publish payment result to payments.processed topic
    json eventJson = event.toJson();
    byte[] eventBytes = eventJson.toJsonString().toBytes();

    error? sendResult = paymentProducer->send({
        topic: "payments.processed",
        value: eventBytes
    });

    if sendResult is error {
        io:println("Failed to publish payment event: ", sendResult.message());
    } else {
        io:println("Published payment event for ticket: ", event.ticketID, " with status: ", event.status);
    }
}

// Simulate payment processing (90% success rate)
function simulatePayment(int ticketID) returns boolean {
    // Simulate random payment success/failure
    // In production, this would integrate with actual payment gateway
    int random = ticketID % 10;
    return random != 0; // 90% success rate
}

// Get current timestamp
function getCurrentTimestamp() returns string {
    time:Utc currentTime = time:utcNow();
    return time:utcToString(currentTime);
}
