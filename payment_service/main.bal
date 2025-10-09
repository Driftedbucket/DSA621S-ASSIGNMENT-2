import ballerina/http;
import ballerina/sql;
import ballerina/io;
import ballerinax/mysql;
import ballerinax/kafka as k;
import ballerina/time;

configurable string HOST = ?;
configurable string DATABASE = "ticketingdb";
configurable string USER = "root";
configurable string PASSWORD = "muddysituation";
configurable int PORT = 3306;

configurable string BALLERINA_KAFKA_BOOTSTRAP = ?;

final mysql:Client db = check new(HOST, DATABASE, USER, PASSWORD, PORT);

// Kafka producer for payments.processed events
final k:Producer paymentProducer = check new(BALLERINA_KAFKA_BOOTSTRAP);

// Kafka consumer for ticket.requests
final k:Consumer ticketConsumer = check new(BALLERINA_KAFKA_BOOTSTRAP, {
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
    string dateCreated;
};

function init() returns error? {
    io:println("Payment Service starting...");
    io:println("Listening for ticket requests on Kafka topic: ticket.requests");

    // Start consuming ticket requests in a worker
    worker TicketProcessor {
        error? result = consumeTicketRequests();
        if result is error {
            io:println("Error consuming ticket requests: ", result.message());
        }
    }
}

// Function to consume ticket requests and process payments
function consumeTicketRequests() returns error? {
    while true {
        // Poll Kafka
        k:BytesConsumerRecord[]|k:Error pollResult = ticketConsumer->poll(1000.0);
        
        if pollResult is k:Error {
            io:println("Kafka poll error: ", pollResult.message());
            continue;
        }

        if pollResult.length() == 0 {
            continue;
        }

        foreach k:BytesConsumerRecord rec in pollResult {
            // Extract value bytes
            byte[] val = rec.value;
            string payloadStr = check string:fromBytes(val);

            // Parse JSON
            json|error jsonResult = payloadStr.fromJsonString();
            if jsonResult is error {
                io:println("Failed to parse JSON: ", jsonResult.message());
                continue;
            }
            json payloadJson = jsonResult;

            // Convert JSON to typed TicketRequest record
            TicketRequest|error ticketResult = payloadJson.cloneWithType(TicketRequest);
            if ticketResult is error {
                io:println("Failed to convert to TicketRequest: ", ticketResult.message());
                continue;
            }
            TicketRequest ticketReq = ticketResult;

            io:println("Received ticket request for ticket: ", ticketReq.ticketID);

            // Process payment
            error? paymentResult = processPayment(ticketReq);
            if paymentResult is error {
                io:println("Payment processing failed: ", paymentResult.message());
            }
     }
  }
}

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

service /payment on paymentListener {
    
    // Health check
    resource function get health() returns json {
        return { status: "Payment Service running" };
    }
    
    // Get payment status for a ticket
    resource function get status/[int ticketID]() returns http:Response|error {
        sql:ParameterizedQuery q = `
            SELECT paymentID, ticketID, status, dateCreated
            FROM Payment
            WHERE ticketID = ${ticketID}
            ORDER BY dateCreated DESC
            LIMIT 1
        `;
        
        PaymentRecord|sql:Error result = db->queryRow(q);
        
        http:Response response = new;
        if result is PaymentRecord {
            json payload = {
                message: "Payment status retrieved",
                payment: result.toJson()
            };
            check response.setJsonPayload(payload);
            response.statusCode = 200;
        } else {
            json payload = { message: "No payment found for this ticket" };
            check response.setJsonPayload(payload);
            response.statusCode = 404;
        }
        
        return response;
   }
}