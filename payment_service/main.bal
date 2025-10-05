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
