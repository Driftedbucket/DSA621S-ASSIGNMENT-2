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
