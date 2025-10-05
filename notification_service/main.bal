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

