CREATE DATABASE IF NOT EXISTS ticketingdb;
USE ticketingdb;

 CREATE TABLE Passenger(
     passengerID INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
     firstName VARCHAR(20) NOT NULL,
     lastName VARCHAR(20) NOT NULL,
     email VARCHAR(35) NOT NULL,
     passwordHash VARCHAR(255) NOT NULL,
     createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
 );

 CREATE TABLE Admin(
     adminID INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
     firstName VARCHAR(20) NOT NULL,
     lastName VARCHAR(20) NOT NULL,
     email VARCHAR(35) NOT NULL,
     passwordHash VARCHAR(255) NOT NULL
 );


 CREATE TABLE Route(
     routeID INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
     routeName VARCHAR(100) NOT NULL,
     routeDescription TEXT
 );

 CREATE TABLE Trip(
     tripID INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
     routeID INT NOT NULL,
     departureTime DATETIME NOT NULL,
     arrivalTime DATETIME NOT NULL,
     status ENUM('scheduled','delayed','canceled'),
     FOREIGN KEY (routeID) REFERENCES Route(routeID)
 );

 CREATE TABLE Ticket(
     ticketID INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
     passengerID INT NOT NULL,
     tripID INT NOT NULL,
     status ENUM('created','paid','validated','expired') DEFAULT 'created',
     dateCreated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
     FOREIGN KEY (passengerID) REFERENCES Passenger(passengerID),
     FOREIGN KEY (tripID) REFERENCES Trip(tripID)
 );

 CREATE TABLE Payment(
     paymentID INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
     ticketID INT NOT NULL,
     status ENUM('pending','completed','failed') DEFAULT 'pending',
     dateCreated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
     FOREIGN KEY (ticketID) REFERENCES Ticket(ticketID)
 );

 CREATE TABLE Notification(
     notificationID INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
     passengerID INT NOT NULL,
     message TEXT,
     dateCreated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
     FOREIGN KEY (passengerID) REFERENCES Passenger(passengerID)
 ); 

