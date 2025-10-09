# DSA621S-ASSIGNMENT-2

# 🚌 Distributed Smart Public Transport Ticketing System

## 📖 Overview

This project implements a **Distributed Smart Public Transport Ticketing System** for the Windhoek City Council.  
It demonstrates a **microservices-based**, **event-driven** architecture using **Ballerina**, **Kafka**, **MySQL**, and **Docker Compose**.

The system replaces traditional paper-based ticketing with a modern, scalable, and fault-tolerant platform that allows:
- Passengers to view routes, purchase tickets, and receive notifications.
- Administrators to create routes, publish trip updates, and monitor sales.
- Asynchronous event processing for tickets, payments, and schedule updates.

---

## 🎯 Learning Objectives

By completing this project, students should be able to:
- Design and implement independent microservices with RESTful APIs.
- Apply **event-driven communication** using **Kafka producers and consumers**.
- Persist and query data from a relational database (**MySQL**).
- Deploy and orchestrate multiple services using **Docker Compose**.
- Demonstrate basic monitoring, testing, and fault tolerance in distributed systems.

---

## 🏗️ System Architecture

The system follows a **microservices + event-driven architecture**:


---

## 🧩 Microservices Overview

| Service | Port | Description |
|----------|------|-------------|
| **Passenger Service** | `8081` | Manages passenger registration, authentication, and viewing tickets. |
| **Transport Service** | `8082` | Allows admins to create/manage routes & trips, and publish schedule updates. |
| **Ticketing Service** | `8083` | Handles ticket creation, updates, and status changes via Kafka. |
| **Payment Service** | `8084` | Simulates payments and publishes confirmations to Kafka. |
| **Notification Service** | `8085` | Listens for Kafka events (schedule updates, ticket confirmations) and stores notifications. |
| **Admin Service** | `8086` | Provides admin controls for monitoring sales and publishing disruptions. |
| **Kafka Broker** | `9092` | Manages event-driven communication between services. |
| **MySQL Database** | `3306` | Stores passengers, routes, trips, tickets, and notifications. |

---

## 🗂️ Technologies Used

| Category | Technology |
|-----------|-------------|
| Language | [Ballerina](https://ballerina.io) |
| Messaging | [Apache Kafka](https://kafka.apache.org/) |
| Database | [MySQL](https://www.mysql.com/) |
| Containerisation | [Docker](https://www.docker.com/) |
| Orchestration | [Docker Compose](https://docs.docker.com/compose/) |

---

## ⚙️ Setup and Installation

### 1. Clone the Repository
```bash
git clone https://github.com/Driftedbucket/DSA621S-ASSIGNMENT-2.git
cd DSA621S-ASSIGNMENT-2
