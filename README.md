# GCP Test Suite for Java

The GCP Test Suite is a testing repository for GCP services in Java.
This repo contains sample implementations of GCP features and services along with a test implementation of
those services.

# Pre-requisites

Docker must be installed and configured so that the current user can invoke containers. On Linux, this means adding
docker and the user to a docker group.
Java 17+ must be installed.
Maven 3.8+ must be installed.

# Build
To build and test:
```bash
mvn clean verify
```

# Services

## Storage
The Storage samples demonstrate how to store and retrieve content in a bucket.

Features:
* Creation of a bucket
* Verifying that a bucket exists
* Writing content to a bucket
* Reading content from a bucket
* Verifying that an object exists
* Storage emulation with TestContainers

## PubSub
The PubSub samples demonstrate how to create topics, publish and receive messages.

Features:
* Creation of a topic
* Creation of a subscriber
* Publish messages to a topic
* Receiving messages from a subscriber
* Topic purging
* PubSub emulation with TestContainers

## Firestore
The Firestore samples demonstrate how to read and write to data to Firestore collections.

Features:
* Batch loading data
* Basic queries
* Range queries
* Transactional updates
* Collection name class mapping
* Lombok based pojo's
* Firestore emulation with TestContainers

## Datastore
The Datastore samples demonstrate how to read and write to data to a Firestore in Datastore mode using Objectify.

Features:
* Objectify pojo's
* Loading data using the Objectify Service
* Saving data using the Objectify Service
* Using filters when loading data
* Datastore emulation with TestContainers