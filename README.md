# Pintu Sr. Data Engineer Assesment
This documentation will explain step by step to completed the assessment.

# Pre-Required
1. Docker
2. Python

# How to run the assessment
1. Running docker
    ```
    docker-compose up -d
    ```
2. Verify the services are running
    ```
    docker-compose ps
    ```
3. Create the Kafka topic
    - technical_assessment
        ```
        docker-compose exec kafka kafka-topics --create --topic technical_assessment --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
        ```
    - order_book
        ```
        docker-compose exec kafka kafka-topics --create --topic order_book --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
        ```
4. Create Schema Registry
    ```
    python3 producer/generate_schema.py
    ```
5. Start producing messages
    ```
    python3 producer/generate_mock_messages.py
    ```
6. Develop and run your Spark Structured Streaming application:
    ```
    Run your Spark Structured Streaming
    ```
7. Read data using read_data.ipynb