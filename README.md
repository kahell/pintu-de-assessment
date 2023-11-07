# Pintu Sr. Data Engineer Assesment
This documentation will explain step by step to completed the assessment.

# Pre-Required
1. Docker & Install Dependencies
2. Python & Install Dependencies
3. Scala & Install Dependencies

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
4. Create Schema Registry
    ```
    python3 scripts/producer/generate_schema.py
    ```
5. Start producing messages
    ```
    python3 scripts/producer/generate_mock_messages.py
    ```
6. Build JAR
7. Run application:
    ```
    spark-submit \
    --class org.pintu.HealthCheck \
    --master local[*] \
    /path/to/your/jar/pintu-de-assesment-1.0-SNAPSHOT.jar
    ```
7. Verify the data using scripts/consumer/read_data.ipynb

# Unit Test
1. Goto `/src/test/scala/org/pintu`
2. Run that test