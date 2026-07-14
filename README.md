# orderLakeHouse
orderLakeHouse is a solution for multi-chain distributed businesses who need to manage, store, and analyse their order data. It provides a fault-tolerant, region-aware pipeline that ingests orders at scale, streams them through isolated Kafka clusters, and lands them into a structured HDFS lakehouse partitioned by business, region, state, outlet, and time.

<br>

## Design Philosophy 
The core design principle of orderLakeHouse is that the event drives everything. No external mapping tables, no configuration files, no hardcoded routing logic  every decision about where data goes is derived directly from the fields of the OrderRequest itself.

The topic is the businessId. Rather than maintaining a registry of businesses and their corresponding Kafka topics, the producer simply uses the businessId as the topic name. This means a new business is onboarded the moment its first order arrives the topic is created automatically and the consumer picks it up immediately because it listens to any 9-digit topic pattern rather than a fixed list.

The key is the stateId. Kafka uses the message key to determine which partition a message lands on. By using stateId as the key, all orders from the same state are guaranteed to land on the same partition, preserving ordering within a state without any explicit partition assignment logic.

The path is the event. The HDFS storage path is not configured anywhere  it is assembled at write time from regionId, businessId, stateId, and the date components extracted from orderTime. This means the physical layout of data in HDFS is a direct structural reflection of the business hierarchy: region contains business, business contains state, state contains time. Querying data for a specific business, region, or time range maps directly to a directory traversal.

The storage is append-first. Orders from the same business, region, state, and day accumulate in a single orders.json file as newline-delimited JSON. This avoids the overhead of creating one file per order while keeping the data human-readable and easy to process incrementally.

## Architectural Layers 
### Generation Layer
The generation layer is everything that produces and routes an order event before it reaches the streaming infrastructure. It includes the ordering machine, the API gateway, and the region-specific dispatch queues. Its job is to authenticate, enrich, and deliver the right event to the right producer.
### Production Layer
The production layer is the set of region-specific event-producer-svc instances. Each producer receives an order event from its assigned queue, preprocesses it, resolves the target Kafka cluster from regionId, and resolves the target topic from businessId then publishes the event to Kafka. This is where raw order data becomes a structured, routable stream.
### Persistence Layer
The persistence layer spans from Kafka consumption to HDFS storage. The kafka-hdfs-sink-svc polls the Kafka cluster, reads each event, and derives the full HDFS storage path directly from the event's own fields no external mapping required. Data lands as Parquet files in a deeply partitioned directory structure, ready for analysis.

## Data flow 
An order enters the system as an OrderRequest  a validated Java object carrying businessId, regionId, stateId, outletId, orderTime, and products. All fields are strictly validated before anything moves forward: businessId must be exactly 9 digits, regionId 3 digits, stateId 6 digits, outletId 11 digits, and orderTime must be a valid ISO8601 timestamp with a UTC offset.

Once validated, the producer service resolves two things from the request  the topic and the key. The businessId becomes the Kafka topic, and the stateId becomes the message key. The full OrderRequest object is then serialized to JSON by the KafkaTemplate and sent via kafkaTemplate.send(topic, key, value). No further processing happens in the producer  routing is entirely driven by the event fields themselves.

The message lands in Kafka, held in the topic named after the businessId, partitioned by the stateId key. On the other side, the consumer service listens to any topic matching the pattern \d{9}  so every new business topic is picked up automatically without any configuration change. When a message arrives, the OrderRequest is deserialized back from JSON and passed directly to the HDFS service.

The HDFS service builds the storage path entirely from the event data. It parses orderTime using OffsetDateTime to extract year, month, and day, then constructs the path as {basePath}/{regionId}/{businessId}/{stateId}/{year}/{month}/{day}/orders.json. If the directory doesn't exist it is created with mkdirs(). If the orders.json file already exists the order is appended to it; if not, the file is created fresh. The order is written as a single JSON line followed by a newline character, flushed, and the stream is closed. Every order from the same business, region, state, and day accumulates in the same file, one line per order.

## Components 
### Kafka orderLakeHouseKafka091 (Indian cluster BASE)

The cluster runs as three Docker containers on a dedicated bridge network called orderLakeHouseKafka091Network. Each node runs apache/kafka:latest in KRaft mode, meaning every node is simultaneously a broker and a controller with no ZooKeeper and no separate controller process.
The cluster ID is orderLakeHouseKafka091, which encodes the regionId 091 directly into the cluster identity.

Each node has three listeners: INTERNAL on port 9092 for inter-broker communication, CONTROLLER on port 9093 for KRaft consensus, and EXTERNAL on port 9094 mapped to the host. The three nodes are exposed to the host on ports 9091, 9092, and 9093 respectively. Quorum voters are all three nodes, so the cluster can tolerate one node failure and still maintain consensus.
Both the default replication factor and the offsets topic replication factor are set to 3, meaning every message and every consumer offset is replicated across all three nodes. Each node is heap-capped at 200MB. Data is persisted to named Docker volumes orderLakeHouseKafka091Node1Data, Node2Data, and Node3Data mounted at /var/lib/kafka/data.

### HDFS  orderLakeHouseHdfs (Universal)

The HDFS cluster runs on orderLakeHouseHdfsNetwork and consists of 7 containers: 2 NameNodes, 2 DataNodes, 1 ResourceManager, and 2 NodeManagers, all running bde2020/hadoop 3.2.1 on Java 8, pinned to linux/amd64.
orderLakeHouseHdfsNameNode1 is the active NameNode, exposed on port 9870 for the web UI and port 9000 for HDFS RPC. It is the filesystem entry point and all services connect to hdfs://orderLakeHouseHdfsNameNode1:9000 as fs.defaultFS. orderLakeHouseHdfsNameNode2 is the standby, connected to the same network but not exposed externally, and depends on NameNode1 being up first.

Both DataNodes  orderLakeHouseHdfsDataNode1 and orderLakeHouseHdfsDataNode2  connect to NameNode1 and store blocks at /hadoop/dfs/data backed by named volumes. The replication factor is set to 2, so every block written by the consumer is stored on both DataNodes.
The YARN layer consists of orderLakeHouseHdfsResourceManager1 exposed on port 8032 for RPC and 8088 for the web UI, and two NodeManagers  orderLakeHouseHdfsNodeManager1 and orderLakeHouseHdfsNodeManager2  each allocated 1024MB of memory and 2 vCPUs with mapreduce_shuffle enabled as an auxiliary service. NodeManager local data is persisted to named volumes at /hadoop/yarn/local. The cluster name is orderLakeHouse and all nodes share the same bridge network with no external port exposure except the NameNode and ResourceManager.

### Producer orderlakehouse-producer-svc (generic)

The producer is a Spring Boot application named orderlakehouse-producer-svc running on port 8081. It connects to all three Kafka nodes at orderLakeHouseKafka091Node1:9092, orderLakeHouseKafka091Node2:9092, and orderLakeHouseKafka091Node3:9092 as its bootstrap servers, giving it awareness of the full cluster from startup.

Message keys are serialized using StringSerializer and message values are serialized using Spring's JsonSerializer. The property spring.json.add.type.headers is explicitly set to false, which means the producer does not embed the Java class name in the Kafka message headers. This is what allows the consumer to deserialize the payload into its own OrderRequest class independently without any class name coupling between the two services.
The service class OrderLakeHouseProducerGenericService extracts the businessId from the OrderRequest as the topic name and the stateId as the message key, then dispatches via kafkaTemplate.send(topic, key, orderRequest). No transformation, no conditional logic  routing is entirely field-driven.

### Consumer  orderlakehouse-kafka-to-hdfs-ingestor-svc (generic)

The consumer is a Spring Boot application named orderlakehouse-kafka-to-hdfs-ingestor-svc running on port 8080. It connects to the same three Kafka bootstrap servers as the producer and belongs to the consumer group orderLakeHouseIngestorGroup with auto-offset-reset set to earliest, meaning it will always read from the beginning of a topic if no committed offset exists.

Keys are deserialized as plain strings using StringDeserializer. Values are deserialized using Spring's JsonDeserializer with trusted packages set to wildcard and type header inference disabled via spring.json.use.type.headers: false. The target type is explicitly fixed to org.hackctl.order.orderlakehouse.ingestor.kafka.hdfs.dto.OrderRequest via spring.json.value.default.type, which is what allows the consumer to deserialize the payload correctly without relying on the producer embedding any type information in the message headers.
The HDFS connection points to hdfs://orderLakeHouseHdfsNameNode1:9000 and the base path for all writes is /orders. 

The full storage path constructed at runtime is /orders/{regionId}/{businessId}/{stateId}/{year}/{month}/{day}/orders.json, built entirely from the fields of the incoming OrderRequest. The service class OrderLakeHouseConsumerGenericService listens to any topic matching the pattern \d{9}, delegates every received order to HdfsService.writeOrder, and catches any HDFS write exceptions at the message level, logging the error without retrying or routing to a dead letter queue.

# How to Run 
To start the service, run the following command in base directory:
```bash
bash start 
```

Sample Producer api call ~

POST http://localhost:8081/api/v1/order
Content-Type: application/json

```
{
  "businessId": "000000001",
  "regionId": "091",
  "stateId": "110022",
  "outletId": "00000000001",
  "orderTime": "2026-06-11T10:30:00+05:30",
  "products": [
    {
      "name": "Chicken Burger",
      "quantity": 2,
      "price": 250.00
    },
    {
      "name": "Cold Drink",
      "quantity": 1,
      "price": 80.00
    }
  ]
}
```

To stop the service, run the following command in base directory:
```bash
bash clean
```
