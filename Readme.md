# Kafka Demo

A .NET Kafka demonstration with publishers, consumers, and retry logic.

## Prerequisites

- .NET 9.0
- Kafka running on localhost:9092

## Projects

- **Publisher1** - Sends orders 101, 103, 105, 107
- **Publisher2** - Sends orders with failures (every 7th message ends with "-failed")
- **Consumer1** - Processes orders, handles failed messages without committing
- **Consumer2** - Basic consumer template
- **Consumer3** - Basic consumer template
- **RetryConsumer** - Detects failed messages and sends to retry topic
- **DLQConsumer** - Processes retry messages with retry limit (3 attempts)
- **DLQMonitor** - Monitors dead letter queue messages

## Run

In separate terminals:

```bash
dotnet run --project Publisher1
dotnet run --project Publisher2
dotnet run --project Consumer1
dotnet run --project RetryConsumer
dotnet run --project DLQConsumer
dotnet run --project DLQMonitor
```

## Topics

- `orders` - Main topic for order messages
- `orders-retry` - Retry topic for failed messages
- `orders-dlq` - Dead letter queue for permanently failed messages


## Notes

- First, run the Consumer1 project in three terminals (3 consumers).
- Then, run the Publisher2 project in one terminal (1 publisher).
- I have three (3) partitions in the order topic.

## DLQ Flow

1. **RetryConsumer** detects failed messages and sends to `orders-retry` topic
2. **DLQConsumer** processes retry messages with up to 3 attempts
3. After 3 failed attempts, messages are sent to `orders-dlq` topic
4. **DLQMonitor** displays all messages in the dead letter queue


## Run components

In separate terminals, run the following commands:

```bash
dotnet run --project DLQConsumer
dotnet run --project DLQMonitor
```


### Interview question

1. You have 3 partitions and 6 consumers — how does Kafka distribute?

    Kafka works on one rule:

    One partition → max ONE consumer in a consumer group.

    So with 3 partitions and 6 consumers in the same consumer group:

    ✔ Only 3 consumers will get assigned (1 per partition)
    ✔ Remaining 3 consumers will stay idle (no messages)



| Partition   | Assigned to Consumer |
| ----------- | -------------------- |
| Partition 0 | Consumer 1           |
| Partition 1 | Consumer 2           |
| Partition 2 | Consumer 3           |
| Partition 3 | ❌ No such partition  |
| Partition 4 | ❌ No such partition  |
| Partition 5 | ❌ No such partition  |




1. Difference between Kafka Streams vs Consumer API?

Streams → real-time processing, windowing, state store.

Kafka consumer API → simple message consumption; stateless.


2. How do you design Retry + DLQ Architecture?

Answer:

Main Topic → Process

On exception → Retry topic

Max retries → DLQ topic

Operator manually replays DLQ messages


3. How do you ensure message ordering in Kafka?


Answer: 

Use same key for related messages → same partition → ordered consumption.



4. How do you monitor Kafka consumers?
Answer: 
Use Kafka monitoring tools (e.g., Confluent Control Center, Prometheus + Grafana) to track consumer lag, throughput, and errors.

5. How do you handle schema evolution in Kafka?

Answer:
Use a schema registry (e.g., Confluent Schema Registry) to manage and evolve schemas
without breaking compatibility.


How to use all 6 consumers?
✔ Increase partitions to at least 6.

Do consumers in different groups get all messages?
✔ Yes, each group has its own copy.

If one consumer dies, what happens?
✔ Rebalance → idle consumer takes the partition.


### prompt

if you search in openAI , or other AI, then you should ask question

Give me a prompt template of the above result

