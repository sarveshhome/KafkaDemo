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