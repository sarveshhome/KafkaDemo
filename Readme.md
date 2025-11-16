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

## Run

In separate terminals:

```bash
dotnet run --project Publisher1
dotnet run --project Publisher2
dotnet run --project Consumer1
dotnet run --project RetryConsumer
```

## Topics

- `orders` - Main topic for order messages
- `orders-retry` - Retry topic for failed messages