using Confluent.Kafka;

const string BootstrapServers = "localhost:9092";
const string DlqTopicName = "orders-dlq";

var config = new ConsumerConfig
{
    BootstrapServers = BootstrapServers,
    GroupId = "dlq-monitor",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = true
};

using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
consumer.Subscribe(DlqTopicName);

Console.WriteLine("DLQ Monitor started. Watching for dead letter messages...");

try
{
    while (true)
    {
        var cr = consumer.Consume(TimeSpan.FromMilliseconds(100));
        if (cr != null)
        {
            var message = cr.Message.Value;
            var failedAt = GetHeaderValue(cr.Message.Headers, "failed-at");
            
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"[DLQ] Message: {message}");
            Console.WriteLine($"[DLQ] Failed at: {failedAt}");
            Console.WriteLine($"[DLQ] Partition: {cr.Partition}, Offset: {cr.Offset}");
            Console.WriteLine("---");
            Console.ResetColor();
        }
    }
}
catch (OperationCanceledException)
{
    consumer.Close();
}

static string GetHeaderValue(Headers headers, string key)
{
    var header = headers?.FirstOrDefault(h => h.Key == key);
    return header != null ? System.Text.Encoding.UTF8.GetString(header.GetValueBytes()) : "Unknown";
}