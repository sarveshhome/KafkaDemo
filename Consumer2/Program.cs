using Confluent.Kafka;
using System;

const string BootstrapServers = "localhost:9092";
const string TopicName = "orders";

var config = new ConsumerConfig
{
    BootstrapServers = BootstrapServers,
    GroupId = "order-processors",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false
};

using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();

consumer.Subscribe(TopicName);

Console.WriteLine($"Consumer started. Waiting for messages...");

try
{
    while (true)
    {
        try
        {
            var cr = consumer.Consume(TimeSpan.FromMilliseconds(100));
            if (cr != null)
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine($"[ACTIVE] Received: {cr.Message.Value} | Partition: {cr.Partition}");
                Console.ResetColor();
                consumer.Commit(cr);
            }
        }
        catch (ConsumeException e)
        {
            Console.WriteLine($"Error: {e.Error.Reason}");
        }
    }
}
catch (OperationCanceledException)
{
    consumer.Close();
}