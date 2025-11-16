using System;
using Confluent.Kafka;

const string BootstrapServers = "localhost:9092";
const string TopicName = "orders";

var config = new ConsumerConfig
{
    BootstrapServers = BootstrapServers,
    GroupId = "order-processors",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false,
    ClientId = "Consumer-" + Random.Shared.Next(1000, 9999),
};

using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();

consumer.Subscribe(TopicName);

Console.WriteLine($"Consumer started. Waiting for messages..." + consumer.Name);

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

                var messageValue = cr.Message.Value;
                if (messageValue.EndsWith("-failed"))
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine(
                        $"[FAILED] Processing failed for: {messageValue} | Partition: {cr.Partition} | offset: {cr.Offset} | by {consumer.Name}"
                    );
                    // Do not commit the offset for failed messages
                    Console.ResetColor();
                }
                else
                {
                    Console.WriteLine(
                        $"[ACTIVE] Received: {cr.Message.Value} | Partition: {cr.Partition} | offset: {cr.Offset} | by {consumer.Name}"
                    );
                    Console.ResetColor();
                    consumer.Commit(cr);
                }
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
