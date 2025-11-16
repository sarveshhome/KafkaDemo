using Confluent.Kafka;
using System;

const string BootstrapServers = "localhost:9092";
const string TopicName = "orders";
const string RetryTopicName = "orders-retry";

var config = new ConsumerConfig
{
    BootstrapServers = BootstrapServers,
    GroupId = "retry-processor",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false
};

var producerConfig = new ProducerConfig { BootstrapServers = BootstrapServers };

using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
using var producer = new ProducerBuilder<Null, string>(producerConfig).Build();

consumer.Subscribe(TopicName);

Console.WriteLine("Retry Consumer started. Monitoring for failed messages...");

try
{
    while (true)
    {
        var cr = consumer.Consume(TimeSpan.FromMilliseconds(100));
        if (cr != null)
        {
            var message = cr.Message.Value;
            
            if (message.EndsWith("-failed"))
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"[FAILED] Detected: {message}");
                Console.ResetColor();
                
                var retryMessage = message.Replace("-failed", "-retry");
                await producer.ProduceAsync(RetryTopicName, new Message<Null, string> { Value = retryMessage });
                
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine($"[RETRY] Sent to retry topic: {retryMessage}");
                Console.ResetColor();
            }
            else
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine($"[SUCCESS] Processed: {message}");
                Console.ResetColor();
            }
            
            consumer.Commit(cr);
        }
    }
}
catch (OperationCanceledException)
{
    consumer.Close();
}