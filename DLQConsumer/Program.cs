using Confluent.Kafka;

const string BootstrapServers = "localhost:9092";
const string RetryTopicName = "orders-retry";
const string DlqTopicName = "orders-dlq";
const int MaxRetryAttempts = 3;

var config = new ConsumerConfig
{
    BootstrapServers = BootstrapServers,
    GroupId = "dlq-processor",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false
};

var producerConfig = new ProducerConfig { BootstrapServers = BootstrapServers };

using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
using var producer = new ProducerBuilder<Null, string>(producerConfig).Build();

consumer.Subscribe(RetryTopicName);

Console.WriteLine("DLQ Consumer started. Processing retry messages...");

try
{
    while (true)
    {
        var cr = consumer.Consume(TimeSpan.FromMilliseconds(100));
        if (cr != null)
        {
            var message = cr.Message.Value;
            var retryCount = GetRetryCount(cr.Message.Headers);
            
            Console.WriteLine($"Processing retry message: {message} (attempt {retryCount + 1})");
            
            if (ShouldProcessMessage(message))
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine($"[SUCCESS] Processed: {message}");
                Console.ResetColor();
            }
            else
            {
                if (retryCount >= MaxRetryAttempts - 1)
                {
                    await SendToDlq(message);
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine($"[DLQ] Sent to dead letter queue: {message}");
                    Console.ResetColor();
                }
                else
                {
                    await RetryMessage(message, retryCount + 1);
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine($"[RETRY] Retrying: {message} (attempt {retryCount + 2})");
                    Console.ResetColor();
                }
            }
            
            consumer.Commit(cr);
        }
    }
}
catch (OperationCanceledException)
{
    consumer.Close();
}

static int GetRetryCount(Headers headers)
{
    var retryHeader = headers?.FirstOrDefault(h => h.Key == "retry-count");
    return retryHeader != null ? int.Parse(System.Text.Encoding.UTF8.GetString(retryHeader.GetValueBytes())) : 0;
}

static bool ShouldProcessMessage(string message)
{
    return !message.Contains("-retry") || Random.Shared.Next(1, 4) == 1;
}

async Task RetryMessage(string message, int retryCount)
{
    var headers = new Headers { { "retry-count", System.Text.Encoding.UTF8.GetBytes(retryCount.ToString()) } };
    await producer.ProduceAsync(RetryTopicName, new Message<Null, string> 
    { 
        Value = message, 
        Headers = headers 
    });
}

async Task SendToDlq(string message)
{
    var headers = new Headers { { "failed-at", System.Text.Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString()) } };
    await producer.ProduceAsync(DlqTopicName, new Message<Null, string> 
    { 
        Value = message, 
        Headers = headers 
    });
}