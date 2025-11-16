using Confluent.Kafka;
using System;
using System.Threading.Tasks;

const string BootstrapServers = "localhost:9092";
const string TopicName = "orders";

var config = new ProducerConfig { BootstrapServers = BootstrapServers };

using var producer = new ProducerBuilder<Null, string>(config).Build();

var orders = new[] { "Order-101", "Order-103", "Order-105", "Order-107" };

foreach (var order in orders)
{
    var message = new Message<Null, string> { Value = order };
    var dr = await producer.ProduceAsync(TopicName, message);
    Console.WriteLine($"Publisher 1 → Sent: {order} to partition {dr.Partition}");
    await Task.Delay(500);
}