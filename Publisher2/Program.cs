using Confluent.Kafka;
using System;
using System.Threading.Tasks;

const string BootstrapServers = "localhost:9092";
const string TopicName = "orders";

var config = new ProducerConfig { BootstrapServers = BootstrapServers };

using var producer = new ProducerBuilder<Null, string>(config).Build();

//var orders = new[] { "Order-112", "Order-114", "Order-116", "Order-118","Order-120", "Order-122", "Order-124", "Order-125" };
var i=0;
while(true)
{ 
    var order = "Order-"+ (++i);
    if(i%7==0)
        order += "-failed";
    var message = new Message<Null, string> { Value = order };
    var dr = await producer.ProduceAsync(TopicName, message);
    Console.WriteLine($"Publisher 2 → Sent: {order} to partition {dr.Partition} to OffSet {dr.Offset}");
    await Task.Delay(1000);
}