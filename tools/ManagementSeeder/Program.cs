using System.Text;
using CosmoBroker.Client;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var natsUrl = Environment.GetEnvironmentVariable("SEED_NATS_URL") ?? "nats://127.0.0.1:4531";
var amqpHost = Environment.GetEnvironmentVariable("SEED_AMQP_HOST") ?? "127.0.0.1";
var amqpPort = int.TryParse(Environment.GetEnvironmentVariable("SEED_AMQP_PORT"), out var parsedAmqpPort) ? parsedAmqpPort : 5687;

Console.WriteLine($"Seeding broker using NATS={natsUrl} and AMQP={amqpHost}:{amqpPort}");

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

await using var natsOps = new CosmoClient(new CosmoClientOptions { Url = natsUrl });
await using var natsSubA = new CosmoClient(new CosmoClientOptions { Url = natsUrl });
await using var natsSubB = new CosmoClient(new CosmoClientOptions { Url = natsUrl });

await natsOps.ConnectAsync(cts.Token);
await natsSubA.ConnectAsync(cts.Token);
await natsSubB.ConnectAsync(cts.Token);

var subscriberTasks = new[]
{
    HoldSubscriptionAsync(natsSubA, "demo.orders.created", cts.Token),
    HoldSubscriptionAsync(natsSubB, "demo.audit.>", cts.Token)
};

var js = new CosmoJetStream(natsOps);
await js.CreateStreamAsync(new StreamConfig
{
    Name = "MGMT",
    Subjects = new List<string> { "mgmt.events.>" }
}, cts.Token);

for (var i = 1; i <= 12; i++)
{
    await natsOps.PublishAsync("demo.orders.created", $"order-{i:D3}", ct: cts.Token);
    await natsOps.PublishAsync("demo.audit.login", $"login-{i:D3}", ct: cts.Token);
}

for (var i = 1; i <= 8; i++)
{
    await natsOps.PublishAsync($"mgmt.events.sample.{i:D2}", Encoding.UTF8.GetBytes($"js-{i:D2}"), ct: cts.Token);
}

var factory = new ConnectionFactory
{
    HostName = amqpHost,
    Port = amqpPort,
    UserName = "guest",
    Password = "guest",
    VirtualHost = "/",
    DispatchConsumersAsync = true,
    RequestedHeartbeat = TimeSpan.FromSeconds(5)
};

using var amqpPublisherConnection = factory.CreateConnection();
using var amqpPublisherChannel = amqpPublisherConnection.CreateModel();
using var amqpConsumerConnection = factory.CreateConnection();
using var amqpConsumerChannel = amqpConsumerConnection.CreateModel();

amqpPublisherChannel.ExchangeDeclare("mgmt.demo.x", ExchangeType.Direct, durable: true, autoDelete: false);
amqpPublisherChannel.QueueDeclare("mgmt.demo.q", durable: true, exclusive: false, autoDelete: false);
amqpPublisherChannel.QueueBind("mgmt.demo.q", "mgmt.demo.x", "orders");

amqpPublisherChannel.ExchangeDeclare("mgmt.events.x", ExchangeType.Fanout, durable: false, autoDelete: false);
amqpPublisherChannel.QueueDeclare("mgmt.events.q", durable: false, exclusive: false, autoDelete: false);
amqpPublisherChannel.QueueBind("mgmt.events.q", "mgmt.events.x", "");

amqpConsumerChannel.BasicQos(0, 2, false);

var heldDeliveries = new List<ulong>();
var consumerReady = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
var consumer = new AsyncEventingBasicConsumer(amqpConsumerChannel);
consumer.Received += async (_, ea) =>
{
    lock (heldDeliveries)
    {
        if (heldDeliveries.Count < 2)
        {
            heldDeliveries.Add(ea.DeliveryTag);
            consumerReady.TrySetResult();
            return;
        }
    }

    amqpConsumerChannel.BasicAck(ea.DeliveryTag, false);
    await Task.CompletedTask;
};

amqpConsumerChannel.BasicConsume("mgmt.demo.q", autoAck: false, consumer: consumer);

for (var i = 1; i <= 10; i++)
{
    var props = amqpPublisherChannel.CreateBasicProperties();
    props.Persistent = true;
    props.ContentType = "text/plain";
    amqpPublisherChannel.BasicPublish("mgmt.demo.x", "orders", props, Encoding.UTF8.GetBytes($"amqp-order-{i:D3}"));
}

for (var i = 1; i <= 4; i++)
{
    amqpPublisherChannel.BasicPublish("mgmt.events.x", "", basicProperties: null, body: Encoding.UTF8.GetBytes($"fanout-{i:D2}"));
}

await consumerReady.Task.WaitAsync(TimeSpan.FromSeconds(5), cts.Token);

Console.WriteLine("Seeded:");
Console.WriteLine("- NATS subscriptions: demo.orders.created, demo.audit.>");
Console.WriteLine("- JetStream stream: MGMT");
Console.WriteLine("- RabbitMQ exchanges: mgmt.demo.x, mgmt.events.x");
Console.WriteLine("- RabbitMQ queues: mgmt.demo.q, mgmt.events.q");
Console.WriteLine("- RabbitMQ held unacked deliveries: 2");
Console.WriteLine("Seeder is staying alive so the UI continues to show active connections and consumers.");

try
{
    await Task.Delay(Timeout.Infinite, cts.Token);
}
catch (OperationCanceledException)
{
}

lock (heldDeliveries)
{
    foreach (var deliveryTag in heldDeliveries)
        amqpConsumerChannel.BasicAck(deliveryTag, false);
}

await Task.WhenAll(subscriberTasks);

static async Task HoldSubscriptionAsync(CosmoClient client, string subject, CancellationToken ct)
{
    await foreach (var _ in client.SubscribeAsync(subject, ct: ct))
    {
    }
}
