using Azure.Messaging.EventHubs;
using Confluent.Kafka;
using System.Diagnostics;
using System.Text.Json;

// Global variables for Event Hubs configuration
if (args.Length < 2)
{
    Console.WriteLine("Usage: dotnet run <EventHubsConnStr> <EventStreamConnStr>");
    return;
}
string ehConnStr = args[0];
string esConnStr = args[1];

var dataConnStr = EventHubsConnectionStringProperties.Parse(ehConnStr);
var eventHubsNamespace = dataConnStr.FullyQualifiedNamespace;
var eventHubName = dataConnStr.EventHubName;
var telConnStr = EventHubsConnectionStringProperties.Parse(esConnStr);
var eventStreamNamespace = telConnStr.FullyQualifiedNamespace;
var eventStreamName = telConnStr.EventHubName;

// Kafka configuration for Azure Event Hubs
var dataConfig = new ProducerConfig
{
    BootstrapServers = $"{eventHubsNamespace}:9093",
    SecurityProtocol = SecurityProtocol.SaslSsl,
    SaslMechanism = SaslMechanism.Plain,
    SaslUsername = "$ConnectionString",
    SaslPassword = ehConnStr,
    Acks = Acks.All
};

var telemetryConfig = new ProducerConfig
{
    BootstrapServers = $"{eventStreamNamespace}:9093",
    SecurityProtocol = SecurityProtocol.SaslSsl,
    SaslMechanism = SaslMechanism.Plain,
    SaslUsername = "$ConnectionString",
    SaslPassword = esConnStr,
    Acks = Acks.All
};

using var producer = new ProducerBuilder<string, string>(dataConfig).Build();
using var telemetryProducer = new ProducerBuilder<string, string>(telemetryConfig).Build();

List<EventResult> results = new List<EventResult>();
int batchTimeThresholdMs = 1000;
int accumulatedElapsedMs = 0;

var stopwatch = Stopwatch.StartNew();
string clientId = Guid.NewGuid().ToString();
while (true)
{
    stopwatch.Restart();
    var result = await SendRandomEvent(producer);
    stopwatch.Stop();
    result.ElapsedMilliseconds = (int)stopwatch.ElapsedMilliseconds;
    result.ClientId = clientId;
    Console.WriteLine($"{result.ClientId} {result.Timestamp:yyyy-MM-dd HH:mm:ss.fff} - SendRandomEvent completed in {result.ElapsedMilliseconds}ms - Partition: {result.PartitionId}, Offset: {result.Offset}, Error: {result.ExceptionMessage}");
    
    results.Add(result);
    accumulatedElapsedMs += result.ElapsedMilliseconds;
    
    if (accumulatedElapsedMs >= batchTimeThresholdMs)
    {
        _ = SendTelemetryBatch(telemetryProducer, results);
        results = new List<EventResult>();
        accumulatedElapsedMs = 0;
    }
}

// MAIN PROGRAM ENDS HERE

async Task<EventResult> SendRandomEvent(IProducer<string, string> producer)
{
    try
    {
        // Send events to the Event Hub via Kafka
        string vin = "WDDZF" + Random.Shared.Next(1, 999999);
        string payload = new string('X', 1024); // 1 KB payload
        var result1 = await producer.ProduceAsync(eventHubName, new Message<string, string> { Key = vin, Value = payload });        
        return new EventResult { PartitionId = result1.Partition.Value, Offset = result1.Offset.Value, ExceptionMessage = null, Timestamp = DateTime.Now };
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error sending events: {ex.Message}");
        Thread.Sleep(1000);        
        return new EventResult { PartitionId = -1, Offset = -1, ExceptionMessage = ex.Message, Timestamp = DateTime.Now };
    }
}

async Task SendTelemetryBatch(IProducer<string, string> telemetryProducer, List<EventResult> batch)
{
    try
    {
        string jsonBatch = JsonSerializer.Serialize(batch);
        var result = await telemetryProducer.ProduceAsync(eventStreamName, new Message<string, string> { Key = Guid.NewGuid().ToString(), Value = jsonBatch });
        Console.WriteLine($"Telemetry batch sent - Partition: {result.Partition}, Offset: {result.Offset}, Events: {batch.Count}");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error sending telemetry batch: {ex.Message}");
    }
}

public class EventResult
{
    public string? ClientId { get; set; }
    public int PartitionId { get; set; }
    public long Offset { get; set; }
    public string? ExceptionMessage { get; set; }
    public DateTime Timestamp { get; set; }
    public int ElapsedMilliseconds { get; set; }
}