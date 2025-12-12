# Event Hubs Prober

A .NET console application that sends random events to Azure Event Hubs using the Kafka protocol and collects telemetry metrics into a Microsoft Fabric Eventstream.

## Prerequisites

- .NET 6.0 or higher
- An Azure Event Hubs namespace, the one utilized to send events to
- A Microsoft Fabric Eventstream, the one utilized to push telemetry metrics
- Connection strings for both the Event Hub and Fabric Eventstream

## Setup

### 1. Install Dependencies

```bash
dotnet restore
```

### 2. Set Environment Variables

Set the following environment variables with your Azure Event Hubs connection strings:

**On macOS/Linux:**
```bash
export EH_CONNECTION_STRING="Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=your-key;EntityPath=your-event-hub"
export ES_CONNECTION_STRING="Endpoint=sb://your-stream-namespace.servicebus.windows.net/;SharedAccessKeyName=key_name;SharedAccessKey=your-key;EntityPath=your-event-stream"
```

**On Windows (PowerShell):**
```powershell
$env:EH_CONNECTION_STRING = "Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=your-key;EntityPath=your-event-hub"
$env:ES_CONNECTION_STRING = "Endpoint=sb://your-stream-namespace.servicebus.windows.net/;SharedAccessKeyName=key_name;SharedAccessKey=your-key;EntityPath=your-event-stream"
```

### 3. Run the Program

```bash
dotnet run $EH_CONNECTION_STRING $ES_CONNECTION_STRING
```
## How It Works

- The program sends events with random Partition Keys and payloads of 1KB to the Event Hubs data instance
- Every 1000 milliseconds, it batches the telemetry results for each send and uploads them to the Fabric Event Stream
- Each event includes partition ID, offset, timestamp, elapsed time and ExceptionMessages if any.

## Output

The program logs:
- Each event sent with timestamp, elapsed time, partition, and offset
- Telemetry batch confirmations with number of events included per batch

A sample run will look like:
```
mb-prober % bin/Debug/net8.0/eh-prober $ehConnStr $telConnStr 
94c48933-56e5-49bf-aa4b-ec7621fc8269 2025-12-13 05:02:20.221 - SendRandomEvent completed in 12206ms - Partition: 1, Offset: 2100620, Error: 
Telemetry batch sent - Partition: [1], Offset: 164602, Events: 1
94c48933-56e5-49bf-aa4b-ec7621fc8269 2025-12-13 05:02:21.117 - SendRandomEvent completed in 763ms - Partition: 1, Offset: 2100630, Error: 
94c48933-56e5-49bf-aa4b-ec7621fc8269 2025-12-13 05:02:21.876 - SendRandomEvent completed in 758ms - Partition: 5, Offset: 2082968, Error: 
Telemetry batch sent - Partition: [2], Offset: 163820, Events: 2
```