# MemoryMessaging latency demo

This solution demonstrates round-trip latency for a tiny request/reply messaging abstraction. It currently includes TCP sockets plus two Windows shared-memory transports built with .NET `MemoryMappedFile` primitives.

## Shape

- Each `Api` instance starts the consumers listed in `Messaging:Consumers`.
- Each `Api` instance starts the producers listed in `Messaging:Producers` that have a configured target for their transport.
- Every consumer/producer entry has a `Type`; `Tcp`, `SynchronizedSharedMemory`, and `RingBufferSharedMemory` are implemented.
- `SynchronizedSharedMemory` is the simple MemoryMappedFile + named `Mutex`/`EventWaitHandle` transport.
- `RingBufferSharedMemory` uses paired single-producer/single-consumer request/response rings in MemoryMappedFile and avoids the mutex on the hot path.
- The producer sends timestamped requests, receives responses, calculates RTT, and records `{ transportType, rtt }` through `IRttMetrics`.
- `MetricsLoggerService` periodically logs count, average, min, max, and latest RTT per transport.

## Aspire

`AppHost` starts two `Api` instances:

- `consumer`: starts TCP, synchronized shared-memory, and ring-buffer shared-memory listeners.
- `producer`: starts matching producers for all three transports.

Run it with:

```powershell
dotnet run --project .\AppHost\AppHost.csproj
```

## Manual two-process run

If running without Aspire, give each process a unique HTTP port and configure both transports:

```powershell
$env:ASPNETCORE_URLS = "http://127.0.0.1:5300"
$env:Messaging__Consumers__0__Name = "consumer-a"
$env:Messaging__Consumers__0__Type = "Tcp"
$env:Messaging__Consumers__0__Tcp__ListenPort = "5100"
$env:Messaging__Consumers__1__Name = "consumer-synchronized-shared-memory"
$env:Messaging__Consumers__1__Type = "SynchronizedSharedMemory"
$env:Messaging__Consumers__1__SynchronizedSharedMemory__EndpointName = "memory-messaging-synchronized-demo"
$env:Messaging__Consumers__2__Name = "consumer-ring-buffer-shared-memory"
$env:Messaging__Consumers__2__Type = "RingBufferSharedMemory"
$env:Messaging__Consumers__2__RingBufferSharedMemory__EndpointName = "memory-messaging-ring-demo"
dotnet run --project .\Api\Api.csproj --no-launch-profile
```

In another terminal:

```powershell
$env:ASPNETCORE_URLS = "http://127.0.0.1:5301"
$env:Messaging__Producers__0__Name = "producer-a"
$env:Messaging__Producers__0__Type = "Tcp"
$env:Messaging__Producers__0__Tcp__ConsumerAddress = "127.0.0.1:5100"
$env:Messaging__Producers__1__Name = "producer-synchronized-shared-memory"
$env:Messaging__Producers__1__Type = "SynchronizedSharedMemory"
$env:Messaging__Producers__1__SynchronizedSharedMemory__EndpointName = "memory-messaging-synchronized-demo"
$env:Messaging__Producers__2__Name = "producer-ring-buffer-shared-memory"
$env:Messaging__Producers__2__Type = "RingBufferSharedMemory"
$env:Messaging__Producers__2__RingBufferSharedMemory__EndpointName = "memory-messaging-ring-demo"
dotnet run --project .\Api\Api.csproj --no-launch-profile
```

