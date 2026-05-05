using MemoryMessaging.Api.Transport;
using MemoryMessaging.Api.Transport.RingBufferSharedMemory;
using MemoryMessaging.Api.Transport.SynchronizedSharedMemory;
using MemoryMessaging.Api.Transport.Tcp;

namespace MemoryMessaging.Api.Messaging.Options;

public sealed class ProducerOptions
{
    public string Name { get; set; } = "producer";

    public TransportType Type { get; set; }

    public TcpTransportOptions Tcp { get; set; } = new();

    public SynchronizedSharedMemoryOptions SynchronizedSharedMemory { get; set; } = new();

    public RingBufferSharedMemoryOptions RingBufferSharedMemory { get; set; } = new();

    public int SendIntervalMilliseconds { get; set; } = 1_000;

    public int PayloadSizeBytes { get; set; } = 32;
}
