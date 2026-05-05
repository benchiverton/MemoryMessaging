namespace MemoryMessaging.Api.Transport.RingBufferSharedMemory;

public sealed class RingBufferSharedMemoryOptions
{
    public string EndpointName { get; set; } = "memory-messaging-ring";

    public int SlotCount { get; set; } = 64;

    public int MaxFrameBytes { get; set; } = 64 * 1024;

    public int ConnectTimeoutMilliseconds { get; set; } = 5_000;

    public int OperationTimeoutMilliseconds { get; set; } = 5_000;

    public int SpinBeforeWait { get; set; } = 64;
}

