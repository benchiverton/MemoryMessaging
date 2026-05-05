namespace MemoryMessaging.Api.Transport.SynchronizedSharedMemory;

public sealed class SynchronizedSharedMemoryOptions
{
    public string EndpointName { get; set; } = "memory-messaging";

    public int CapacityBytes { get; set; } = 1_048_576;

    public int MaxFrameBytes { get; set; } = 1_048_576;

    public int ConnectTimeoutMilliseconds { get; set; } = 5_000;

    public int OperationTimeoutMilliseconds { get; set; } = 5_000;
}

