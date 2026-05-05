namespace MemoryMessaging.Api.Transport;

public enum TransportType
{
    Unknown = 0,
    Tcp = 1,
    SynchronizedSharedMemory = 2,
    RingBufferSharedMemory = 3,
}
