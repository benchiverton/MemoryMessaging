namespace MemoryMessaging.Api.Transport.Tcp;

public sealed class TcpTransportOptions
{
    public string? ConsumerAddress { get; set; }

    public string ListenHost { get; set; } = "127.0.0.1";

    public int ListenPort { get; set; } = 5100;

    public int ConnectTimeoutMilliseconds { get; set; } = 5_000;

    public int MaxFrameBytes { get; set; } = 1_048_576;
}
