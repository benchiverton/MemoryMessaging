namespace MemoryMessaging.Api.Metrics;

public sealed record TransportLatencySnapshot(
    string TransportType,
    long Count,
    TimeSpan Average,
    TimeSpan Min,
    TimeSpan Max,
    TimeSpan Latest,
    DateTimeOffset LatestMeasuredAt);
