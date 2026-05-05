namespace MemoryMessaging.Api.Metrics;

public sealed record RttMetric(
    string TransportType,
    TimeSpan Rtt,
    DateTimeOffset MeasuredAt);
