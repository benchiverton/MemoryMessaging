namespace MemoryMessaging.Api.Metrics;

public sealed class MetricsOptions
{
    public const string SectionName = "Metrics";

    public int LogIntervalMilliseconds { get; set; } = 5_000;
}
