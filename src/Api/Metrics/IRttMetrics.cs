namespace MemoryMessaging.Api.Metrics;

public interface IRttMetrics
{
    public void Record(RttMetric metric);
}
