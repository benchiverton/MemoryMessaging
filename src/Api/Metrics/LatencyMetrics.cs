namespace MemoryMessaging.Api.Metrics;

public sealed class LatencyMetrics : IRttMetrics
{
    private readonly object _gate = new();
    private readonly Dictionary<string, TransportLatencyWindow> _windows = new(StringComparer.OrdinalIgnoreCase);

    public void Record(RttMetric metric)
    {
        lock (_gate)
        {
            if (!_windows.TryGetValue(metric.TransportType, out var window))
            {
                window = new TransportLatencyWindow(metric.TransportType);
                _windows[metric.TransportType] = window;
            }

            window.Add(metric.Rtt, metric.MeasuredAt);
        }
    }

    public IReadOnlyCollection<TransportLatencySnapshot> SnapshotAndReset()
    {
        lock (_gate)
        {
            var snapshots = _windows.Values
                .Where(window => window.Count > 0)
                .Select(window => window.ToSnapshotAndReset())
                .ToArray();

            return snapshots;
        }
    }
}
