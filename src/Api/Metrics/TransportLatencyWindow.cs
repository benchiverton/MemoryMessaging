namespace MemoryMessaging.Api.Metrics;

public sealed class TransportLatencyWindow(string transportType)
{
    private long _count;
    private double _totalMilliseconds;
    private TimeSpan _min = TimeSpan.MaxValue;
    private TimeSpan _max = TimeSpan.Zero;
    private TimeSpan _latest = TimeSpan.Zero;
    private DateTimeOffset _latestMeasuredAt;

    public long Count => _count;

    public void Add(TimeSpan rtt, DateTimeOffset measuredAt)
    {
        _count++;
        _totalMilliseconds += rtt.TotalMilliseconds;

        if (rtt < _min)
        {
            _min = rtt;
        }

        if (rtt > _max)
        {
            _max = rtt;
        }

        _latest = rtt;
        _latestMeasuredAt = measuredAt;
    }

    public TransportLatencySnapshot ToSnapshotAndReset()
    {
        var snapshot = new TransportLatencySnapshot(
            transportType,
            _count,
            TimeSpan.FromMilliseconds(_totalMilliseconds / _count),
            _min,
            _max,
            _latest,
            _latestMeasuredAt);

        _count = 0;
        _totalMilliseconds = 0;
        _min = TimeSpan.MaxValue;
        _max = TimeSpan.Zero;
        _latest = TimeSpan.Zero;
        _latestMeasuredAt = default;

        return snapshot;
    }
}
