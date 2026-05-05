using Microsoft.Extensions.Options;

namespace MemoryMessaging.Api.Metrics;

public sealed class MetricsLoggerService(
    LatencyMetrics metrics,
    IOptions<MetricsOptions> options,
    ILogger<MetricsLoggerService> logger)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var interval = TimeSpan.FromMilliseconds(Math.Max(1, options.Value.LogIntervalMilliseconds));

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(interval, stoppingToken);

            var snapshots = metrics.SnapshotAndReset();
            if (snapshots.Count == 0)
            {
                logger.LogInformation("No RTT metrics recorded in the last {IntervalMilliseconds} ms.", interval.TotalMilliseconds);
                continue;
            }

            foreach (var snapshot in snapshots)
            {
                logger.LogInformation(
                    "RTT metrics transport={TransportType} count={Count} avg={AverageMilliseconds:N3} ms min={MinMilliseconds:N3} ms max={MaxMilliseconds:N3} ms latest={LatestMilliseconds:N3} ms latestAt={LatestMeasuredAt:o}",
                    snapshot.TransportType,
                    snapshot.Count,
                    snapshot.Average.TotalMilliseconds,
                    snapshot.Min.TotalMilliseconds,
                    snapshot.Max.TotalMilliseconds,
                    snapshot.Latest.TotalMilliseconds,
                    snapshot.LatestMeasuredAt);
            }
        }
    }
}


