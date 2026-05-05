using MemoryMessaging.Api.Messaging.Messages;
using MemoryMessaging.Api.Messaging.Options;
using MemoryMessaging.Api.Metrics;
using MemoryMessaging.Api.Transport;
using Microsoft.Extensions.Options;

namespace MemoryMessaging.Api.Messaging;

public sealed class MessageProducerService(
    IMessageTransportRegistry transports,
    IRttMetrics metrics,
    IOptions<MessagingOptions> options,
    ILogger<MessageProducerService> logger)
    : BackgroundService, IResponseHandler
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var producers = options.Value.Producers
            .Where(CanSend)
            .ToArray();

        if (producers.Length == 0)
        {
            logger.LogInformation("No message producers are configured with a consumer address.");
            return;
        }

        var producerTasks = producers
            .Select(producer => RunProducerAsync(producer, stoppingToken))
            .ToArray();

        await Task.WhenAll(producerTasks);
    }

    private async Task RunProducerAsync(ProducerOptions producer, CancellationToken stoppingToken)
    {
        if (!transports.TryGet(producer.Type, out var transport))
        {
            logger.LogWarning(
                "Producer {ProducerName} has unsupported transport type {TransportType}; supported transport types are {SupportedTransportTypes}.",
                producer.Name,
                producer.Type,
                string.Join(", ", transports.SupportedTransportTypes));

            return;
        }

        var payload = new string('x', Math.Max(0, producer.PayloadSizeBytes));
        var interval = TimeSpan.FromMilliseconds(Math.Max(1, producer.SendIntervalMilliseconds));
        var target = transport.DescribeTarget(producer);

        logger.LogInformation(
            "Producer {ProducerName} sending {TransportType} requests to {Target} every {IntervalMilliseconds} ms.",
            producer.Name,
            transport.TransportType,
            target,
            interval.TotalMilliseconds);

        while (!stoppingToken.IsCancellationRequested)
        {
            var request = new Request(Guid.NewGuid(), DateTimeOffset.UtcNow, payload);

            try
            {
                var response = await transport.RequestAsync(producer, request, stoppingToken);
                await HandleResponse(transport.TransportType, response, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Producer {ProducerName} request {MessageId} failed.", producer.Name, request.MessageId);
            }

            await Task.Delay(interval, stoppingToken);
        }
    }

    public ValueTask Handle(Response response, CancellationToken cancellationToken) =>
        HandleResponse(TransportType.Unknown, response, cancellationToken);

    private ValueTask HandleResponse(
        TransportType transportType,
        Response response,
        CancellationToken cancellationToken)
    {
        _ = cancellationToken;

        var receivedAt = DateTimeOffset.UtcNow;
        var rtt = receivedAt - response.RequestSentAt;

        metrics.Record(new RttMetric(transportType.ToString(), rtt, receivedAt));

        return ValueTask.CompletedTask;
    }

    private bool CanSend(ProducerOptions producer)
    {
        if (!transports.TryGet(producer.Type, out var transport))
        {
            return true;
        }

        return transport.CanSend(producer);
    }
}


