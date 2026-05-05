using MemoryMessaging.Api.Messaging.Options;
using MemoryMessaging.Api.Transport;
using Microsoft.Extensions.Options;

namespace MemoryMessaging.Api.Messaging;

public sealed class MessageConsumerService(
    IMessageTransportRegistry transports,
    IRequestHandler requestHandler,
    IOptions<MessagingOptions> options,
    ILogger<MessageConsumerService> logger)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumers = options.Value.Consumers.ToArray();
        if (consumers.Length == 0)
        {
            logger.LogInformation("No message consumers are configured.");
            return;
        }

        var listenerTasks = consumers
            .Select(consumer => Task.Run(() => ListenToConsumerAsync(consumer, stoppingToken), stoppingToken))
            .ToArray();

        await Task.WhenAll(listenerTasks);
    }

    private async Task ListenToConsumerAsync(ConsumerOptions consumer, CancellationToken stoppingToken)
    {
        if (!transports.TryGet(consumer.Type, out var transport))
        {
            logger.LogWarning(
                "Consumer {ConsumerName} has unsupported transport type {TransportType}; supported transport types are {SupportedTransportTypes}.",
                consumer.Name,
                consumer.Type,
                string.Join(", ", transports.SupportedTransportTypes));

            return;
        }

        try
        {
            await transport.ListenAsync(consumer, requestHandler, stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Normal shutdown.
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Message consumer {ConsumerName} stopped unexpectedly.", consumer.Name);
            throw;
        }
    }
}


