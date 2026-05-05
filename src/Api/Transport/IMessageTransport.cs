using MemoryMessaging.Api.Messaging;
using MemoryMessaging.Api.Messaging.Messages;
using MemoryMessaging.Api.Messaging.Options;

namespace MemoryMessaging.Api.Transport;

public interface IMessageTransport
{
    public TransportType TransportType { get; }

    public bool CanSend(ProducerOptions producer);

    public string DescribeTarget(ProducerOptions producer);

    public Task ListenAsync(
        ConsumerOptions consumer,
        IRequestHandler requestHandler,
        CancellationToken cancellationToken);

    public Task<Response> RequestAsync(
        ProducerOptions producer,
        Request request,
        CancellationToken cancellationToken);
}
