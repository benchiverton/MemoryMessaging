namespace MemoryMessaging.Api.Transport;

public interface IMessageTransportRegistry
{
    public IReadOnlyCollection<TransportType> SupportedTransportTypes { get; }

    public bool TryGet(TransportType transportType, out IMessageTransport transport);
}

