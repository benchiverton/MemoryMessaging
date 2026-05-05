namespace MemoryMessaging.Api.Transport;

public sealed class MessageTransportRegistry : IMessageTransportRegistry
{
    private readonly IReadOnlyDictionary<TransportType, IMessageTransport> _transports;

    public MessageTransportRegistry(IEnumerable<IMessageTransport> transports) =>
        _transports = transports.ToDictionary(transport => transport.TransportType);

    public IReadOnlyCollection<TransportType> SupportedTransportTypes => _transports.Keys.ToArray();

    public bool TryGet(TransportType transportType, out IMessageTransport transport) =>
        _transports.TryGetValue(transportType, out transport!);
}

