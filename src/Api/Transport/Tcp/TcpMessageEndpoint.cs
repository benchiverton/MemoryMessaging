namespace MemoryMessaging.Api.Transport.Tcp;

public sealed record TcpMessageEndpoint(string Host, int Port)
{
    public static TcpMessageEndpoint Parse(string address)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(address);

        const string tcpPrefix = "tcp://";
        if (address.StartsWith(tcpPrefix, StringComparison.OrdinalIgnoreCase))
        {
            address = address[tcpPrefix.Length..];
        }

        var separatorIndex = address.LastIndexOf(':');
        if (separatorIndex <= 0 || separatorIndex == address.Length - 1)
        {
            throw new FormatException($"Consumer address '{address}' must be in 'host:port' or 'tcp://host:port' format.");
        }

        var host = address[..separatorIndex];
        var portText = address[(separatorIndex + 1)..];

        if (!int.TryParse(portText, out var port) || port is <= 0 or > 65_535)
        {
            throw new FormatException($"Consumer address '{address}' has an invalid port.");
        }

        return new TcpMessageEndpoint(host, port);
    }

    public override string ToString() => $"{Host}:{Port}";
}
