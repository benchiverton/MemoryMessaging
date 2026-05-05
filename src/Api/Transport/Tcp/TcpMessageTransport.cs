using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using MemoryMessaging.Api.Messaging;
using MemoryMessaging.Api.Messaging.Messages;
using MemoryMessaging.Api.Messaging.Options;

namespace MemoryMessaging.Api.Transport.Tcp;

public sealed class TcpMessageTransport(
    ILogger<TcpMessageTransport> logger)
    : IMessageTransport
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public TransportType TransportType => TransportType.Tcp;

    public bool CanSend(ProducerOptions producer) =>
        !string.IsNullOrWhiteSpace(producer.Tcp.ConsumerAddress);

    public string DescribeTarget(ProducerOptions producer) =>
        producer.Tcp.ConsumerAddress ?? "<not configured>";

    public async Task ListenAsync(
        ConsumerOptions consumer,
        IRequestHandler requestHandler,
        CancellationToken cancellationToken)
    {
        var tcp = consumer.Tcp;
        var ipAddress = await ResolveListenAddress(tcp.ListenHost, cancellationToken);
        var listener = new TcpListener(ipAddress, tcp.ListenPort);
        listener.Start();

        logger.LogInformation(
            "TCP consumer {ConsumerName} listening on {Host}:{Port}",
            consumer.Name,
            tcp.ListenHost,
            tcp.ListenPort);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var client = await listener.AcceptTcpClientAsync(cancellationToken);
                _ = HandleClientAsync(client, consumer, requestHandler, cancellationToken);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Normal shutdown.
        }
        finally
        {
            listener.Stop();
        }
    }

    public async Task<Response> RequestAsync(
        ProducerOptions producer,
        Request request,
        CancellationToken cancellationToken)
    {
        var endpoint = TcpMessageEndpoint.Parse(producer.Tcp.ConsumerAddress ?? string.Empty);

        using var client = new TcpClient();
        client.NoDelay = true;

        using var connectTimeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        connectTimeout.CancelAfter(producer.Tcp.ConnectTimeoutMilliseconds);

        try
        {
            await client.ConnectAsync(endpoint.Host, endpoint.Port, connectTimeout.Token);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException($"Timed out connecting to {endpoint}.");
        }

        await using var stream = client.GetStream();
        await WriteFrameAsync(stream, request, cancellationToken);

        return await ReadFrameAsync<Response>(stream, producer.Tcp.MaxFrameBytes, cancellationToken);
    }

    private async Task HandleClientAsync(
        TcpClient client,
        ConsumerOptions consumer,
        IRequestHandler requestHandler,
        CancellationToken cancellationToken)
    {
        using (client)
        {
            client.NoDelay = true;

            try
            {
                await using var stream = client.GetStream();
                var request = await ReadFrameAsync<Request>(stream, consumer.Tcp.MaxFrameBytes, cancellationToken);
                var response = await requestHandler.Handle(request, cancellationToken);
                await WriteFrameAsync(stream, response, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Normal shutdown.
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "TCP request handling failed.");
            }
        }
    }

    private static async Task<T> ReadFrameAsync<T>(
        NetworkStream stream,
        int maxFrameBytes,
        CancellationToken cancellationToken)
    {
        var lengthBuffer = new byte[4];
        await stream.ReadExactlyAsync(lengthBuffer, cancellationToken);

        var length = BinaryPrimitives.ReadInt32BigEndian(lengthBuffer);
        if (length <= 0 || length > maxFrameBytes)
        {
            throw new InvalidOperationException($"Received invalid frame length {length}.");
        }

        var payloadBuffer = new byte[length];
        await stream.ReadExactlyAsync(payloadBuffer, cancellationToken);

        var message = JsonSerializer.Deserialize<T>(payloadBuffer, JsonOptions);
        return message ?? throw new InvalidOperationException($"Received an empty {typeof(T).Name} frame.");
    }

    private static async Task WriteFrameAsync<T>(
        NetworkStream stream,
        T message,
        CancellationToken cancellationToken)
    {
        var payloadBuffer = JsonSerializer.SerializeToUtf8Bytes(message, JsonOptions);
        var lengthBuffer = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(lengthBuffer, payloadBuffer.Length);

        await stream.WriteAsync(lengthBuffer, cancellationToken);
        await stream.WriteAsync(payloadBuffer, cancellationToken);
        await stream.FlushAsync(cancellationToken);
    }

    private static async Task<IPAddress> ResolveListenAddress(string host, CancellationToken cancellationToken)
    {
        if (IPAddress.TryParse(host, out var ipAddress))
        {
            return ipAddress;
        }

        var addresses = await Dns.GetHostAddressesAsync(host, cancellationToken);
        return addresses.FirstOrDefault(address => address.AddressFamily == AddressFamily.InterNetwork)
            ?? addresses.FirstOrDefault()
            ?? IPAddress.Loopback;
    }
}



