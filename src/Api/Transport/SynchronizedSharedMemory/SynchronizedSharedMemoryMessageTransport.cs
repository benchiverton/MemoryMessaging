#pragma warning disable CA1416

using System.Buffers.Binary;
using System.IO.MemoryMappedFiles;
using System.Text;
using System.Text.Json;
using MemoryMessaging.Api.Messaging;
using MemoryMessaging.Api.Messaging.Messages;
using MemoryMessaging.Api.Messaging.Options;

namespace MemoryMessaging.Api.Transport.SynchronizedSharedMemory;

public sealed class SynchronizedSharedMemoryMessageTransport(
    ILogger<SynchronizedSharedMemoryMessageTransport> logger)
    : IMessageTransport
{
    private const int LengthPrefixBytes = 4;
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public TransportType TransportType => TransportType.SynchronizedSharedMemory;

    public bool CanSend(ProducerOptions producer) =>
        !string.IsNullOrWhiteSpace(producer.SynchronizedSharedMemory.EndpointName);

    public string DescribeTarget(ProducerOptions producer) =>
        producer.SynchronizedSharedMemory.EndpointName;

    public async Task ListenAsync(
        ConsumerOptions consumer,
        IRequestHandler requestHandler,
        CancellationToken cancellationToken)
    {
        EnsureSupportedPlatform();

        var options = consumer.SynchronizedSharedMemory;
        var names = SharedMemoryNames.ForEndpoint(options.EndpointName);
        var capacityBytes = GetCapacityBytes(options.CapacityBytes, options.MaxFrameBytes);

        using var channel = MemoryMappedFile.CreateOrOpen(names.ChannelName, capacityBytes);
        using var requestReady = new EventWaitHandle(false, EventResetMode.AutoReset, names.RequestReadyEventName);
        using var channelMutex = new Mutex(false, names.MutexName);

        ResetChannel(channel, capacityBytes);

        logger.LogInformation(
            "Synchronized shared memory consumer {ConsumerName} listening on {EndpointName}",
            consumer.Name,
            options.EndpointName);

        while (!cancellationToken.IsCancellationRequested)
        {
            var waitResult = WaitHandle.WaitAny([requestReady, cancellationToken.WaitHandle]);
            if (waitResult == 1 || cancellationToken.IsCancellationRequested)
            {
                break;
            }

            try
            {
                var frame = ReadFrame<SharedMemoryRequestFrame>(
                    channel,
                    capacityBytes,
                    options.MaxFrameBytes);

                var response = await requestHandler.Handle(frame.Request, cancellationToken);

                using var replyChannel = MemoryMappedFile.OpenExisting(frame.ReplyChannelName);
                using var replyReady = EventWaitHandle.OpenExisting(frame.ReplyReadyEventName);

                WriteFrame(
                    replyChannel,
                    response,
                    frame.ReplyCapacityBytes,
                    Math.Min(frame.ReplyCapacityBytes - LengthPrefixBytes, options.MaxFrameBytes));

                replyReady.Set();
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Synchronized shared memory request handling failed for endpoint {EndpointName}.", options.EndpointName);
            }
        }
    }

    public Task<Response> RequestAsync(
        ProducerOptions producer,
        Request request,
        CancellationToken cancellationToken)
    {
        EnsureSupportedPlatform();

        var options = producer.SynchronizedSharedMemory;
        var names = SharedMemoryNames.ForEndpoint(options.EndpointName);
        var capacityBytes = GetCapacityBytes(options.CapacityBytes, options.MaxFrameBytes);
        var replyCapacityBytes = capacityBytes;
        var replyNames = SharedMemoryNames.ForReply(options.EndpointName, request.MessageId);
        var timeout = TimeSpan.FromMilliseconds(Math.Max(1, options.ConnectTimeoutMilliseconds));
        var operationTimeout = TimeSpan.FromMilliseconds(Math.Max(1, options.OperationTimeoutMilliseconds));

        using var channel = OpenExistingChannel(names.ChannelName, timeout, cancellationToken);
        using var requestReady = OpenExistingEvent(names.RequestReadyEventName, timeout, cancellationToken);
        using var channelMutex = OpenExistingMutex(names.MutexName, timeout, cancellationToken);
        using var replyChannel = MemoryMappedFile.CreateOrOpen(replyNames.ChannelName, replyCapacityBytes);
        using var replyReady = new EventWaitHandle(false, EventResetMode.AutoReset, replyNames.ReplyReadyEventName);

        ResetChannel(replyChannel, replyCapacityBytes);

        var hasMutex = false;
        try
        {
            hasMutex = Wait(channelMutex, operationTimeout, cancellationToken);
            if (!hasMutex)
            {
                throw new TimeoutException($"Timed out waiting for shared memory endpoint '{options.EndpointName}'.");
            }

            var frame = new SharedMemoryRequestFrame(
                request,
                replyNames.ChannelName,
                replyNames.ReplyReadyEventName,
                replyCapacityBytes);

            WriteFrame(channel, frame, capacityBytes, options.MaxFrameBytes);
            requestReady.Set();

            if (!Wait(replyReady, operationTimeout, cancellationToken))
            {
                throw new TimeoutException($"Timed out waiting for shared memory response from '{options.EndpointName}'.");
            }

            var response = ReadFrame<Response>(replyChannel, replyCapacityBytes, options.MaxFrameBytes);
            return Task.FromResult(response);
        }
        finally
        {
            if (hasMutex)
            {
                channelMutex.ReleaseMutex();
            }
        }
    }

    private static void ResetChannel(MemoryMappedFile channel, int capacityBytes)
    {
        using var accessor = channel.CreateViewAccessor(0, capacityBytes);
        accessor.Write(0, 0);
    }

    private static void EnsureSupportedPlatform()
    {
        if (!OperatingSystem.IsWindows())
        {
            throw new PlatformNotSupportedException(
                "The shared memory transport uses named MemoryMappedFile, Mutex, and EventWaitHandle objects and is supported by this demo on Windows.");
        }
    }

    private static void WriteFrame<T>(
        MemoryMappedFile channel,
        T message,
        int capacityBytes,
        int maxFrameBytes)
    {
        var payload = JsonSerializer.SerializeToUtf8Bytes(message, JsonOptions);
        if (payload.Length > maxFrameBytes || payload.Length > capacityBytes - LengthPrefixBytes)
        {
            throw new InvalidOperationException(
                $"Shared memory frame length {payload.Length} exceeds the configured maximum.");
        }

        var lengthBuffer = new byte[LengthPrefixBytes];
        BinaryPrimitives.WriteInt32BigEndian(lengthBuffer, payload.Length);

        using var accessor = channel.CreateViewAccessor(0, capacityBytes);
        accessor.WriteArray(0, lengthBuffer, 0, lengthBuffer.Length);
        accessor.WriteArray(LengthPrefixBytes, payload, 0, payload.Length);
    }

    private static T ReadFrame<T>(
        MemoryMappedFile channel,
        int capacityBytes,
        int maxFrameBytes)
    {
        using var accessor = channel.CreateViewAccessor(0, capacityBytes);

        var lengthBuffer = new byte[LengthPrefixBytes];
        accessor.ReadArray(0, lengthBuffer, 0, lengthBuffer.Length);

        var length = BinaryPrimitives.ReadInt32BigEndian(lengthBuffer);
        if (length <= 0 || length > maxFrameBytes || length > capacityBytes - LengthPrefixBytes)
        {
            throw new InvalidOperationException($"Received invalid shared memory frame length {length}.");
        }

        var payload = new byte[length];
        accessor.ReadArray(LengthPrefixBytes, payload, 0, payload.Length);

        var message = JsonSerializer.Deserialize<T>(payload, JsonOptions);
        return message ?? throw new InvalidOperationException($"Received an empty {typeof(T).Name} shared memory frame.");
    }

    private static MemoryMappedFile OpenExistingChannel(
        string name,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                return MemoryMappedFile.OpenExisting(name);
            }
            catch (FileNotFoundException) when (DateTimeOffset.UtcNow < deadline)
            {
                SleepBeforeRetry(cancellationToken);
            }
        }

        cancellationToken.ThrowIfCancellationRequested();
        throw new TimeoutException($"Timed out opening shared memory channel '{name}'.");
    }

    private static EventWaitHandle OpenExistingEvent(
        string name,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                return EventWaitHandle.OpenExisting(name);
            }
            catch (WaitHandleCannotBeOpenedException) when (DateTimeOffset.UtcNow < deadline)
            {
                SleepBeforeRetry(cancellationToken);
            }
        }

        cancellationToken.ThrowIfCancellationRequested();
        throw new TimeoutException($"Timed out opening shared memory event '{name}'.");
    }

    private static Mutex OpenExistingMutex(
        string name,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                return Mutex.OpenExisting(name);
            }
            catch (WaitHandleCannotBeOpenedException) when (DateTimeOffset.UtcNow < deadline)
            {
                SleepBeforeRetry(cancellationToken);
            }
        }

        cancellationToken.ThrowIfCancellationRequested();
        throw new TimeoutException($"Timed out opening shared memory mutex '{name}'.");
    }

    private static bool Wait(WaitHandle waitHandle, TimeSpan timeout, CancellationToken cancellationToken)
    {
        var waitResult = WaitHandle.WaitAny([waitHandle, cancellationToken.WaitHandle], timeout);
        if (waitResult == WaitHandle.WaitTimeout)
        {
            return false;
        }

        cancellationToken.ThrowIfCancellationRequested();
        return waitResult == 0;
    }

    private static void SleepBeforeRetry(CancellationToken cancellationToken)
    {
        if (WaitHandle.WaitAny([cancellationToken.WaitHandle], TimeSpan.FromMilliseconds(25)) == 0)
        {
            cancellationToken.ThrowIfCancellationRequested();
        }
    }

    private static int GetCapacityBytes(int configuredCapacityBytes, int maxFrameBytes) =>
        Math.Max(configuredCapacityBytes, maxFrameBytes + LengthPrefixBytes);

    private sealed record SharedMemoryRequestFrame(
        Request Request,
        string ReplyChannelName,
        string ReplyReadyEventName,
        int ReplyCapacityBytes);

    private sealed record SharedMemoryNames(
        string ChannelName,
        string RequestReadyEventName,
        string ReplyReadyEventName,
        string MutexName)
    {
        public static SharedMemoryNames ForEndpoint(string endpointName)
        {
            var safeEndpointName = SanitizeName(endpointName);
            return new SharedMemoryNames(
                $"Local\\MemoryMessaging.{safeEndpointName}.Channel",
                $"Local\\MemoryMessaging.{safeEndpointName}.RequestReady",
                $"Local\\MemoryMessaging.{safeEndpointName}.ReplyReady",
                $"Local\\MemoryMessaging.{safeEndpointName}.Mutex");
        }

        public static SharedMemoryNames ForReply(string endpointName, Guid messageId)
        {
            var safeEndpointName = SanitizeName(endpointName);
            var safeMessageId = messageId.ToString("N");
            return new SharedMemoryNames(
                $"Local\\MemoryMessaging.{safeEndpointName}.{safeMessageId}.ReplyChannel",
                $"Local\\MemoryMessaging.{safeEndpointName}.{safeMessageId}.UnusedRequestReady",
                $"Local\\MemoryMessaging.{safeEndpointName}.{safeMessageId}.ReplyReady",
                $"Local\\MemoryMessaging.{safeEndpointName}.{safeMessageId}.UnusedMutex");
        }

        private static string SanitizeName(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return "default";
            }

            var builder = new StringBuilder(value.Length);
            foreach (var character in value)
            {
                builder.Append(char.IsLetterOrDigit(character) ? character : '_');
            }

            return builder.ToString();
        }
    }
}

#pragma warning restore CA1416

