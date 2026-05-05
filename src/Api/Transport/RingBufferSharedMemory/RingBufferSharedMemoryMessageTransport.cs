using System.Buffers.Binary;
using System.IO.MemoryMappedFiles;
using System.Text;
using System.Text.Json;
using MemoryMessaging.Api.Messaging;
using MemoryMessaging.Api.Messaging.Messages;
using MemoryMessaging.Api.Messaging.Options;

namespace MemoryMessaging.Api.Transport.RingBufferSharedMemory;

public sealed class RingBufferSharedMemoryMessageTransport(
    ILogger<RingBufferSharedMemoryMessageTransport> logger)
    : IMessageTransport
{
    private const int LengthPrefixBytes = 4;
    private const int RingHeaderBytes = 128;
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public TransportType TransportType => TransportType.RingBufferSharedMemory;

    public bool CanSend(ProducerOptions producer) =>
        !string.IsNullOrWhiteSpace(producer.RingBufferSharedMemory.EndpointName);

    public string DescribeTarget(ProducerOptions producer) =>
        producer.RingBufferSharedMemory.EndpointName;

    public async Task ListenAsync(
        ConsumerOptions consumer,
        IRequestHandler requestHandler,
        CancellationToken cancellationToken)
    {
        EnsureSupportedPlatform();

        var options = consumer.RingBufferSharedMemory;
        var layout = RingBufferLayout.Create(options.SlotCount, options.MaxFrameBytes);
        var names = RingBufferNames.ForEndpoint(options.EndpointName);

        using var sharedMemory = MemoryMappedFile.CreateOrOpen(names.ChannelName, layout.TotalBytes);
        using var accessor = sharedMemory.CreateViewAccessor(0, layout.TotalBytes);
        using var requestAvailable = new EventWaitHandle(false, EventResetMode.AutoReset, names.RequestAvailableEventName);
        using var responseAvailable = new EventWaitHandle(false, EventResetMode.AutoReset, names.ResponseAvailableEventName);

        ResetRing(accessor, layout.RequestRingOffset);
        ResetRing(accessor, layout.ResponseRingOffset);

        logger.LogInformation(
            "Ring-buffer shared memory consumer {ConsumerName} listening on {EndpointName}",
            consumer.Name,
            options.EndpointName);

        while (!cancellationToken.IsCancellationRequested)
        {
            if (!TryReadFrame<Request>(accessor, layout, layout.RequestRingOffset, options.MaxFrameBytes, out var request))
            {
                var waitResult = WaitHandle.WaitAny([requestAvailable, cancellationToken.WaitHandle]);
                if (waitResult == 1 || cancellationToken.IsCancellationRequested)
                {
                    break;
                }

                continue;
            }

            try
            {
                var response = await requestHandler.Handle(request, cancellationToken);
                WaitForWrite(
                    accessor,
                    layout,
                    layout.ResponseRingOffset,
                    response,
                    options.MaxFrameBytes,
                    responseAvailable,
                    options.OperationTimeoutMilliseconds,
                    options.SpinBeforeWait,
                    cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Ring-buffer shared memory request handling failed for endpoint {EndpointName}.", options.EndpointName);
            }
        }
    }

    public Task<Response> RequestAsync(
        ProducerOptions producer,
        Request request,
        CancellationToken cancellationToken)
    {
        EnsureSupportedPlatform();

        var options = producer.RingBufferSharedMemory;
        var layout = RingBufferLayout.Create(options.SlotCount, options.MaxFrameBytes);
        var names = RingBufferNames.ForEndpoint(options.EndpointName);
        var timeout = TimeSpan.FromMilliseconds(Math.Max(1, options.ConnectTimeoutMilliseconds));

        using var sharedMemory = OpenExistingChannel(names.ChannelName, timeout, cancellationToken);
        using var accessor = sharedMemory.CreateViewAccessor(0, layout.TotalBytes);
        using var requestAvailable = OpenExistingEvent(names.RequestAvailableEventName, timeout, cancellationToken);
        using var responseAvailable = OpenExistingEvent(names.ResponseAvailableEventName, timeout, cancellationToken);

        WaitForWrite(
            accessor,
            layout,
            layout.RequestRingOffset,
            request,
            options.MaxFrameBytes,
            requestAvailable,
            options.OperationTimeoutMilliseconds,
            options.SpinBeforeWait,
            cancellationToken);

        var response = WaitForRead<Response>(
            accessor,
            layout,
            layout.ResponseRingOffset,
            options.MaxFrameBytes,
            responseAvailable,
            options.OperationTimeoutMilliseconds,
            options.SpinBeforeWait,
            cancellationToken);

        if (response.MessageId != request.MessageId)
        {
            throw new InvalidOperationException(
                $"Received response {response.MessageId} while waiting for request {request.MessageId}. Ring-buffer shared memory endpoints are single-producer/single-consumer.");
        }

        return Task.FromResult(response);
    }

    private static void WaitForWrite<T>(
        MemoryMappedViewAccessor accessor,
        RingBufferLayout layout,
        long ringOffset,
        T message,
        int maxFrameBytes,
        EventWaitHandle itemAvailable,
        int timeoutMilliseconds,
        int spinBeforeWait,
        CancellationToken cancellationToken)
    {
        var deadline = DateTimeOffset.UtcNow + TimeSpan.FromMilliseconds(Math.Max(1, timeoutMilliseconds));
        var spinCount = 0;

        while (!cancellationToken.IsCancellationRequested)
        {
            if (TryWriteFrame(accessor, layout, ringOffset, message, maxFrameBytes))
            {
                itemAvailable.Set();
                return;
            }

            if (DateTimeOffset.UtcNow >= deadline)
            {
                throw new TimeoutException("Timed out waiting for a free ring-buffer shared memory slot.");
            }

            SpinOrYield(spinBeforeWait, ref spinCount, cancellationToken);
        }

        cancellationToken.ThrowIfCancellationRequested();
    }

    private static T WaitForRead<T>(
        MemoryMappedViewAccessor accessor,
        RingBufferLayout layout,
        long ringOffset,
        int maxFrameBytes,
        EventWaitHandle itemAvailable,
        int timeoutMilliseconds,
        int spinBeforeWait,
        CancellationToken cancellationToken)
    {
        var deadline = DateTimeOffset.UtcNow + TimeSpan.FromMilliseconds(Math.Max(1, timeoutMilliseconds));
        var spinCount = 0;

        while (!cancellationToken.IsCancellationRequested)
        {
            if (TryReadFrame<T>(accessor, layout, ringOffset, maxFrameBytes, out var message))
            {
                return message;
            }

            if (DateTimeOffset.UtcNow >= deadline)
            {
                throw new TimeoutException("Timed out waiting for a ring-buffer shared memory item.");
            }

            if (spinCount < Math.Max(1, spinBeforeWait))
            {
                spinCount++;
                Thread.SpinWait(64);
                continue;
            }

            WaitHandle.WaitAny([itemAvailable, cancellationToken.WaitHandle], TimeSpan.FromMilliseconds(1));
        }

        cancellationToken.ThrowIfCancellationRequested();
        throw new OperationCanceledException(cancellationToken);
    }

    private static bool TryWriteFrame<T>(
        MemoryMappedViewAccessor accessor,
        RingBufferLayout layout,
        long ringOffset,
        T message,
        int maxFrameBytes)
    {
        var writeSequence = accessor.ReadInt64(ringOffset);
        var readSequence = accessor.ReadInt64(ringOffset + sizeof(long));
        if (writeSequence - readSequence >= layout.SlotCount)
        {
            return false;
        }

        var payload = JsonSerializer.SerializeToUtf8Bytes(message, JsonOptions);
        if (payload.Length > maxFrameBytes || payload.Length > layout.SlotPayloadBytes)
        {
            throw new InvalidOperationException(
                $"Ring-buffer shared memory frame length {payload.Length} exceeds the configured maximum.");
        }

        var slotOffset = layout.GetSlotOffset(ringOffset, writeSequence);
        var lengthBuffer = new byte[LengthPrefixBytes];
        BinaryPrimitives.WriteInt32BigEndian(lengthBuffer, payload.Length);

        accessor.WriteArray(slotOffset, lengthBuffer, 0, lengthBuffer.Length);
        accessor.WriteArray(slotOffset + LengthPrefixBytes, payload, 0, payload.Length);

        Thread.MemoryBarrier();
        accessor.Write(ringOffset, writeSequence + 1);
        return true;
    }

    private static bool TryReadFrame<T>(
        MemoryMappedViewAccessor accessor,
        RingBufferLayout layout,
        long ringOffset,
        int maxFrameBytes,
        out T message)
    {
        var readSequence = accessor.ReadInt64(ringOffset + sizeof(long));
        var writeSequence = accessor.ReadInt64(ringOffset);
        if (readSequence >= writeSequence)
        {
            message = default!;
            return false;
        }

        Thread.MemoryBarrier();

        var slotOffset = layout.GetSlotOffset(ringOffset, readSequence);
        var lengthBuffer = new byte[LengthPrefixBytes];
        accessor.ReadArray(slotOffset, lengthBuffer, 0, lengthBuffer.Length);

        var length = BinaryPrimitives.ReadInt32BigEndian(lengthBuffer);
        if (length <= 0 || length > maxFrameBytes || length > layout.SlotPayloadBytes)
        {
            throw new InvalidOperationException($"Received invalid ring-buffer shared memory frame length {length}.");
        }

        var payload = new byte[length];
        accessor.ReadArray(slotOffset + LengthPrefixBytes, payload, 0, payload.Length);

        message = JsonSerializer.Deserialize<T>(payload, JsonOptions)
            ?? throw new InvalidOperationException($"Received an empty {typeof(T).Name} ring-buffer shared memory frame.");

        Thread.MemoryBarrier();
        accessor.Write(ringOffset + sizeof(long), readSequence + 1);
        return true;
    }

    private static void ResetRing(MemoryMappedViewAccessor accessor, long ringOffset)
    {
        accessor.Write(ringOffset, 0L);
        accessor.Write(ringOffset + sizeof(long), 0L);
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
        throw new TimeoutException($"Timed out opening ring-buffer shared memory channel '{name}'.");
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
        throw new TimeoutException($"Timed out opening ring-buffer shared memory event '{name}'.");
    }

    private static void SpinOrYield(int spinBeforeWait, ref int spinCount, CancellationToken cancellationToken)
    {
        if (spinCount < Math.Max(1, spinBeforeWait))
        {
            spinCount++;
            Thread.SpinWait(64);
            return;
        }

        if (WaitHandle.WaitAny([cancellationToken.WaitHandle], TimeSpan.FromMilliseconds(1)) == 0)
        {
            cancellationToken.ThrowIfCancellationRequested();
        }
    }

    private static void SleepBeforeRetry(CancellationToken cancellationToken)
    {
        if (WaitHandle.WaitAny([cancellationToken.WaitHandle], TimeSpan.FromMilliseconds(25)) == 0)
        {
            cancellationToken.ThrowIfCancellationRequested();
        }
    }

    private static void EnsureSupportedPlatform()
    {
        if (!OperatingSystem.IsWindows())
        {
            throw new PlatformNotSupportedException(
                "The ring-buffer shared memory transport uses named MemoryMappedFile and EventWaitHandle objects and is supported by this demo on Windows.");
        }
    }

    private sealed record RingBufferLayout(
        int SlotCount,
        int SlotPayloadBytes,
        int SlotBytes,
        long RequestRingOffset,
        long ResponseRingOffset,
        long TotalBytes)
    {
        public static RingBufferLayout Create(int configuredSlotCount, int maxFrameBytes)
        {
            var slotCount = Math.Max(2, configuredSlotCount);
            var slotPayloadBytes = Math.Max(LengthPrefixBytes, maxFrameBytes);
            var slotBytes = LengthPrefixBytes + slotPayloadBytes;
            var requestRingOffset = 0L;
            var ringBytes = RingHeaderBytes + ((long)slotCount * slotBytes);
            var responseRingOffset = ringBytes;
            var totalBytes = ringBytes * 2;

            return new RingBufferLayout(
                slotCount,
                slotPayloadBytes,
                slotBytes,
                requestRingOffset,
                responseRingOffset,
                totalBytes);
        }

        public long GetSlotOffset(long ringOffset, long sequence)
        {
            var slotIndex = sequence % SlotCount;
            var slotByteOffset = slotIndex * SlotBytes;
            return ringOffset + RingHeaderBytes + slotByteOffset;
        }
    }

    private sealed record RingBufferNames(
        string ChannelName,
        string RequestAvailableEventName,
        string ResponseAvailableEventName)
    {
        public static RingBufferNames ForEndpoint(string endpointName)
        {
            var safeEndpointName = SanitizeName(endpointName);
            return new RingBufferNames(
                $"Local\\MemoryMessaging.{safeEndpointName}.Ring.Channel",
                $"Local\\MemoryMessaging.{safeEndpointName}.Ring.RequestAvailable",
                $"Local\\MemoryMessaging.{safeEndpointName}.Ring.ResponseAvailable");
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
