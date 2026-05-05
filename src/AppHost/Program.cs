using Aspire.Hosting;

var builder = DistributedApplication.CreateBuilder(args);

var consumer = builder.AddProject<Projects.Api>("consumer")
	.WithEnvironment("Messaging__Consumers__0__Name", "consumer-a")
	.WithEnvironment("Messaging__Consumers__0__Type", "Tcp")
	.WithEnvironment("Messaging__Consumers__0__Tcp__ListenHost", "127.0.0.1")
	.WithEnvironment("Messaging__Consumers__0__Tcp__ListenPort", "5100")
	.WithEnvironment("Messaging__Consumers__1__Name", "consumer-synchronized-shared-memory")
	.WithEnvironment("Messaging__Consumers__1__Type", "SynchronizedSharedMemory")
	.WithEnvironment("Messaging__Consumers__1__SynchronizedSharedMemory__EndpointName", "memory-messaging-synchronized-demo")
	.WithEnvironment("Messaging__Consumers__2__Name", "consumer-ring-buffer-shared-memory")
	.WithEnvironment("Messaging__Consumers__2__Type", "RingBufferSharedMemory")
	.WithEnvironment("Messaging__Consumers__2__RingBufferSharedMemory__EndpointName", "memory-messaging-ring-demo");

builder.AddProject<Projects.Api>("producer")
	.WithEnvironment("Messaging__Producers__0__Name", "producer-a")
	.WithEnvironment("Messaging__Producers__0__Type", "Tcp")
	.WithEnvironment("Messaging__Producers__0__Tcp__ConsumerAddress", "127.0.0.1:5100")
	.WithEnvironment("Messaging__Producers__0__SendIntervalMilliseconds", "1000")
	.WithEnvironment("Messaging__Producers__0__PayloadSizeBytes", "32")
	.WithEnvironment("Messaging__Producers__1__Name", "producer-synchronized-shared-memory")
	.WithEnvironment("Messaging__Producers__1__Type", "SynchronizedSharedMemory")
	.WithEnvironment("Messaging__Producers__1__SynchronizedSharedMemory__EndpointName", "memory-messaging-synchronized-demo")
	.WithEnvironment("Messaging__Producers__1__SendIntervalMilliseconds", "1000")
	.WithEnvironment("Messaging__Producers__1__PayloadSizeBytes", "32")
	.WithEnvironment("Messaging__Producers__2__Name", "producer-ring-buffer-shared-memory")
	.WithEnvironment("Messaging__Producers__2__Type", "RingBufferSharedMemory")
	.WithEnvironment("Messaging__Producers__2__RingBufferSharedMemory__EndpointName", "memory-messaging-ring-demo")
	.WithEnvironment("Messaging__Producers__2__SendIntervalMilliseconds", "1000")
	.WithEnvironment("Messaging__Producers__2__PayloadSizeBytes", "32")
	.WaitFor(consumer);

builder.Build().Run();
