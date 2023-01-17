using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace DotNet.Utils.Kafka
{
    public class KafkaConsumer<TKey, TValue, THandler> : IKafkaConsumer where THandler : IKafkaConsumerHandler<TKey, TValue>
    {
        private readonly KafkaConfig<TKey, TValue> _kafkaSettings;
        private readonly IServiceProvider _serviceProvider;
        private readonly ILogger<KafkaConsumer<TKey, TValue, THandler>> _logger;
        private readonly ConsumerConfig _config;
        private Lazy<ISubject<ConsumeResult<TKey, TValue>>>[] _partitionQueues;
        private Lazy<IKafkaConsumerHandler<TKey, TValue>>[] _handlers;
        private readonly CancellationTokenSource _cancelToken;
        private IConsumer<TKey, TValue> _consumer;
        private IKafkaMetrics _metrics;

        public KafkaConsumer(KafkaConfig<TKey, TValue> kafkaSettings, IServiceProvider serviceProvider, ILogger<KafkaConsumer<TKey, TValue, THandler>> logger)
        {
            _kafkaSettings = kafkaSettings;
            _serviceProvider = serviceProvider;
            _logger = logger;

            _config = new ConsumerConfig
            {
                BootstrapServers = _kafkaSettings.Servers,
                GroupId = _kafkaSettings.GroupId,
                HeartbeatIntervalMs = 10000,
                EnableAutoCommit = false,
                AutoOffsetReset = _kafkaSettings.AutoOffsetReset,
                AllowAutoCreateTopics = true,
            };
            _cancelToken = new CancellationTokenSource();
        }

        public void Start()
        {
            if (_logger == null)
            {
                throw new ArgumentNullException($"logger can not null: {_kafkaSettings.Topic}");
            }
            _metrics = _serviceProvider.GetRequiredService<IKafkaMetrics>().SetLabels(_config.GroupId, _kafkaSettings.Topic);
            var partitionCount = GetPartitionCount();
            _partitionQueues = new Lazy<ISubject<ConsumeResult<TKey, TValue>>>[partitionCount];
            _handlers = new Lazy<IKafkaConsumerHandler<TKey, TValue>>[partitionCount];
            _consumer = ConsumerBuild();
            _ = Run();
            _logger.LogInformation($"Kafka consumer is running: {_kafkaSettings.Topic}");
        }

        private Task Run()
        {
            for (var i = 0; i < _partitionQueues.Length; i++)
            {
                var handler = _handlers[i] = new Lazy<IKafkaConsumerHandler<TKey, TValue>>(_serviceProvider.GetRequiredService<IKafkaConsumerHandler<TKey, TValue>>);
                var subject = _partitionQueues[i] = new Lazy<ISubject<ConsumeResult<TKey, TValue>>>(() =>
                {
                    var _subject = new Subject<ConsumeResult<TKey, TValue>>();
                    _subject.Limit(async r =>
                    {
                    retry:
                        try
                        {
                            while (!await handler.Value.HandlerAsync(r))
                            {
                                await Task.Delay(10);
                            }
                            _metrics.HandlerInc();
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, $"{nameof(IKafkaConsumerHandler<TKey, TValue>)} handle error: {_kafkaSettings.Topic}");
                            await Task.Delay(1000);
                            goto retry;
                        }
                    }, _kafkaSettings.MaxBufferCount, _cancelToken.Token)
                    .Batch(TimeSpan.FromSeconds(5), _kafkaSettings.MaxBufferCount)
                    .DoAsync(async r =>
                    {
                        if (!r.Any())
                        {
                            return;
                        }
                    retry:
                        try
                        {
                            while (!await handler.Value.CommitAsync())
                            {
                                await Task.Delay(10);
                                _logger.LogWarning($"{nameof(IKafkaConsumerHandler<TKey, TValue>)} CommitAsync retry: {_kafkaSettings.Topic}");
                            }
                            _consumer.Commit(r.Last());
                            _metrics.CommitInc(r.Count);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, $"{nameof(IKafkaConsumerHandler<TKey, TValue>)} commit error: {_kafkaSettings.Topic}");
                            await Task.Delay(1000);
                            goto retry;
                        }
                    }).Subscribe(_cancelToken.Token);
                    return _subject;
                });
            }
            return Task.Run(() =>
            {
                while (!_cancelToken.IsCancellationRequested)
                {
                    try
                    {
                        var msg = _consumer.Consume(_cancelToken.Token);
                        _partitionQueues[msg.Partition.Value].Value.OnNext(msg);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, $"Consume Error: {_kafkaSettings.Topic}");
                    }
                }
            });
        }
        private IConsumer<TKey, TValue> ConsumerBuild()
        {
            var consumer = new ConsumerBuilder<TKey, TValue>(_config)
                            .SetErrorHandler((_consumer, err) =>
                            {
                                if (err.IsFatal)
                                {
                                    _logger.LogError($"Fatal: {_kafkaSettings.Topic} {err.Code} {err.Reason}");
                                }
                                else if (err.IsError)
                                {
                                    _logger.LogWarning($"{_kafkaSettings.Topic} {err.Code} {err.Reason}");
                                }
                            })
                            .SetKeyDeserializerOrDefault(_kafkaSettings.KeyDeserializer)
                            .SetValueDeserializerOrDefault(_kafkaSettings.ValueDeserializer)
                            .Build();
            consumer.Subscribe(_kafkaSettings.Topic);
            return consumer;
        }

        private int GetPartitionCount()
        {
            var partitionCount = _kafkaSettings.MaxPartitions;
            var config = new AdminClientConfig
            {
                BootstrapServers = _config.BootstrapServers,
            };
            using var admin = new AdminClientBuilder(config).Build();
            var meta = admin.GetMetadata(_kafkaSettings.Topic, TimeSpan.FromSeconds(30));
            var topicMeta = meta.Topics.FirstOrDefault(r => r.Topic == _kafkaSettings.Topic);
            if (topicMeta != null && topicMeta.Error.Code == Confluent.Kafka.ErrorCode.NoError)
            {
                partitionCount = topicMeta.Partitions.Count;
            }
            if (partitionCount <= 0)
            {
                partitionCount = _kafkaSettings.MaxPartitions;
            }
            return partitionCount;
        }

        public void Stop()
        {
            _cancelToken.Cancel();
            try
            {
                _consumer.Close();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Kafka Consumer Stop error: {_kafkaSettings.Topic}");
            }
            foreach (var subject in _partitionQueues)
            {
                try
                {
                    if (subject.IsValueCreated)
                    {
                        subject.Value.OnCompleted();
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"Kafka Subject Stop error: {_kafkaSettings.Topic}");
                }
            }
        }
    }
}
