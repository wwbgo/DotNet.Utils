using Confluent.Kafka;

namespace DotNet.Utils.Kafka
{
    public interface IKafkaConsumerHandler<TKey, TValue>
    {
        ValueTask<bool> HandlerAsync(ConsumeResult<TKey, TValue?> result);
        ValueTask<bool> HandlerAsync(IList<ConsumeResult<TKey, TValue>> results);
        ValueTask<bool> CommitAsync();
    }
}
