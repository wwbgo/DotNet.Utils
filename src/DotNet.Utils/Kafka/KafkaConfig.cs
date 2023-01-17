using Confluent.Kafka;

namespace DotNet.Utils.Kafka
{
    public class KafkaConfig<TKey, TValue>
    {
        public string Servers { get; set; }
        public string GroupId { get; set; }
        public string Topic { get; set; }
        public AutoOffsetReset AutoOffsetReset { get; set; } = AutoOffsetReset.Earliest;
        public int MaxPartitions { get; set; } = 64;
        public int MaxBufferCount { get; set; } = 10000;

        public Func<IDeserializer<TKey>> KeyDeserializer { get; set; }
        public Func<IDeserializer<TValue>> ValueDeserializer { get; set; }
    }
}
