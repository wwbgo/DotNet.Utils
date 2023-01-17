using Confluent.Kafka;

namespace DotNet.Utils
{
    public static class KafkaExtensions
    {
        public static ConsumerBuilder<TKey, TValue> SetKeyDeserializerOrDefault<TKey, TValue>(this ConsumerBuilder<TKey, TValue> consumerBuilder, Func<IDeserializer<TKey>> deserializer)
        {
            if (deserializer != null)
            {
                consumerBuilder.SetKeyDeserializerOrDefault(deserializer());
            }
            return consumerBuilder;
        }
        public static ConsumerBuilder<TKey, TValue> SetValueDeserializerOrDefault<TKey, TValue>(this ConsumerBuilder<TKey, TValue> consumerBuilder, Func<IDeserializer<TValue>> deserializer)
        {
            if (deserializer != null)
            {
                consumerBuilder.SetValueDeserializerOrDefault(deserializer());
            }
            return consumerBuilder;
        }
        public static ConsumerBuilder<TKey, TValue> SetKeyDeserializerOrDefault<TKey, TValue>(this ConsumerBuilder<TKey, TValue> consumerBuilder, IDeserializer<TKey> deserializer)
        {
            if (deserializer != null)
            {
                consumerBuilder.SetKeyDeserializer(deserializer);
            }
            return consumerBuilder;
        }
        public static ConsumerBuilder<TKey, TValue> SetValueDeserializerOrDefault<TKey, TValue>(this ConsumerBuilder<TKey, TValue> consumerBuilder, IDeserializer<TValue> deserializer)
        {
            if (deserializer != null)
            {
                consumerBuilder.SetValueDeserializer(deserializer);
            }
            return consumerBuilder;
        }
    }
}
