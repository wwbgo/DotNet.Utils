using Confluent.Kafka;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotNet.Utils.Kafka
{
    public static class KafkaServiceExtensions
    {
        public static IServiceCollection ConfigureKafka(this IServiceCollection services, IConfiguration config)
        {
            services.Configure<AppSettings>(config.GetSection("App"));
            services.Configure<AppSettings.KafkaSettings>(config.GetSection("App:Kafka"));

            //var kafkaSettings = config.GetSection("App:Kafka").Get<AppSettings.KafkaSettings>();

            services.AddTransient<IKafkaMetrics, KafkaMetrics>();

            return services;
        }
        public static IServiceCollection AddKafka<TKey, TValue, TKafkaHandler>(this IServiceCollection services, string topic, Func<IDeserializer<TKey>> keyDeserializer = default, Func<IDeserializer<TValue?>> valueDeserializer = default) where TKafkaHandler : class, IKafkaConsumerHandler<TKey, TValue?>
        {
            services.AddTransient<IKafkaConsumerHandler<TKey, TValue?>, TKafkaHandler>();
            services.AddSingleton<IKafkaConsumer, KafkaConsumer<TKey, TValue?, IKafkaConsumerHandler<TKey, TValue?>>>(_services =>
            {
                var appSettings = _services.GetRequiredService<IOptions<AppSettings>>().Value;
                var kafkaSettings = _services.GetRequiredService<IOptions<AppSettings.KafkaSettings>>().Value;
                var config = new KafkaConfig<TKey, TValue?>
                {
                    Servers = kafkaSettings.Servers,
                    GroupId = $"{kafkaSettings.GroupId}_{appSettings.ClusterId}",
                    MaxPartitions = kafkaSettings.MaxPartitions,
                    Topic = kafkaSettings.TopicPrefix + topic,
                    KeyDeserializer = keyDeserializer,
                    ValueDeserializer = valueDeserializer,
                };
                var logger = _services.GetService<ILogger<KafkaConsumer<TKey, TValue?, IKafkaConsumerHandler<TKey, TValue?>>>>();
                return new KafkaConsumer<TKey, TValue?, IKafkaConsumerHandler<TKey, TValue?>>(config, _services, logger);
            });
            return services;
        }

        public static IApplicationBuilder UseKafka(this IApplicationBuilder app)
        {
            var kafkas = app.ApplicationServices.GetServices<IKafkaConsumer>();
            foreach (var kafka in kafkas)
            {
                kafka.Start();
            }

            return app;
        }
    }
}
