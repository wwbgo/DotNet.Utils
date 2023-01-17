using System.Collections.Concurrent;
using System.Reactive.Linq;

namespace DotNet.Utils
{
    public static class IObservableExtensions
    {
        public static IObservable<TResult> SelectAsync<TSource, TResult>(this IObservable<TSource> source, Func<TSource, Task<TResult>> selector, int maxConcurrent = 1)
        {
            return source.Select(r => Observable.FromAsync(() => selector(r))).Merge(maxConcurrent);
        }
        public static IObservable<TSource> DoAsync<TSource>(this IObservable<TSource> source, Func<TSource, Task> selector, int maxConcurrent = 1)
        {
            return source.Select(r => Observable.FromAsync(async () =>
            {
                await selector(r);
                return r;
            })).Merge(maxConcurrent);
        }
        public static IObservable<TSource> WithAsync<TSource>(this IObservable<TSource> source, Func<TSource, Task> selector)
        {
            return source.Select(r => Observable.FromAsync(async () =>
            {
                await selector(r);
                return r;
            })).Concat();
        }

        public static IObservable<T> Limit<T>(this IObservable<T> source, int maxCount = 10000, CancellationToken cancellationToken = default)
        {
            return Limit<T>(source, null, maxCount, cancellationToken);
        }
        public static IObservable<T> Limit<T>(this IObservable<T> source, Func<T, Task> onNext, int maxCount = 10000, CancellationToken cancellationToken = default)
        {
            return Observable.Using(() => new BlockingCollection<T>(maxCount), queue =>
            {
                if (cancellationToken == default)
                {
                    cancellationToken = CancellationToken.None;
                }
                source.Do(r => queue.Add(r, cancellationToken)).Subscribe(cancellationToken);
                return Observable.Create<T>(subscribe =>
                {
                    Task.Run(async () =>
                    {
                        while (!cancellationToken.IsCancellationRequested)
                        {
                            var data = queue.Take(cancellationToken);
                            if (onNext != null)
                            {
                                await onNext(data);
                            }
                            subscribe.OnNext(data);
                        }
                    }).ConfigureAwait(false);
                    return () =>
                    {
                        subscribe.OnCompleted();
                    };
                });
            });
        }
        public static IObservable<IList<T>> Batch<T>(this IObservable<T> source, TimeSpan timeSpan, int count)
        {
            return source.Buffer(timeSpan, count).Where(r => r.Any());
        }
    }
}
