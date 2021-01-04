import concurrent.futures
import progressbar


def map_concurrency(
    func, iterator, func_args=(), func_kwargs={}, max_workers=10
):
    results = []
    bar = progressbar.ProgressBar(max_value=len(iterator))
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max_workers
    ) as executor:
        # Start the load operations and mark each future with its URL
        future_to_url = {
            executor.submit(func, i, *func_args, **func_kwargs): i
            for i in iterator
        }
        count = 0
        for future in concurrent.futures.as_completed(future_to_url):
            data = future.result()
            results.append(data)
            count += 1
            bar.update(count)
    return results
