import sys
read machine-sensors

#sliding for minimum
def sliding_min(arr, k=60):
    lowest = sys.maxsize
    n = len(arr)
    for i in range(n - k + 1):
        window_min = min(arr[i:i+k])
        lowest = min(lowest, window_min)
    return lowest

# sliding for max
def sliding_max(arr, k=60):
    highest = -sys.maxsize
    n = len(arr)
    for i in range(n - k + 1):
        window_max = max(arr[i:i+k])
        highest = max(highest, window_max)
    return highest

#tumbling average
from collections import defaultdict

def tumbling_average(events, window_size=60):
    buckets = defaultdict(list)
    for ts, value in events:
        window_id = ts // window_size
        buckets[window_id].append(value)

    averages = {}
    for window_id, values in buckets.items():
        averages[window_id] = sum(values) / len(values)
    return averages



#tumbling count
def tumbling_count_average(values, window_size=60):
    """
    values: list of numbers
    window_size: number of elements per window
    """
    averages = []
    for i in range(0, len(values), window_size):
        window = values[i:i+window_size]
        avg = sum(window) / len(window)
        averages.append(avg)
    return averages
