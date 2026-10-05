from collections import OrderedDict, defaultdict


class LFUCache:
    """LFU cache with LRU tiebreak. get and put are both O(1)."""

    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self.key_to_val: dict[int, int] = {}
        self.key_to_freq: dict[int, int] = {}
        # Each bucket is insertion-ordered, so the first key is the LRU one.
        self.freq_to_keys: dict[int, OrderedDict[int, None]] = defaultdict(OrderedDict)
        self.min_freq = 0

    def _touch(self, key: int) -> None:
        """Move key from bucket f to bucket f+1, keeping min_freq correct."""
        freq = self.key_to_freq[key]
        del self.freq_to_keys[freq][key]
        if not self.freq_to_keys[freq]:
            del self.freq_to_keys[freq]  # drop empty buckets so the dict can't grow forever
            if self.min_freq == freq:
                self.min_freq += 1  # safe: nothing can live below the bucket we just emptied
        self.key_to_freq[key] = freq + 1
        self.freq_to_keys[freq + 1][key] = None

    def get(self, key: int) -> int:
        if key not in self.key_to_val:
            return -1
        self._touch(key)
        return self.key_to_val[key]

    def put(self, key: int, value: int) -> None:
        if self.capacity <= 0:
            return

        if key in self.key_to_val:
            self.key_to_val[key] = value
            self._touch(key)  # an update is an access
            return

        if len(self.key_to_val) >= self.capacity:
            evicted, _ = self.freq_to_keys[self.min_freq].popitem(last=False)
            if not self.freq_to_keys[self.min_freq]:
                del self.freq_to_keys[self.min_freq]
            del self.key_to_val[evicted]
            del self.key_to_freq[evicted]

        self.key_to_val[key] = value
        self.key_to_freq[key] = 1
        self.freq_to_keys[1][key] = None
        self.min_freq = 1


if __name__ == "__main__":
    ops = ["LFUCache", "put", "put", "get", "put", "get", "get", "put", "get", "get", "get"]
    args = [[2], [1, 1], [2, 2], [1], [3, 3], [2], [3], [4, 4], [1], [3], [4]]

    output = []
    cache = None
    for op, arg in zip(ops, args):
        if op == "LFUCache":
            cache = LFUCache(*arg)
            output.append(None)
        elif op == "put":
            cache.put(*arg)
            output.append(None)
        elif op == "get":
            output.append(cache.get(*arg))

    expected = [None, None, None, 1, None, -1, 3, None, -1, 3, 4]

    print("Output:  ", output)
    print("Expected:", expected)
    print("Match:   ", output == expected)
