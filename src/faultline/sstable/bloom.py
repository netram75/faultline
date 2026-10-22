"""Bloom filter: a quick way to know a key is NOT in a file.

Most lookups for a key go through several SSTables before they find it, and
most of those files don't have it. Reading a block from each one just to find
nothing is a waste of disk reads. A bloom filter answers "is this key maybe in
here?" from memory:

* "no" is always right, so we skip the file
* "maybe" is usually right, and sometimes wrong (a false positive)

How it works: a row of bits, all 0 at first. Each key is hashed k times and
the k bits it lands on are set to 1. To check a key, hash it the same way.
If any of its bits is still 0, the key was never added.

With 10 bits per key and 7 hashes, about 1% of absent keys get a "maybe".

On disk the filter is just the bit array followed by one byte holding k.
"""

import hashlib
import math

from faultline.sstable.errors import CorruptTable

DEFAULT_BITS_PER_KEY = 10


def _hash_pair(key: bytes) -> tuple[int, int]:
    # Not Python's hash(): for bytes it changes every time the process starts,
    # so a filter saved to disk would be garbage after a restart.
    digest = hashlib.blake2b(key, digest_size=8).digest()
    first = int.from_bytes(digest[:4], "little")
    # Odd step, so the k positions don't all collapse onto the same few bits
    step = int.from_bytes(digest[4:], "little") | 1
    return first, step


def _positions(key: bytes, k: int, num_bits: int) -> list[int]:
    # Two hashes are enough to make k of them: first, first + step,
    # first + 2 * step, ... (Kirsch and Mitzenmacher, 2006)
    first, step = _hash_pair(key)
    return [(first + i * step) % num_bits for i in range(k)]


def build_filter(keys: list[bytes], bits_per_key: int = DEFAULT_BITS_PER_KEY) -> bytes:
    # A tiny filter has too many collisions, so never go below 64 bits
    num_bits = max(64, len(keys) * bits_per_key)
    num_bits = (num_bits + 7) // 8 * 8
    # bits_per_key * ln(2) hashes gives the lowest false positive rate
    k = max(1, min(30, round(bits_per_key * math.log(2))))

    bits = bytearray(num_bits // 8)
    for key in keys:
        for pos in _positions(key, k, num_bits):
            bits[pos // 8] |= 1 << (pos % 8)
    return bytes(bits) + bytes([k])


class BloomFilter:
    def __init__(self, data: bytes) -> None:
        if len(data) < 2 or data[-1] == 0:
            raise CorruptTable("bloom filter is too short or has zero hashes")
        self._bits = data[:-1]
        self._k = data[-1]
        self._num_bits = len(self._bits) * 8

    def may_contain(self, key: bytes) -> bool:
        return all(
            self._bits[pos // 8] & (1 << (pos % 8))
            for pos in _positions(key, self._k, self._num_bits)
        )
