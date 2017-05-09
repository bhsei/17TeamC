from hashlib import *
import math


class Filters(object):

    A = [ # used to do | or & operator when set bit or check bit
          0b10000000, 0b01000000,   #128, 64
          0b00100000, 0b00010000,   #32,  16
          0b00001000, 0b00000100,   #8,   4
          0b00000010, 0b00000001]   #2,   1

    hash_funcs = [md5, sha1, sha256, sha512, sha224, md5, sha1, sha256, sha512, sha224]

    def __init__(self, n_items, error_rate=0.001):
        m_length = self._m_length(n_items, error_rate)
        self.n_hash = self._n_hash(m_length, n_items)
        print "bits : {}    hash : {}".format(m_length, self.n_hash)
        assert self.n_hash < len(self.hash_funcs),\
                "bad argument; there are only {} hash functions provided".format(len(self.hash_funcs))

        n = int(m_length/8 + 1)
        self.n = n * 8
        self.m = bytearray(n)

    def add(self, item):
        results = map(self._add(item), self.hash_funcs[:self.n_hash])
        return bool(True in results)

    def __contains__(self, item):
        for func in self.hash_funcs[:self.n_hash]:
            n_byte, n_bit = self._bit(func, item)
            if not self._has(n_byte, n_bit):
                return False
        return True

    def _add(self, item):
        def _set(hash_func):
            n_byte, n_bit = self._bit(hash_func, item)
            return self._set_bit(n_byte, n_bit)
        return  _set

    def _bit(self, hash_func, item):
        hash_value = int(hash_func(item).hexdigest(), 16)
        t = hash_value % self.n
        return t/8, t%8

    def _set_bit(self, n_byte, n_bit):
        if self._has(n_byte, n_bit):
            return False
        else:
           self.m[n_byte] = self.m[n_byte] | self.A[n_bit]
           return True

    def _has(self, n_byte, n_bit):
        return bool(self.m[n_byte] & self.A[n_bit])

    def _m_length(self, n_items, error_rate):

        n, p = n_items, error_rate
        m_length = -(n * math.log(p)) / math.pow(math.log(2), 2)   # 0.7 ~= In2
        return m_length

    def _n_hash(self, m_length, n_items):
        return int(math.log(2) * (m_length / n_items))   # 0.7 ~= In2


if __name__ == "__main__":
    f = Filters(10000,0.001)
    f.add("helloworld")
    print "helloworld" in f
    f.add("hello")
    print "hello" in f
    print "world" in f

