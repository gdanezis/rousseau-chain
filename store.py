from collections import Sequence
from os.path import exists

class diskHashList(Sequence):
    def __init__(self, filename):
        if not exists(filename):
            fx = open(filename, "w+b")
            fx.close()
        self.f = open(filename, "r+b")

    def append(self, value):
        assert type(value) == bytes and len(value) == 32
        self.f.seek(0, 2) # End of the file
        self.f.write(value)
        self.f.flush()

    def __len__(self):
        self.f.seek(0,2) # move the cursor to the end of the file
        size = self.f.tell()
        return size / 32

    def __getitem__(self, key):
        size = len(self)

        if key < 0:
            key = size + key

        assert type(key) == int and 0 <= key
        self.f.seek(32 * key, 0)
        data = self.f.read(32)
        assert len(data) == 32
        return data

    def __del__(self):
        self.f.flush()
        self.f.close()

