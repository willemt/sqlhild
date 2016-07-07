import functools


@functools.total_ordering
class TupleTuple(object):
    """
    An Adapter that makes N tuples act as a single tuple
    """
    def __init__(self, *tuples):
        tups = []
        for tup in tuples:
            if isinstance(tup, TupleTuple):
                for ttup in tup.tuples:
                    tups.append(ttup)
            else:
                tups.append(tup)

        self.tuples = tups
        self.len = sum(map(len, self.tuples))
        self.ids = tuple(map(id, self.tuples))

    def __len__(self):
        return self.len

    def __getitem__(self, key):
        assert isinstance(key, int)
        for tup in self.tuples:
            if key < len(tup):
                return tup[key]
            key -= len(tup)
        raise IndexError()

    def __iter__(self):
        for tup in self.tuples:
            for i in tup:
                yield i

    def __reversed__(self):
        return None

    def __contains__(self, key):
        try:
            self.__getitem__(key)
        except IndexError:
            return False
        else:
            return True

    def __eq__(self, other):
        return self.ids == other.ids

    def __lt__(self, other):
        return self.ids < other.ids


@functools.total_ordering
class StructTuple(object):
    """
    An Adapter that makes a struct act like a tuple
    The order of the struct's fields is important
    """
    def __init__(self, struct):
        self.struct = struct

    def __len__(self):
        return len(self.struct._fields_)

    def __getitem__(self, key):
        assert isinstance(key, int)
        name, _ = self.struct._fields_[key]
        return getattr(self.struct, name)

    def __iter__(self):
        for name, _ in self.struct._fields_:
            yield getattr(self.struct, name)

    def __reversed__(self):
        return None

    def __contains__(self, key):
        try:
            self.__getitem__(key)
        except IndexError:
            return False
        else:
            return True

    def __eq__(self, other):
        return id(self) == id(other)

    def __lt__(self, other):
        return id(self) == id(other)
