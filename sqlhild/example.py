import numpy

from . import table


class Process(table.Table):
    def produce(self):
        import psutil
        return [p.as_dict(attrs=['pid', 'name', 'username', 'status'])
                for p in psutil.process_iter()]


class Users(table.Table):
    def produce(self):
        import psutil
        return [o._asdict() for o in psutil.users()]


class DiskPartitions(table.Table):
    def produce(self):
        import psutil
        return [o._asdict() for o in psutil.disk_partitions()]


class TestA(table.Table):
    sorted = True
    tuples = True

    @property
    def column_metadata(self):
        return [
            ('id', numpy.int64),
            ('val', numpy.int64),
        ]

    def produce(self):
        return ((i, i * 2) for i in range(1, 10))


class TestC(table.Table):
    sorted = True
    tuples = True

    @property
    def column_metadata(self):
        return [('val', numpy.unicode_)]

    def produce(self):
        return iter([
            {'val': 'A'},
            {'val': 'B'},
            {'val': 'D'},
            {'val': 'E'},
            ])


class TestD(table.Table):
    sorted = True

    @property
    def column_metadata(self):
        return [('val', numpy.unicode_)]

    def produce(self):
        return iter([
            {'val': 'A'},
            {'val': 'A'},
            {'val': 'B'},
            {'val': 'B'},
            {'val': 'C'},
            {'val': 'D'},
            {'val': 'E'},
            {'val': 'F'},
            ])


class TestB(table.Table):
    sorted = True

    @property
    def column_metadata(self):
        return [
            ('id', numpy.int64),
            ('val', numpy.int64),
        ]

    def produce(self):
        return ((i, i * 2) for i in range(1, 13))


class ThreeToSeven(table.Table):
    sorted = True

    @property
    def column_metadata(self):
        return [('val', numpy.int64)]

    def produce(self):
        return numpy.array([(i,) for i in range(3, 8)], dtype=self.numpy_dtype)


class OneToTen(table.Table):
    sorted = True

    @property
    def column_metadata(self):
        return [('val', numpy.int64)]

    def produce(self):
        return numpy.array([(i,) for i in range(1, 11)], dtype=self.numpy_dtype)


class TwoToTwentyInTwos(table.Table):
    sorted = True

    @property
    def column_metadata(self):
        return [('val', numpy.int64)]

    def produce(self):
        return [(i,) for i in range(2, 6, 2)]
