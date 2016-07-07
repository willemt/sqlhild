import ctypes
import importlib
import lmdb
import numpy
import re
import sqlalchemy

from . import column
from . import iterator
from .exception import (
    ColumnMetadataUndefined,
    TableDoesNotExist,
)


_tables = {}


# FIXME: replace with setup tools
class TableWatcher(type):
    """
    Register tables for use in FROM and JOIN clauses
    """

    def __init__(cls, name, bases, clsdict):
        # logging.debug("Registering table: {0}".format(name))
        try:
            _tables[cls._name] = cls
        except AttributeError:
            _tables[name] = cls
        super(TableWatcher, cls).__init__(name, bases, clsdict)


class AbstractTable(iterator.Iterator):
    tuples = False

    """
    Used in FROM and JOIN clauses
    """
    def __init__(self, identifier=None):
        self.identifier = getattr(self, 'identifier', self.name)

        if not self.identifier:
            self.identifier = identifier

        super(AbstractTable, self).__init__()

    def determine_columns(self):
        if not hasattr(self, 'column_metadata'):
            raise ColumnMetadataUndefined()
        for c in self.column_metadata:
            self.columns.append(
                column_identifier=self.identifier + '.' + c[0],
                data_type=c[1] if isinstance(c[1], column.DataType) else column.DataType(c[1]))

    @property
    def name(self):
        if hasattr(self, '_name'):
            return self._name
        else:
            return self.__class__.__name__

    @property
    def label(self):
        return self.name

    # @property
    # def columns(self):
    #     return [(self.name + '.' + c[0], c[1]) for c in self.column_metadata]

    def produce(self):
        """
        Iterator that yields all rows in any order
        Rows MUST be a tuple of values
        """
        raise NotImplementedError()

    @property
    def numpy_dtype(self):
        return numpy.dtype([
            ('{}.{}'.format(self.name, name), datatype)
            for name, datatype in self.column_metadata
            ])

    def to_sqlalchemy(self, metadata):
        columns = []
        for name, datatype in self.column_metadata:
            # FIXME: needs a generic number type
            if datatype in [int, numpy.int64]:
                datatype = sqlalchemy.Integer
            # else:
            #     raise Exception()
                columns.append(sqlalchemy.Column(name, datatype))
        return sqlalchemy.Table(
            self.table_name,
            metadata,
            *columns
            )


class Table(AbstractTable, metaclass=TableWatcher):
    __metaclass__ = TableWatcher


class LMDBTable(AbstractTable):
    def __init__(self, identifier, name, metadata):
        self.column_registry = column.ColumnRegistry(table_meta=metadata)
        self.table_name = name
        super(LMDBTable, self).__init__(identifier)
        self.metadata = metadata
        self.db = lmdb.open('sqlhild.lmdb', max_dbs=255)

    @property
    def name(self):
        return self.table_name

    @property
    def column_metadata(self):
        return [
            (col.name, col.data_type)
            for col in self.column_registry.columns
        ]

    def produce(self):
        table_name = bytes(self.table_name.encode('utf8'))
        table_db = self.db.open_db(table_name)
        with self.db.begin(write=False) as txn:
            cursor = txn.cursor(db=table_db)
            for k, v in cursor:
                yield ctypes.pointer(self.column_registry.row_struct.from_buffer_copy(v))


class SQLHildTable(Table):
    """
    All the tables contained within LMDB
    """
    def __init__(self):
        self.column_registry = column.ColumnRegistry()
        for c in self.column_metadata:
            self.column_registry.append(*c)
        super().__init__()
        self.db = lmdb.open('sqlhild.lmdb', max_dbs=255)

    @property
    def column_metadata(self):
        return [
            ('name', column.DataType(str, 100))
        ]

    def produce(self):
        table_db = self.db.open_db()
        with self.db.begin() as txn:
            cursor = txn.cursor(db=table_db)
            for k, v in cursor:
                yield ctypes.pointer(self.column_registry.row_struct(k))


class SQLHildColumn(Table):
    """
    All the columns contained within LMDB
    """
    def __init__(self):
        self.column_registry = column.ColumnRegistry()
        for c in self.column_metadata:
            self.column_registry.append(*c)
        super().__init__()
        self.db = lmdb.open('sqlhild.lmdb', max_dbs=255)

    @property
    def column_metadata(self):
        return [
            ('table_name', column.DataType('varchar', 100)),
            ('name', column.DataType('varchar', 100)),
            ('type', column.DataType('varchar', 100)),
            ('type_length', column.DataType('int')),
            ('position', column.DataType('int')),
        ]

    def produce(self):
        table_name = bytes(self.table_name.encode('utf8'))
        table_db = self.db.open_db(table_name)
        with self.db.begin() as txn:
            for k, v in txn.cursor(db=table_db):
                yield ctypes.pointer(self.column_registry.row_struct.from_buffer_copy(v))


class __Table(Table):
    """
    All the tables contained within LMDB
    """
    @property
    def column_metadata(self):
        return [
            ('name', column.DataType(str, 100))
        ]

    def produce(self):
        for table_name in _tables.keys():
            yield [table_name]


def import_module(module_name):
    # TODO: detect if it's a module name or pat
    spec = importlib.util.spec_from_file_location("x", module_name)
    if spec:
        foo = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(foo)
    else:
        importlib.import_module(module_name)


def extract_module_and_name(name):
    return re.match(r'(.*)\.(.*?)$', name).groups()


def get(name):
    try:
        table_class = _tables[name]
    except KeyError:
        # Attempt to load a module based off the table's name
        try:
            module_name, table_name = extract_module_and_name(name)
            import_module(module_name)
            table_class = _tables[table_name]
        except:
            raise TableDoesNotExist(name)

    tabl = table_class()

    try:
        tabl.determine_columns()
    except ColumnMetadataUndefined:
        return iterator.RowTypeDiscoverer(tabl)

    return tabl
