"""Column registry.

Responsible for managing columns inside a query plan.

"""

import ctypes
import datetime
import numpy
import re
import typeguard

from ctypes import Structure, c_int64

from .exception import UnknownColumn, HasNoColumns


def has_common_column(a, b):
    """
    Returns:
        True if A & B have at least one common column
    """
    columns_a = [(c.name, c.data_type) for c in a.columns.columns]
    columns_b = [(c.name, c.data_type) for c in b.columns.columns]
    if list(set(columns_a) & set(columns_b)):
        return True
    return False


def columnidentifier_to_tablename(name):
    """
    Column's table is determined implicitly
    eg. "SELECT value" as opposed to "SELECT a.value" or "SELECT table.value"
    """
    match = re.match(r'^`(.*)`\..*$', name)
    if match:
        return match.groups()[0]

    match = re.match(r'^(.*)\..*$', name)
    if match:
        return match.groups()[0]

    return None


def columnidentifier_to_columnname(name):
    """
    eg. a.value -> value
    """
    match = re.match(r'^`.*`\.(.*)$', name)
    if match:
        return match.groups()[0]

    match = re.match(r'^.*\.(.*)$', name)
    if match:
        return match.groups()[0]

    return name


class DataType(object):
    def __init__(self, provided_type, length=None):

        # TODO:
        if provided_type is str:
            derived_name = 'varchar'
            length = 255
        elif provided_type is int:
            derived_name = 'int'
        elif provided_type is numpy.int64:
            derived_name = 'int'
        elif provided_type is float:
            derived_name = 'float'
        # TODO: determine better name
        elif provided_type is bool:
            derived_name = 'int'
        # TODO: determine better name
        elif provided_type is tuple:
            derived_name = 'varchar'
            length = 255
        # TODO: determine better name
        elif provided_type is list:
            derived_name = 'varchar'
            length = 255
        # TODO: determine better name
        elif provided_type is dict:
            derived_name = 'varchar'
            length = 255
        # TODO: determine better name
        elif provided_type is datetime.datetime:
            derived_name = 'varchar'
            length = 255
        # TODO: determine better name
        # elif provided_type is None:
        #     derived_name = 'varchar'
        #     length = 255
        elif issubclass(provided_type, type(None)):
            derived_name = 'varchar'
            length = 255
        else:
            raise Exception('Unknown type: "{0}"'.format(provided_type))

        # # TODO: remove
        # if not isinstance(provided_type, str):
        #     try:
        #         name = name.__name__
        #     except AttributeError:
        #         pass

        self.name = derived_name.lower()
        self.length = length
        self.provided_type = provided_type

    @property
    def ctype(self):
        if self.name == 'char':
            return ctypes.c_char
        elif self.name == 'varchar':
            return ctypes.c_char * self.length
        elif self.name == 'int':
            return ctypes.c_int64
        elif self.name == 'float':
            return ctypes.c_float
        else:
            raise Exception('Unknown type: "{0}"'.format(self.name))


class ColumnMetaData(object):
    def __init__(self, identifier, data_type: DataType):
        self.identifier = identifier
        self.table_name = columnidentifier_to_tablename(identifier)
        self.name = columnidentifier_to_columnname(identifier)
        self.data_type = data_type

    def __str__(self):
        return '<{0}>'.format(self.identifier)

    def __repr__(self):
        return '<{0}>'.format(self.identifier)


class ColumnRegistry(object):
    """
    Hold metadata about columns
    """
    def __init__(self, table_meta=None):
        self.columns = []
        self.row_struct = type('SubClass', (Structure,), {'_fields_': []})
        self.table_names = set()

        if table_meta:
            for column_name, column_meta in table_meta['columns'].items():
                data_type = DataType(
                    name=column_meta['type']['name'],
                    length=column_meta['type']['length'])
                self.append(column_name, data_type)

    def prepend(self, column_identifier, data_type):
        column = ColumnMetaData(column_identifier, data_type)
        self.columns.insert(0, column)

        # TODO: can be removed?
        # assert len([field[0] for field in rowclass._fields_ if field[0] == "id"])
        self.row_struct = type('SubClass', (Structure,), {
            '_fields_': [(column.name, c_int64)] + list(self.row_struct._fields_)})

    def append(self, column_identifier, data_type: DataType):
        assert typeguard.check_argument_types()

        column = ColumnMetaData(column_identifier, data_type)
        self.columns.append(column)

        self.table_names.add(column.table_name)

        # TODO: can be removed?
        # maintain ctypes struct
        self.row_struct = type('SubClass', (Structure,), {
            '_fields_': list(self.row_struct._fields_) + [(column.name, data_type.ctype)]})

    def contains(self, column_name: str):
        """
        Could be a column identifier: tablename.columnname or alias.columnname
        Could be a column name: columname
        """
        column_name = columnidentifier_to_columnname(column_name)
        for col in self.columns:
            if columnidentifier_to_columnname(col.identifier) == column_name:
                return True
        return False

    def contains_column_identifier(self, column_identifier: str):
        assert('.' in column_identifier)
        for col in self.columns:
            if col.identifier == column_identifier:
                return True
        return False

    def get_column_idx_from_identifier(self, column_identifier):
        """
        Get column index from column identifier
        """
        return self._get_column_from_identifier(column_identifier)[0]

    def _get_column_from_identifier(self, column_identifier):
        # FIXME: should use fully qualified column identifiers everywhere
        # eg. `sqlhild.example.OneToTen`.`val`

        column_identifier = column_identifier.replace('`', '')

        for i, c in enumerate(self.columns):
            if c.identifier == column_identifier:
                return i, c

        # Try again but without the full identifier
        for i, c in enumerate(self.columns):
            if c.identifier.split('.')[-1] == column_identifier:
                return i, c

        # sqlhild.example.OneToTen.val -> OneToTen.val
        match = re.match(r'^.+\.(.*\..*)$', column_identifier)
        if match:
            short_column_identifier = match.groups()[0]
            for i, c in enumerate(self.columns):
                if c.identifier == short_column_identifier:
                    return i, c

        raise UnknownColumn("Unknown column '{0}' in 'field list' in {1}".format(column_identifier, self.columns))

    def get_column_from_identifier(self, column_identifier):
        return self._get_column_from_identifier(column_identifier)[1]

    def clone(self):
        """
        Returns:
            A copy of this ColumnRegistry
        """
        registry = ColumnRegistry()
        for c in self.columns:
            registry.append(c.identifier, c.data_type)
        return registry

    def clone_only_these_columns(self, column_identifiers):
        registry = ColumnRegistry()

        # TODO: Column 'id' in field list is ambiguous

        for identifier in column_identifiers:
            table_name = columnidentifier_to_tablename(identifier)
            if table_name:
                for c in self.columns:
                    if c.identifier == identifier:
                        registry.append(c.identifier, c.data_type)
            else:
                for c in self.columns:
                    if c.name == identifier:
                        registry.append(c.identifier, c.data_type)

        return registry

    def clone_identifized_columns(self, table_name):
        registry = ColumnRegistry()
        for c in self.columns:
            # FIXME: rely on column to identifize itself
            registry.append('{0}.{1}'.format(table_name, c.name), c.data_type)
        return registry

    def __add__(self, other_registry):
        registry = ColumnRegistry()
        for c in self.columns:
            registry.append(c.identifier, c.data_type)
        for c in other_registry.columns:
            registry.append(c.identifier, c.data_type)
        return registry

    def has_common_column(self, other_registry):
        """
        Returns:
            True if source A & B have at least one common column
        """
        columns_a = [(c.identifier, c.data_type) for c in self.columns]
        columns_b = [(c.identifier, c.data_type) for c in other_registry.columns]
        if list(set(columns_a) & set(columns_b)):
            return True
        return False

    def matching_columns(self, other_registry):
        """
        Returns:
            A list of columns that match between this registry and the other
        """
        columns_a = [(c.identifier, c.data_type) for c in self.columns]
        columns_b = [(c.identifier, c.data_type) for c in other_registry.columns]
        return list(set(columns_a) & set(columns_b))

    def columnidentifiers_to_columnidxs(self, column_identifiers):
        """
        Returns:
            A list of column indexes
        """
        column_idxes = []
        for identifier in column_identifiers:
            try:
                column_idxes.append(self.get_column_idx_from_identifier(identifier))
            except UnknownColumn:
                pass

        if not column_idxes:
            raise HasNoColumns

        return column_idxes

    def cast_row(self, row):
        # TODO: this should probably be removed?
        for i, (c, val) in enumerate(zip(self.columns, row)):
            if c.data_type.name == 'char':
                row[i] = bytes(val.encode('utf8'))
            elif c.data_type.name == 'varchar':
                row[i] = bytes(val.encode('utf8'))
            elif c.data_type.name == 'float':
                if val == 'NULL':
                    row[i] = 0
                else:
                    row[i] = float(val)
            else:
                raise Exception()

        return row

    def __len__(self):
        return len(list(self.columns))

    def __eq__(self, other):
        return self.columns == other.columns

    def __and__(self, other):
        return set(self.columns) & set(other.columns)

    def column_name_iter(self):
        """
        Returns:
            An iterator of column names
        """
        for c in self.columns:
            yield c.identifier

    @property
    def column_metadata(self):
        return [(c.identifier, c.data_type.provided_type) for c in self.columns]
