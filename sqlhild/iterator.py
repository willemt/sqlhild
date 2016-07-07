"""Iterators.

Iterators that are yielded from to derive rows from the query plan.
"""

import csv
import functools
import heapq
import itertools
import numba
import operator

from ctypes import pointer
from terminaltables import GithubFlavoredMarkdownTable

from . import function  # NOQA - called by generated functions
from . import relational_algebra
from .column import ColumnRegistry, DataType
from .exception import UnknownColumn
from .utils import StructTuple, TupleTuple


class Iterator(object):
    def __init__(self):
        self.sorted = False
        if hasattr(self, 'name'):
            self.table_name = self.name
        else:
            # logging.warning('Using "_" as table name for {0}'.format(self))
            self.table_name = '_'
        self.columns = ColumnRegistry()
        self.seen = 0

    @property
    def label(self):
        return str(id(self))

    def set_sources(self, sources):
        table_names = set([s.table_name for s in sources if hasattr(s, 'table_name')])
        if len(table_names) == 1:
            self.table_name = list(table_names)[0]

        self.sources = sources

    def replace_source(self, old_source, new_source):
        for i, s in enumerate(self.sources):
            if old_source == s:
                self.sources[i] = new_source
                return
        raise Exception('Source not found')

    def __repr__(self):
        return '<{0}>'.format(self.__class__.__name__)

    def pretty_print(self):
        if self.seen:
            return '{0}\n<{1}>\n{2}'.format(self.__class__.__name__, self.table_name, self.seen)
        else:
            return '{0}\n<{1}>'.format(self.__class__.__name__, self.table_name)

    def _get_next(self, it):
        try:
            return next(it)
        except StopIteration:
            return None

    def finalize(self):
        """
        Prepare to run.
        At this point we are guaranteed to be part of the query plan.
        """
        if hasattr(self, 'sources'):
            for s in self.sources:
                s.finalize()


class SingleSourceIterator(Iterator):
    def __init__(self, source):
        super().__init__()
        self.set_sources([source])
        self.sorted = getattr(source, 'sorted', False)
        self.columns = source.columns.clone()


class Null(Iterator):
    def produce(self):
        return []


class EmptySet(Iterator):
    def __init__(self):
        super(EmptySet, self).__init__()
        self.sorted = True

    def produce(self):
        return []


class OneRowSet(Iterator):
    """
    This is used to make stuff like this work:
        SELECT 1
    """
    def __init__(self):
        super(OneRowSet, self).__init__()
        self.sorted = True

    def produce(self):
        return [()]


class Filter(Iterator):
    """
    Run this operator over the incoming rows
    """
    def __init__(self, source, test):
        self.test = test
        super(Filter, self).__init__()
        self.set_sources([source])
        self.sorted = getattr(self.sources[0], 'sorted', False)
        self.columns = self.sources[0].columns.clone()

    def __repr__(self):
        return '{0}-{1}'.format(self.__class__.__name__, self.test)

    def pretty_print(self):
        return '{0}-{1}\n<{2}>'.format(self.__class__.__name__, self.test, self.table_name)

    def produce(self):
        self.test.determine_columns(self.columns)

        for row in self.sources[0].produce():
            if self.test.run(row):
                yield row


# TODO: REMOVE
class FuncBuilder(object):
    def __init__(self, name, arg_names):
        self.source = 'def {0}({1}):'.format(name, ','.join(arg_names))

    def nl(self, line):
        """
        Add new line
        """
        self.source += '\n\t' + line

    def materialize_variable(self, varname, operand, columns: ColumnRegistry):
        """
        Return lines required to initialise this variable.
        FIXME: should be moved into a sub-classed func builder.
        """
        if isinstance(operand, relational_algebra.Column):
            yield '{0} = row[{1}]'.format(
                varname,
                columns.get_column_idx_from_identifier(
                    operand.column_identifier))
        elif isinstance(operand, relational_algebra.String):
            yield '{0} = bytes("{1}".encode("utf8"))'.format(varname, operand.val)
        elif isinstance(operand, relational_algebra.Number):
            yield '{0} = {1}'.format(varname, operand.val)
        elif isinstance(operand, relational_algebra.Function):
            args = []
            for i, o in enumerate(operand.operands):
                var_name = '{0}f{1}'.format(varname, i)
                if isinstance(o, relational_algebra.Column):
                    yield '{0} = row[{1}]'.format(
                        var_name,
                        columns.get_column_idx_from_identifier(o.column_identifier))
                elif isinstance(o, relational_algebra.Number):
                    yield '{0} = {1}'.format(var_name, o.val)
                else:
                    raise Exception()
                args.append(var_name)

            yield '{0} = function.{1}({2})'.format(varname, operand.name.upper(), ','.join(args))
        else:
            raise Exception('Unknown operand: {}'.format(operand))


class HashJoin(Iterator):
    """
    Joins 2 streams into 1

    The join uses the document ID

    See:
     - Sort vs. Hash Revisited: Fast Join Implementation on Modern Multi-Core CPUs, Changkyu Kim et all
    """

    def __init__(self, a, b, column_a=0, column_b=0):
        super(HashJoin, self).__init__()
        self.set_sources([a, b])
        assert a.columns == b.columns
        self.columns = a.columns.clone()
        self.column_a = column_b
        self.sorted = all([source.sorted for source in self.sources])

    def produce(self):
        ids = set()
        for a in self.sources[0].produce():
            ids.add(a[self.column_a])
        for b in self.sources[1].produce():
            if b[self.column_b] in ids:
                yield b


class HashFilter(Iterator):
    """
    Get the intersection of these two streams
    """

    def __init__(self, a, b):
        super(HashFilter, self).__init__()
        if len(a.columns.columns) < len(b.columns.columns):
            self.set_sources([b, a])
            self.columns = b.columns.clone()
        else:
            self.set_sources([a, b])
            self.columns = a.columns.clone()

        # Find a common column we can filter by
        matching_columns = self.sources[0].columns.matching_columns(self.sources[1].columns)

        if not matching_columns:
            raise Exception("No columns to match on:\n\t{0}\n\t{1}".format(
                self.sources[0].columns,
                self.sources[1].columns))

        self.column_a = self.sources[0].columns.get_column_idx_from_identifier(matching_columns[0][0])
        self.column_b = self.sources[1].columns.get_column_idx_from_identifier(matching_columns[0][0])
        self.sorted = all([source.sorted for source in self.sources])

    def produce(self):
        ids = set(r[self.column_b] for r in self.sources[1].produce())
        for r in self.sources[0].produce():
            if r[self.column_a] in ids:
                yield r


class Cross(Iterator):
    """
    Cross Join
    """
    def __init__(self, a, b):
        super(Cross, self).__init__()
        self.set_sources([a, b])
        self.columns = a.columns + b.columns
        self.sorted = all([source.sorted for source in self.sources])

    def produce(self):
        bs = list(self.sources[1].produce())
        for a in self.sources[0].produce():
            for b in bs:
                self.seen += 1
                yield TupleTuple(a, b)


class CrossOuter(Cross):
    """
    Cross Outer Join
    """
    def produce(self):
        try:
            b, b_ = itertools.tee(self.sources[1].produce())
            b_first_row = next(b_)
        except StopIteration:
            b_first_row = None
        else:
            b = list(b)

        try:
            a, a_ = itertools.tee(self.sources[0].produce())
            a_first_row = next(a_)
        except StopIteration:
            a_first_row = None

        if a_first_row:
            if b_first_row:
                for a_row in a:
                    for b_row in b:
                        yield TupleTuple(a_row, b_row)
            else:
                for a_row in a:
                    # FIXME:
                    yield tuple(a_row)
                    # yield tuple(a_row[:])
        elif b_first_row:
            for b_row in b:
                # FIXME:
                yield tuple(b_row)
                # yield tuple(b_row[:])


# class MergeFilter(HashFilter):
#     """
#     Get the intersection of these two streams
#     """
#
#     def __init__(self, *args, **kwargs):
#         super(MergeFilter, self).__init__(*args, **kwargs)
#         assert all([source.sorted for source in self.sources])
#         self.sorted = True
#
#     def produce(self):
#         _a = self.sources[0].produce()
#         _b = self.sources[1].produce()
#         a = self._get_next(_a)
#         b = self._get_next(_b)
#         count = 0
#         while a and b:
#             if a[self.column_a] < b[self.column_b]:
#                 a = self._get_next(_a)
#             elif a[self.column_a] > b[self.column_b]:
#                 b = self._get_next(_b)
#             else:
#                 yield tuple((count,) + a[:] + b[:])
#                 count += 1
#                 next_a = self._get_next(_a)
#                 next_b = self._get_next(_b)
#
#                 while True:
#                     if next_a and next_a[self.column_a] == b[self.column_b]:
#                         yield next_a
#                         next_a = self._get_next(_a)
#                     elif next_b and a[self.column_a] == next_b[self.column_b]:
#                         yield a
#                         next_b = self._get_next(_b)
#                     else:
#                         break
#                     count += 1
#                 a = next_a
#                 b = next_b


class MergeJoin(Iterator):
    """
    Merge 2 streams (with the same columns) into one
    The join uses the document ID
    Input must be sorted for the join to work.
    Output is sorted
    """

    def __init__(self, a, b):
        super().__init__()
        self.set_sources([a, b])
        self.columns = a.columns
        self.sorted = all([source.sorted for source in self.sources])
        assert self.sorted
        # TODO: assert columns from sources are the same

    def finalize(self):
        super().finalize()

        func_name = 'generated_func'
        code = FuncBuilder(func_name, ['a', 'b'])

        if 1 == len(self.sources[0].columns.table_names):
            code.nl('a = id(a)')
            code.nl('b = id(b)')
        code.nl('if a < b: return -1')
        code.nl('elif b < a: return 1')
        code.nl('elif a == b: return 0')

        exec(code.source)

        func = locals()[func_name]

        jitted_func = numba.jit()(func)

        self.cmp = jitted_func

    def produce(self):
        _a = self.sources[0].produce()
        _b = self.sources[1].produce()
        a = self._get_next(_a)
        b = self._get_next(_b)

        while a and b:
            if self.cmp(a, b) == -1:
                a = self._get_next(_a)
            elif self.cmp(a, b) == 1:
                b = self._get_next(_b)
            else:
                self.seen += 1
                yield a

                _a, __a = itertools.tee(_a)
                next_a = self._get_next(__a)
                while next_a and self.cmp(next_a, b) == 0:
                    self.seen += 1
                    yield a
                    next_a = self._get_next(__a)

                _b, __b = itertools.tee(_b)
                next_b = self._get_next(__b)
                while next_b and self.cmp(a, next_b) == 0:
                    self.seen += 1
                    yield a
                    next_b = self._get_next(__b)

                a = self._get_next(_a)
                b = self._get_next(_b)


class Idize(SingleSourceIterator):
    """
    Prepend ID to rows
    Useful when source doesn't have primary key
    """
    def __init__(self, source):
        super(Idize, self).__init__(source)
        assert not source.has_column_identifier('id')
        self.columns.prepend(source.table_name + '.' + 'id', int)

    def produce(self):
        for i, row in enumerate(self.sources[0].produce()):
            yield tuple([i] + list(row[:]))

            # new_row = self.columns.row_struct()
            # new_row.id = i
            # # FIXME: handle other fields
            # new_row.val = row.contents.val
            # yield pointer(new_row)


class Tuplize(SingleSourceIterator):
    """
    Convert from struct row into tuple row
    """
    def produce(self):
        for row in self.sources[0].produce():
            self.seen += 1
            yield StructTuple(row.contents)
            # yield tuple([getattr(row.contents, c.name, None) for c in self.sources[0].columns.columns])


class Stringify(SingleSourceIterator):
    """
    Convert byte columns into strings
    """
    def produce(self):
        for row in self.sources[0].produce():
            self.seen += 1
            yield [val.decode('utf8') if isinstance(val, bytes) else val for val in row]


class Structize(SingleSourceIterator):
    """
    Convert from tuple into struct
    """
    def produce(self):
        for row in self.sources[0].produce():
            new_row = self.columns.row_struct(*row)
            yield pointer(new_row)


# class TupleProjection(SingleSourceIterator):
#     """
#     """
#     def produce(self):
#         for row in self.sources[0].produce():
#             new_row = self.columns.row_struct(*row)
#             yield pointer(new_row)


class DistinctMerge(Iterator):
    """
    Merge 2 streams (with the same columns) into one and ensure distinctiveness
    Used for combining 2 iterators with the same source table.
    Generally used for ORs
    """
    def __init__(self, a, b, id_column=0):
        super(DistinctMerge, self).__init__()

        if a.columns != b.columns:
            if a.columns & b.columns == set():
                b = SelectColumns(b, list(a.columns.column_name_iter()))
                self.set_sources([a, b])
            else:
                raise Exception('Columns do not match')
        else:
            self.set_sources([a, b])

        # assert self.sources[0].columns == self.sources[1].columns
        # TODO: assert columns from sources are the same
        self.sorted = all([source.sorted for source in self.sources])
        assert self.sorted
        self.columns = self.sources[0].columns
        # self.id_column = id_column

    def produce(self):
        _a = self.sources[0].produce()
        _b = self.sources[1].produce()
        a = self._get_next(_a)
        b = self._get_next(_b)
        while a is not None or b is not None:
            if not a:
                yield b
                b = self._get_next(_b)
            elif not b:
                yield a
                a = self._get_next(_a)
            # elif a[self.id_column] < b[self.id_column]:
            elif a < b:
                yield a
                a = self._get_next(_a)
            # elif a[self.id_column] > b[self.id_column]:
            elif a > b:
                yield b
                b = self._get_next(_b)
            else:
                yield a
                a = self._get_next(_a)
                b = self._get_next(_b)


class SelectColumns(Iterator):
    """
    Only show specific columns
    """
    def __init__(self, source, columns_to_filter_for):
        super(SelectColumns, self).__init__()
        self.set_sources([source])
        self.sorted = getattr(self.sources[0], 'sorted', False)
        self.columns_to_filter_for = columns_to_filter_for
        self.columns = self.sources[0].columns.clone_only_these_columns(columns_to_filter_for)
        self.column_id_to_select = self.sources[0].columns.columnidentifiers_to_columnidxs(self.columns_to_filter_for)

    def produce(self):
        try:
            for row in self.sources[0].produce():
                yield tuple(row[i] for i in self.column_id_to_select)
        except UnknownColumn:
            return


class DictRows2Tuples(SingleSourceIterator):
    """
    Convert rows of dicts into tuples
    """
    def __init__(self, source):
        super(DictRows2Tuples, self).__init__(source)
        self.name = source.name
        self._iter, b = itertools.tee(source.produce())
        first_row = next(b)
        for key, val in first_row.items():
            self.columns.append('{0}.{1}'.format(source.table_name, key), DataType(type(val)))

    @property
    def column_metadata(self):
        return self.columns.column_metadata

    def produce(self):
        # TODO: need a test (for dicts that are missing keys)
        # TODO: should we compile a function to do this?
        for row in self._iter:
            yield tuple([row.get(c.name, None) for c in self.columns.columns])


class DictRows2TuplesFaster(DictRows2Tuples):
    def produce(self):
        for row in self._iter:
            yield tuple(row.values())


class Tuple2Dict(SingleSourceIterator):
    """
    Convert rows of tuples into dicts
    """
    def produce(self):
        for row in self.sources[0].produce():
            yield {
                column.name: row[i]
                for i, column in enumerate(self.columns.columns)
            }


class IterIter(Iterator):
    """
    Just iterators over an iterator
    """
    def __init__(self, source, iter):
        super(IterIter, self).__init__()
        self.columns = ColumnRegistry()
        self.name = source.name
        self.set_sources([source])
        self.sorted = getattr(source, 'sorted', False)
        self._iter = iter

    def produce(self):
        for row in self._iter:
            yield row


class Str2Tuple(Iterator):
    """
    A single string is converted into a tuple of 1
    """
    def __init__(self, source):
        super(Str2Tuple, self).__init__()
        self.columns = ColumnRegistry()
        self.columns.append('value', DataType(str))
        self.set_sources([source])
        self.sorted = getattr(source, 'sorted', False)

    def produce(self):
        for row in self.sources[0].produce():
            yield (row,)


class RowTypeDiscoverer(SingleSourceIterator):
    """
    Sometimes we don't know what the columns for the row are.
    Determine the columns by inspecting the first row.
    """
    def __init__(self, source):
        super(RowTypeDiscoverer, self).__init__(source)
        self.name = source.name
        self._iter, b = itertools.tee(source.produce())

        # TODO: needs test
        # don't explode if the source is empty
        try:
            first_row = next(b)
        except StopIteration:
            self.columns = ColumnRegistry()
            return

        intermediary = IterIter(source, self._iter)

        if isinstance(first_row, dict):
            self.child = DictRows2Tuples(intermediary)
            self.columns = self.child.columns
        elif isinstance(first_row, str):
            self.child = Str2Tuple(intermediary)
            self.columns = self.child.columns
        else:
            raise Exception()

    @property
    def column_metadata(self):
        return self.columns.column_metadata

    def produce(self):
        for row in self.child.produce():
            yield row


class Distinct(SingleSourceIterator):
    """
    Only distinct rows
    """
    def __init__(self, source):
        super(Distinct, self).__init__(source)
        assert(self.sorted)
        assert(len(self.sources) == 1)

    def produce(self):
        previous_row = None
        for row in self.sources[0].produce():
            if previous_row:
                if row != previous_row:
                    yield row
            else:
                yield row
            previous_row = row


class Limit(SingleSourceIterator):
    """
    Only first N rows
    """
    def __init__(self, source, limit):
        super(Limit, self).__init__(source)
        self.limit = limit
        assert(len(self.sources) == 1)

    def produce(self):
        count = 0
        for row in self.sources[0].produce():
            if self.limit <= count:
                break
            yield row
            count += 1


class Offset(SingleSourceIterator):
    """
    Only rows after N rows have been seen
    """
    def __init__(self, source, offset):
        super(Offset, self).__init__(source)
        self.offset = offset
        assert(len(self.sources) == 1)

    def produce(self):
        count = 0
        for row in self.sources[0].produce():
            if self.offset <= count:
                yield row
            count += 1


class Sorted(SingleSourceIterator):
    """
    Sorted stream
    """
    def __init__(self, source):
        super(Sorted, self).__init__(source)
        self.sorted = True
        self.heap = []

    def produce(self):
        for row in self.sources[0].produce():
            heapq.heappush(self.heap, row)

        for i in range(len(self.heap)):
            yield heapq.heappop(self.heap)


class SortedById(SingleSourceIterator):
    """
    Sorted stream
    """
    def __init__(self, source):
        super().__init__(source)
        self.sorted = True
        self.heap = []

    def produce(self):
        for row in self.sources[0].produce():
            # FIXME: need something more generic
            if isinstance(row, TupleTuple):
                heapq.heappush(self.heap, (row, row))
            else:
                heapq.heappush(self.heap, (id(row), row))

        for i in range(len(self.heap)):
            yield heapq.heappop(self.heap)[1]


class Tee(Iterator):
    """
    Duplicate stream
    Used so that we don't scan a source table more than once
    """
    def __init__(self, source=None, source_iterator=None):
        super(Tee, self).__init__()
        self.set_sources([source])
        self.sorted = True
        self.columns = self.sources[0].columns.clone()
        self.heap = []
        self.teed = []
        if source_iterator:
            self.iter, self.original_iterator = itertools.tee(source_iterator)
        else:
            self.iter, self.original_iterator = itertools.tee(self.sources[0].produce())

    def tee(self):
        assert self.seen == 0
        tee = Tee(source=self, source_iterator=self.original_iterator)
        self.teed.append(tee)
        return tee

    def produce(self):
        for i in self.iter:
            self.seen += 1
            yield i


class OrderBy(Iterator):
    """
    Duplicate stream
    """
    def __init__(self, source, column_identifiers):
        self.columns_to_order_by = column_identifiers
        super(OrderBy, self).__init__()
        self.set_sources([source])
        self.sorted = True
        self.columns = self.sources[0].columns.clone()
        self.heap = []

        for ci in column_identifiers:
            if not self.columns.contains(ci):
                raise Exception('{0} does not contain {1}'.format(self.columns.columns, ci))

    def __repr__(self):
        return '<{0}: {1}>'.format(self.__class__.__name__, self.columns_to_order_by)

    def pretty_print(self):
        return '{0}\n {1}'.format(self.__class__.__name__,  self.columns_to_order_by)

    def produce(self):
        column_id_to_select = [
            self.sources[0].columns.get_column_idx_from_identifier(column_identifier)
            for column_identifier in self.columns_to_order_by
            ]

        for row in self.sources[0].produce():
            order_by_item = tuple(row[i] for i in column_id_to_select if row[i] is not None)
            heapq.heappush(self.heap, (order_by_item, row))

        last = None

        for i in range(len(self.heap)):
            k, row = heapq.heappop(self.heap)

            if last:
                assert last <= k
            last = k

            yield row


class InnerMerge(Iterator):
    """
    Merge 2 streams (with different columns) into one.
    Used for Table Joins.

    See: http://www.dcs.ed.ac.uk/home/tz/phd/thesis/node20.htm
    """

    def __init__(self, a, b, col_identifier1=0, col_identifier2=0):
        super(InnerMerge, self).__init__()

        self.set_sources([a, b])

        self.sorted = all([source.sorted for source in self.sources])
        assert(self.sorted)

        self.columns = self.sources[0].columns + self.sources[1].columns

        self.col_identifier1 = col_identifier1
        self.col_identifier2 = col_identifier2

        self.seen = 0

    def _get_next(self, it):
        try:
            return next(it)
        except StopIteration:
            return None

    def pretty_print(self):
        return '{0}\n {1} = {2}\n {3}'.format(
            self.__class__.__name__,
            self.col_identifier1,
            self.col_identifier2,
            self.seen,
        )

    def produce(self):
        # TODO: pre-allocate space for new rows
        _a = self.sources[0].produce()
        _b = self.sources[1].produce()
        a = self._get_next(_a)
        b = self._get_next(_b)
        col_idx1 = self.sources[0].columns.get_column_idx_from_identifier(self.col_identifier1)
        col_idx2 = self.sources[1].columns.get_column_idx_from_identifier(self.col_identifier2)

        while a and b:
            a_val = a[col_idx1]
            b_val = b[col_idx2]

            # Need to handle NULLs
            if a_val is None:
                a = self._get_next(_a)
                continue

            # Need to handle NULLs
            if b_val is None:
                b = self._get_next(_b)
                continue

            if a_val < b_val:
                a = self._get_next(_a)
            elif a_val > b_val:
                b = self._get_next(_b)
            else:
                self.seen += 1
                yield TupleTuple(a, b)

                _a, __a = itertools.tee(_a)
                next_a = self._get_next(__a)
                while next_a and next_a[col_idx1] == b[col_idx2]:
                    self.seen += 1
                    yield TupleTuple(next_a, b)
                    next_a = self._get_next(__a)

                _b, __b = itertools.tee(_b)
                next_b = self._get_next(__b)
                while next_b and a[col_idx1] == next_b[col_idx2]:
                    self.seen += 1
                    yield TupleTuple(a, next_b)
                    next_b = self._get_next(__b)

                a = self._get_next(_a)
                b = self._get_next(_b)

        # Expensive but correct
        # b_items = list(self.sources[1].produce())
        # for a in self.sources[0].produce():
        #     for b in b_items:
        #         if a[col_idx1] == b[col_idx2]:
        #             yield tuple(a[:] + b[:])


class LeftMerge(InnerMerge):
    """
    This is for LEFT OUTER JOIN.
    """

    def produce(self):
        # TODO: pre-allocate space for new rows
        _a = self.sources[0].produce()
        _b = self.sources[1].produce()
        a = self._get_next(_a)
        b = self._get_next(_b)
        col_idx1 = self.sources[0].columns.get_column_idx_from_identifier(self.col_identifier1)
        col_idx2 = self.sources[1].columns.get_column_idx_from_identifier(self.col_identifier2)

        right_empty_tuple = tuple([None for x in range(len(self.sources[1].columns))])

        while a and b:
            if a[col_idx1] < b[col_idx2]:
                self.seen += 1
                yield TupleTuple(a, right_empty_tuple)
                a = self._get_next(_a)
            elif a[col_idx1] > b[col_idx2]:
                b = self._get_next(_b)
            else:
                self.seen += 1
                yield TupleTuple(a, b)

                _a, __a = itertools.tee(_a)
                next_a = self._get_next(__a)
                while next_a and next_a[col_idx1] == b[col_idx2]:
                    self.seen += 1
                    yield TupleTuple(next_a, b)
                    next_a = self._get_next(__a)

                _b, __b = itertools.tee(_b)
                next_b = self._get_next(__b)
                while next_b and a[col_idx1] == next_b[col_idx2]:
                    self.seen += 1
                    yield TupleTuple(a, next_b)
                    next_b = self._get_next(__b)

                a = self._get_next(_a)
                b = self._get_next(_b)


class RightMerge(InnerMerge):
    """
    This is for RIGHT OUTER JOIN.
    """

    def produce(self):
        # TODO: pre-allocate space for new rows
        _a = self.sources[0].produce()
        _b = self.sources[1].produce()
        a = self._get_next(_a)
        b = self._get_next(_b)
        col_idx1 = self.sources[0].columns.get_column_idx_from_identifier(self.col_identifier1)
        col_idx2 = self.sources[1].columns.get_column_idx_from_identifier(self.col_identifier2)

        left_empty_tuple = tuple([None for x in range(len(self.sources[0].columns))])

        while a and b:
            if a[col_idx1] < b[col_idx2]:
                a = self._get_next(_a)
            elif a[col_idx1] > b[col_idx2]:
                yield b
                self.seen += 1
                b = self._get_next(_b)
            else:
                self.seen += 1
                yield TupleTuple(a, b)

                _a, __a = itertools.tee(_a)
                next_a = self._get_next(__a)
                while next_a and next_a[col_idx1] == b[col_idx2]:
                    self.seen += 1
                    yield TupleTuple(next_a, b)
                    next_a = self._get_next(__a)

                _b, __b = itertools.tee(_b)
                next_b = self._get_next(__b)
                while next_b and a[col_idx1] == next_b[col_idx2]:
                    self.seen += 1
                    yield TupleTuple(a, next_b)
                    next_b = self._get_next(__b)

                a = self._get_next(_a)
                b = self._get_next(_b)

        while b:
            yield TupleTuple(left_empty_tuple, b)
            self.seen += 1
            b = self._get_next(_b)


class Writer(object):
    def __init__(self):
        self.lines = []

    def write(self, out):
        self.lines.append(out)

    def next(self):
        return self.lines.pop(0)


class CSVOutput(SingleSourceIterator):
    """
    Output into a CSV format
    """
    def produce(self):
        out = Writer()
        columns = [c.name for c in self.sources[0].columns.columns]
        csv_out = csv.DictWriter(out, columns)

        yield ','.join(columns)

        for row in self.sources[0].produce():
            csv_out.writerow(row)
            yield out.next().strip()


class TableOutput(SingleSourceIterator):
    """
    Output into a table format
    """
    def produce(self):
        data = list(self.sources[0].produce())
        self.seen = len(data)

        # Headers
        # TODO: if there is just one table ignore the full column name
        try:
            data.insert(0, [c.name for c in self.sources[0].columns.columns])
        except AttributeError:
            data.insert(0, ['unknown' for c in self.sources[0].columns.columns])

        yield GithubFlavoredMarkdownTable(data).table


class GroupBy(Iterator):
    """
    Group By aggregation (AKA Stream Aggregate)
    Assumes the source table is sorted by the Group By columns
    """
    def __init__(self, source, column_identifiers):
        self.columns_to_order_by = column_identifiers
        super().__init__()
        self.set_sources([source])
        self.sorted = getattr(self.sources[0], 'sorted', False)
        self.columns = ColumnRegistry()
        for col_identifier in self.columns_to_order_by:
            col = self.sources[0].columns.get_column_from_identifier(col_identifier)
            self.columns.append(col_identifier, col.data_type)
        self.seen = 0

    def produce(self):
        column_id_to_select = [
            self.sources[0].columns.get_column_idx_from_identifier(column_identifier)
            for column_identifier in self.columns_to_order_by
            ]
        current_row = None
        for row in self.sources[0].produce():
            new_row = tuple(row[i] for i in column_id_to_select)
            if current_row != new_row:
                yield new_row
                current_row = new_row


class GroupByHash(GroupBy):
    """
    Group By aggregation (AKA Hash Aggregate)
    This algorithm uses a hash table for grouping
    https://mariadb.com/resources/blog/hash-based-group-strategy-mysql
    http://guyharrison.squarespace.com/blog/2009/8/5/optimizing-group-and-order-by.html
    https://blogs.msdn.microsoft.com/craigfr/2006/09/20/hash-aggregate/
    """
    def finalize(self):
        super().finalize()

        func_name = 'generated_func'

        column_id_to_select = [
            self.sources[0].columns.get_column_idx_from_identifier(column_identifier)
            for column_identifier in self.columns_to_order_by
            ]

        code = FuncBuilder(func_name, ['row'])
        for i, col_id in enumerate(column_id_to_select):
            varname = 'v{0}'.format(i)
            code.nl('{0} = row[{1}]'.format(varname, col_id))

        code.nl('return ({0},)'.format(
            ','.join('v{0}'.format(i) for i, col_id in enumerate(column_id_to_select))))

        exec(code.source)

        func = locals()[func_name]

        self.collect = func

    def produce(self):
        groups = {}
        for row in self.sources[0].produce():
            new_row = self.collect(row)
            groups[new_row] = new_row
        for row in groups.keys():
            yield row


class JittedIterator(Iterator):
    """
    Iterates rows out of a Python function
    """

    def __init__(self, sources, func):
        super().__init__()

        if not isinstance(sources, list):
            sources = [sources]

        self.set_sources(sources)

        self.sorted = all([source.sorted for source in self.sources])
        assert(self.sorted)

        self.columns = functools.reduce(operator.add, [s.columns for s in self.sources])

        self.func = func

    def produce(self):
        iterators = [s.produce() for s in self.sources]
        for row in self.func(*iterators):
            yield row


class PopulateFunctionData(Iterator):
    """
    Populates columns that source their data from functions
    """
    def __init__(self, source, projection_items):
        super().__init__()
        self.set_sources([source])
        self.sorted = getattr(source, 'sorted', False)
        self.columns = source.columns.clone()
        # TODO: set columns
        self.projection_items = projection_items

    def produce(self):
        # TODO: compile program
        for row in self.sources[0].produce():
            for item in self.projection_items:
                func = function.get(item.operands[0].name)
                args = item.operands[1:]
                yield (func(*args), )
