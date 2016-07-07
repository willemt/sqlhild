"""Relation Alegbra to Iterator.
Convert relational algebra to an iterator
"""

from functools import reduce, singledispatch

from . import exception
from . import iterator
from . import ra2ast
from . import relational_algebra as ra


def _do_join(a, tables, join_type):
    source1 = ra2iter(a.operands[0].operands[0], tables)
    source2 = ra2iter(a.operands[1].operands[0], tables)
    col_identifiers = a.get_column_identifiers()
    return join_type(
        iterator.OrderBy(source1, [col_identifiers[0]]),
        iterator.OrderBy(source2, [col_identifiers[1]]),
        col_identifiers[0],
        col_identifiers[1],
        )


@singledispatch
def ra2iter(a, tables):
    """
    Convert Relational Algebra into Iterator
    """
    raise Exception(a)


@ra2iter.register
def _(a: ra.Table, tables):
    identifier = a.table_identifier
    table = tables[identifier]
    if not hasattr(table, 'tee'):
        table = iterator.Tee(table)
        tables[identifier] = table
    else:
        tables[identifier] = table.tee()
    return tables[identifier]


@ra2iter.register
def _(a: ra.Select, tables):
    source = ra2iter(a.operands[0], tables)
    ast = ra2ast.convert(a, ra2ast.Context(tables))
    py_func = ra2ast.ast2pyfunc(ast)
    return iterator.JittedIterator(source, py_func)


@ra2iter.register
def _(a: ra.Join, tables):
    ast = ra2ast.convert(a, ra2ast.Context(tables))
    py_func = ra2ast.ast2pyfunc(ast)

    col_identifiers = a.get_column_identifiers()

    source1 = ra2iter(a.operands[0].operands[0], tables)
    source1 = iterator.OrderBy(source1, [col_identifiers[0]])

    source2 = ra2iter(a.operands[1].operands[0], tables)
    source2 = iterator.OrderBy(source2, [col_identifiers[1]])

    return iterator.JittedIterator([source1, source2], py_func)


@ra2iter.register
def _(a: ra.LeftJoin, tables):
    return _do_join(a, tables, iterator.LeftMerge)


@ra2iter.register
def _(a: ra.RightJoin, tables):
    return _do_join(a, tables, iterator.RightMerge)


@ra2iter.register
def _(a: ra.Cross, tables):
    return reduce(
        lambda a, b: iterator.Cross(a, ra2iter(b, tables)),
        a.operands[1:],
        ra2iter(a.operands[0], tables))


@ra2iter.register
def _(a: ra.Intersection, tables):
    return reduce(
        lambda a, b: iterator.MergeJoin(a, iterator.SortedById(ra2iter(b, tables))),
        a.operands[1:],
        iterator.SortedById(ra2iter(a.operands[0], tables)))


@ra2iter.register
def _(a: ra.Union, tables):
    assert(len(a.operands) == 2)
    return iterator.DistinctMerge(
        iterator.SortedById(ra2iter(a.operands[0], tables)),
        iterator.SortedById(ra2iter(a.operands[1], tables))
    )
    # return functools.reduce(
    #     lambda a, b: iterator.DistinctMerge(a, iterator.SortedById(ra2iter(b, tables))),
    #     a.operands[1:],
    #     iterator.SortedById(ra2iter(a.operands[0], tables)))


@ra2iter.register
def _(a: ra.GroupBy, tables):
    col_identifiers = list(a.get_column_identifiers())
    return iterator.GroupByHash(ra2iter(a.operands[0], tables), col_identifiers)
    # source = iterator.OrderBy(ra2iter(a.operands[0], tables), col_identifiers)
    # return iterator.GroupBy(source, col_identifiers)


@ra2iter.register
def _(a: ra.Distinct, tables):
    # TODO: we should only put Sorted in when the source needs it
    return iterator.Distinct(iterator.Sorted(ra2iter(a.operands[0], tables)))


@ra2iter.register
def _(a: ra.Project, tables):
    relation = ra2iter(a.operands[0], tables)

    # TODO: build iterator that populates function data
    # for o in a.operands[1:]:
    #     if isinstance(o, ra.function):

    # Is there a function in SELECT?
    for op in a.operands[1:]:
        if isinstance(op, ra.Function):
            relation = iterator.PopulateFunctionData(relation, a.operands[1:])
            break

    try:
        return iterator.SelectColumns(
            relation,
            [o.name for o in a.operands[1:]]
        )
    except exception.HasNoColumns:
        return relation
    # iterator.Distinct(col_identifiers[0]),


@ra2iter.register
def _(a: ra.EmptySet, tables):
    return iterator.EmptySet()


@ra2iter.register
def _(a: ra.OneRowSet, tables):
    return iterator.OneRowSet()


@ra2iter.register
def _(a: ra.Offset, tables):
    return iterator.Offset(ra2iter(a.operands[0], tables), a.operands[1].val)


@ra2iter.register
def _(a: ra.Limit, tables):
    return iterator.Limit(ra2iter(a.operands[0], tables), a.operands[1].val)
