"""Convert Relational Algebra to Python AST.
"""

import astor
import ast
import inspect
import logging
import uuid

from functools import singledispatch

from ast import (
    And,
    Compare,
    FunctionDef,
    Load,
    Name,
    Or,
)

from . import relational_algebra as ras
from . import ast_transformer


logger = logging.getLogger(__name__)


class Context:
    def __init__(self, tables):
        self.tables = tables
        self.imports_required = []


def FILTER_FUNC(rows):
    """
    Loop through data and yield rows that meet criteria
    """
    for row in rows:
        if __cmp_func__(row):  # NOQA
            yield row


def MERGE_EQUI_JOIN_FUNC(rows_left, rows_right):
    import itertools
    from sqlhild.utils import TupleTuple

    def get_next(it):
        try:
            x = next(it)
        except StopIteration:
            return None
        else:
            return x

    _a = iter(rows_left)
    _b = iter(rows_right)
    a = get_next(_a)
    b = get_next(_b)

    while a and b:
        if __cmp_func__(a, b) == -1:  # NOQA
            a = get_next(_a)
        elif __cmp_func__(a, b) == 1:  # NOQA
            b = get_next(_b)
        else:
            yield TupleTuple(a, b)  # NOQA

            _a, __a = itertools.tee(_a)
            next_a = get_next(__a)
            while next_a and __cmp_func__(next_a, b) == 0:  # NOQA
                yield TupleTuple(next_a, b)  # NOQA
                next_a = get_next(__a)

            _b, __b = itertools.tee(_b)
            next_b = get_next(__b)
            while next_b and __cmp_func__(a, next_b) == 0:  # NOQA
                yield TupleTuple(a, next_b)  # NOQA
                next_b = get_next(__b)

            a = get_next(_a)
            b = get_next(_b)


def unique_name(prefix):
    return '{}_{}'.format(prefix, str(uuid.uuid4())).replace('-', '_')


def ast2pyfunc(astbody):
    """
    Compile and retrieve Python function
    """
    code_object = compile(astbody, filename="<ast>", mode="exec")
    exec(code_object)
    return locals()[astbody.body[-1].name]


def single_row_expression_func(ra, ctx: Context):
    """
    Build a function that calcs the expression and returns it
    """

    func_name = unique_name('cmp')

    return_stmt = ast.Return(value=convert(ra, ctx))

    # Make func def
    func_args = [
        ast.arg(arg='row', annotation=None),
    ]
    func_def = FunctionDef(name=func_name, args=ast.arguments(
            args=func_args, vararg=None, kwonlyargs=[], kw_defaults=[], kwarg=None, defaults=[]),
        body=[
            return_stmt,
        ],
        decorator_list=[],
        returns=None)

    ast.fix_missing_locations(func_def)

    logger.debug(astor.to_source(func_def))

    return func_def


@singledispatch
def convert(ra, ctx: Context):
    """
    Convert Relational Algebra into Python Abstract Syntax Tree
    """
    raise Exception(ra)


@convert.register
def _(ra: ras.Column, ctx: Context, var_name='row'):
    column_idx = ctx.tables[ra.operands[0].name].columns.get_column_idx_from_identifier(ra.column_identifier)
    return ast.Subscript(
        value=Name(id=var_name, ctx=Load()),
        slice=ast.Index(value=ast.Num(column_idx)),
        ctx=Load()
    )


def comparison(ra, ctx, op):
    return Compare(
        left=convert(ra[0], ctx),
        ops=[op],
        comparators=[convert(ra[1], ctx)],
    )


@convert.register
def _(ra: ras.LessThan, ctx: Context):
    return comparison(ra, ctx, ast.Lt())


@convert.register
def _(ra: ras.LessThanEqual, ctx: Context):
    return comparison(ra, ctx, ast.LtE())


@convert.register
def _(ra: ras.GreaterThan, ctx: Context):
    return comparison(ra, ctx, ast.Gt())


@convert.register
def _(ra: ras.GreaterThanEqual, ctx: Context):
    return comparison(ra, ctx, ast.GtE())


@convert.register
def _(ra: ras.Equal, ctx: Context):
    return comparison(ra, ctx, ast.Eq())


@convert.register
def _(ra: ras.Like, ctx: Context):
    # The wildcard function
    query = ra[1].val.replace('%', '*')

    ctx.imports_required.append(
        ast.ImportFrom(module='fnmatch', names=[ast.alias(name='fnmatch', asname=None)], level=0)
    )

    func = Name(id='fnmatch', ctx=Load())
    left = convert(ra[0], ctx)
    return ast.Call(func=func, args=[left, ast.Str(s=query)], keywords=[])


@convert.register
def _(ra: ras.Number, ctx: Context):
    return ast.Num(int(ra.val))


@convert.register
def _(ra: ras.String, ctx: Context):
    return ast.Str(ra.val)


@convert.register
def _(ra: ras.And, ctx: Context):
    return ast.BoolOp(op=And(), values=[
        convert(ra[0], ctx),
        convert(ra[1], ctx),
      ])


@convert.register
def _(ra: ras.Or, ctx: Context):
    return ast.BoolOp(op=Or(), values=[
        convert(ra[0], ctx),
        convert(ra[1], ctx),
      ])


@convert.register
def _(ra: ras.Select, ctx: Context):
    # Function + Loop body
    filter_ast = ast.parse(inspect.getsource(FILTER_FUNC))
    filter_func = filter_ast.body[0]
    filter_func.name = unique_name('filter')

    # Create comparison
    cmp_ast = single_row_expression_func(ra.operands[1], ctx)

    # Inject comparison function defintion into Loop body
    filter_func.body.insert(0, cmp_ast)

    for _import in ctx.imports_required:
        filter_func.body.insert(0, _import)

    filter_ast = ast_transformer.FindAndReplaceNames({
        '__cmp_func__': cmp_ast,
        }).visit(filter_ast)

    ast.fix_missing_locations(filter_ast)

    logger.debug(astor.to_source(filter_ast))

    return filter_ast


@convert.register
def _(ra: ras.Join, ctx: Context):
    # Function
    body_ast = ast.parse(inspect.getsource(MERGE_EQUI_JOIN_FUNC))

    # Loop body
    merge_func_ast = body_ast.body[0]
    merge_func_ast.name = unique_name('merge')

    # Create row comparison
    func_name = unique_name('cmp')
    func_args = [
        ast.arg(arg='left', annotation=None),
        ast.arg(arg='right', annotation=None),
    ]
    # val = ast.BinOp(
    #     left=convert(ra.operands[0].operands[1], ctx, var_name='left'),
    #     op=ast.Sub(),
    #     right=convert(ra.operands[1].operands[1], ctx, var_name='right'),
    # )
    # func_return = ast.Return(value=val)
    left = convert(ra.operands[0].operands[1], ctx, var_name='left')
    right = convert(ra.operands[1].operands[1], ctx, var_name='right')
    if_statement = ast.If(test=Compare(left=left, ops=[
        ast.Lt(),
      ], comparators=[
        right,
      ]), body=[
        ast.Return(value=ast.UnaryOp(op=ast.USub(), operand=ast.Num(n=1))),
      ], orelse=[
        ast.If(test=Compare(left=left, ops=[
            ast.Gt(),
          ], comparators=[
            right,
          ]), body=[
            ast.Return(value=ast.Num(n=1)),
          ], orelse=[
            ast.Return(value=ast.Num(n=0)),
          ]),
      ])
    func_cmp = FunctionDef(name=func_name, args=ast.arguments(
            args=func_args, vararg=None, kwonlyargs=[], kw_defaults=[], kwarg=None, defaults=[]),
        body=[if_statement],
        decorator_list=[],
        returns=None)
    merge_func_ast.body.insert(0, func_cmp)

    # Inject functions
    body_ast = ast_transformer.FindAndReplaceNames({
        '__cmp_func__': func_cmp,
        }).visit(body_ast)

    ast.fix_missing_locations(body_ast)

    logger.debug(astor.to_source(body_ast))

    return body_ast
