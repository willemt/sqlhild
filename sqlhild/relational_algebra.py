"""Relational algebra operators.

Used to represent a query plan before it is converted into a series of
iterators.

References:
    * https://www.cs.rochester.edu/~nelson/courses/csc_173/relations/algebra.html
    * http://www.databasteknik.se/webbkursen/relalg-lecture/
    * https://en.wikipedia.org/wiki/Relational_algebra
    * http://www.cs.toronto.edu/~faye/343/f07/lectures/wk3/03_RAlgebra.pdf
"""

import autopep8
import logging
import re

from matchpy import (
    Arity,
    Operation,
    Symbol,
)


class QueryContext(object):
    def __init__(self):
        self.relation = None
        self.aggregates = None

    def clone(self):
        ctx = QueryContext()
        ctx.relation = self.relation
        ctx.aggregates = self.aggregates
        return ctx


class Relation(object):
    pass


class Table(Symbol):
    def __init__(self, identifier):
        super().__init__(identifier)
        self.name = identifier
        self.table_identifier = identifier
        self.identifier = identifier


class EmptySet(Symbol, Relation):
    def __init__(self):
        super().__init__(name='∅')


class UniverseSet(Symbol, Relation):
    def __init__(self):
        super().__init__(name='𝕌')


class OneRowSet(Symbol, Relation):
    def __init__(self):
        super().__init__(name='∅*')


class Select(Operation, Relation):
    """
    Returns a relation that has had some rows filtered (AKA WHERE clause)
    """
    name = 'σ'
    arity = Arity.binary
    one_identity = False


class Cross(Operation, Relation):
    """
    Returns a relation that is a result of the cartesian product of more than
    one relations.
    """
    name = 'X'
    arity = Arity.polyadic
    associative = True
    commutative = True
    infix = True
    one_identity = True


class Theta(Operation):
    name = 'θ'
    arity = Arity.binary


class Unique(Operation):
    """
    Returns a relation with no duplicates
    """
    name = 'Unique'
    arity = Arity.unary


class Project(Operation):
    name = 'π'
    arity = Arity.polyadic


class Distinct(Operation):
    name = 'Distinct'
    arity = Arity.unary


class Limit(Operation):
    name = 'Limit'
    arity = Arity.binary


class Offset(Operation):
    name = 'Offset'
    arity = Arity.binary


class Join(Operation, Relation):
    name = 'Join'
    arity = Arity.binary
    commutative = True

    def get_column_identifiers(self):
        return (
            self.operands[0].operands[1].operands[0].name + '.' + self.operands[0].operands[1].operands[1].name,
            self.operands[1].operands[1].operands[0].name + '.' + self.operands[1].operands[1].operands[1].name,
        )


class LeftJoin(Operation, Relation):
    name = 'LeftJoin'
    arity = Arity.binary
    commutative = False

    def get_column_identifiers(self):
        return (
            self.operands[0].operands[1].operands[0].name + '.' + self.operands[0].operands[1].operands[1].name,
            self.operands[1].operands[1].operands[0].name + '.' + self.operands[1].operands[1].operands[1].name,
        )


class RightJoin(Operation, Relation):
    name = 'RightJoin'
    arity = Arity.binary
    commutative = False

    def get_column_identifiers(self):
        return (
            self.operands[0].operands[1].operands[0].name + '.' + self.operands[0].operands[1].operands[1].name,
            self.operands[1].operands[1].operands[0].name + '.' + self.operands[1].operands[1].operands[1].name,
        )


class Union(Operation, Relation):
    name = '⋃'
    arity = Arity.variadic
    associative = True
    commutative = True
    infix = True
    one_identity = True


class Intersection(Operation, Relation):
    name = '⋂'
    arity = Arity.variadic
    associative = True
    commutative = True
    infix = True
    one_identity = True


class GroupBy(Operation, Relation):
    name = 'γ'
    arity = Arity.variadic
    # associative = True
    # commutative = True
    # infix = True
    # one_identity = True

    def get_column_identifiers(self):
        for operand in self.operands[1:]:
            yield operand.operands[0].name + '.' + operand.operands[1].name


# class Difference(Operation, Relation):
#     name = '-'
#     arity = Arity.binary
#     # associative = True
#     # commutative = True
#     infix = True
    # one_identity = True


class Not(Operation, Relation):
    name = '!'
    arity = Arity.variadic
    # associative = True
    # commutative = True
    infix = True
    # one_identity = True


class And(Operation):
    name = '⋀'
    arity = Arity.variadic
    associative = True
    commutative = True
    infix = True
    one_identity = True


class Or(Operation):
    name = '∨'
    arity = Arity.variadic
    associative = True
    commutative = True
    infix = True
    one_identity = True


class Column(Operation):
    name = '.'
    arity = Arity.binary
    infix = True

    @property
    def column_identifier(self):
        return '`{0}`.`{1}`'.format(
            self.operands[0].name,
            self.operands[1].name,
        )

    @property
    def table_identifier(self):
        return '{0}'.format(self.operands[0].name)


class Identifier(Operation):
    name = '.'
    arity = Arity.variadic
    infix = True

    @property
    def value(self):
        return self.operands[0]


class ColumnName(Symbol):
    pass


class Criteria(Symbol):
    pass


class Function(Operation):
    name = 'func'
    arity = Arity.variadic


class Equal(Operation):
    name = '=='
    arity = Arity.binary
    associative = True
    commutative = True
    infix = True
    one_identity = False

    def get_column_identifiers(self):
        return {
            self.operands[0].operands[0].name:
                '{0}.{1}'.format(
                    self.operands[0].operands[0].name,
                    self.operands[0].operands[1].name,
                ),
            self.operands[1].operands[0].name:
                '{0}.{1}'.format(
                    self.operands[1].operands[0].name,
                    self.operands[1].operands[1].name,
                )
        }


class NotEqual(Operation):
    name = '!='
    arity = Arity.binary
    associative = True
    commutative = True
    infix = True
    one_identity = False


class List(Operation):
    name = 'list'
    arity = Arity.variadic
    associative = False
    commutative = True
    infix = False
    one_identity = False


class In(Operation):
    name = 'in'
    arity = Arity.binary
    associative = False
    commutative = False
    infix = True
    one_identity = False


class LessThan(Operation):
    name = '<'
    arity = Arity.binary
    infix = True


class LessThanEqual(Operation):
    name = '<='
    arity = Arity.binary
    infix = True


class GreaterThanEqual(Operation):
    name = '<='
    arity = Arity.binary
    infix = True


class GreaterThan(Operation):
    name = '>'
    arity = Arity.binary
    infix = True


class Like(Operation):
    name = 'like'
    arity = Arity.binary
    infix = True


class EqualNonCommutative(Equal):
    commutative = False
    name = '=='

    def get_column_identifiers(self):
        return (
            '{0}.{1}'.format(
                self.operands[0].operands[0].name,
                self.operands[0].operands[1].name,
            ),
            '{0}.{1}'.format(
                self.operands[1].operands[0].name,
                self.operands[1].operands[1].name,
            )
        )


class Number(Symbol, int):
    def __init__(self, val):
        super().__init__(str(val))
        self.val = val


class String(Symbol, str):
    def __init__(self, val):
        super().__init__(str(val))
        self.val = val


class Bool(Symbol):
    def __init__(self, val):
        super().__init__(str(val))
        self.val = val


class BoolFalse(Operation):
    name = 'false'


class BoolTrue(Operation):
    name = 'true'


class Null(Operation):
    name = 'null'


def normalize_tablename(name):
    return name.rstrip('"').lstrip('"')


class TableMetaData(object):
    def __init__(self):
        self.instances = 1


class ColumnMeta(object):
    pass


def flip_op(op):
    if issubclass(op, Equal):
        return Equal
    elif issubclass(op, LessThan):
        return GreaterThan
    elif issubclass(op, LessThanEqual):
        return GreaterThanEqual
    elif issubclass(op, GreaterThanEqual):
        return LessThanEqual
    elif issubclass(op, GreaterThan):
        return LessThan
    else:
        raise Exception('unknown op: {0}'.format(op))


class UnknownOp(Exception):
    pass


def exp2op(exp):
    if exp == '=':
        return Equal
    elif exp == '<':
        return LessThan
    elif exp == '<=':
        return LessThanEqual
    elif exp == '>=':
        return GreaterThanEqual
    elif exp == '>':
        return GreaterThan
    elif exp == '!=':
        return NotEqual
    elif exp == 'like':
        return Like
    elif exp == 'in':
        return In
    else:
        raise UnknownOp(exp['op'])


class RelationalAlgebraParser(object):
    def __init__(self, ast):
        self.ast = ast

        # Track the tables in the SQL query
        self._tables = {}

        self.table_aliases = {}

    def _parse_arg(self, arg, relation):
        if isinstance(arg, str):
            return String(arg)
        elif isinstance(arg, int):
            return Number(arg)
        elif isinstance(arg, dict):
            if 'identifier' in arg:
                return self._parse_column(arg['identifier'], relation)
            elif 'select' in arg:
                raise Exception()
                # return self._parse_SELECT_query(arg, relation)
            elif 'name' in arg:
                return self._parse_function(arg, relation)

        raise Exception('unknown arg')

    def _parse_function(self, function, relation):
        args = [self._parse_arg(arg, relation) for arg in function['args']]
        func = Function(*args)
        func.name = function['name']
        return func

    def _parse_column(self, col_identifier, ctx):
        """
        Convert column identifier (AST) into RA
        """
        col = ColumnMeta()

        if '.' in col_identifier:
            col.alias, col.name = col_identifier.split('.')
        else:
            col.alias = None
            col.name = col_identifier

        # If no table alias then assume we are using one table only
        try:
            table = self.table_aliases[col.alias]
        except KeyError:
            logging.error("unknown table alias '{0}'".format(col.alias))
            col.table_identifier = ctx.relation.identifier
        else:
            col.table_identifier = table.identifier

        return Column(Table(col.table_identifier), ColumnName(col.name))

    def _parse_SELECT(self, ast, ctx):
        pass

    def _parse_FROM(self, ast, ctx):
        for table in ast['from_']:
            relation = self._register_table(table)

            # Continually build source relation
            if ctx.relation:
                ctx.relation = Cross(ctx.relation, relation)
            else:
                ctx.relation = relation

    def _register_table(self, table_ast):
        table = table_ast
        table_name = table.get('table_name').get('identifier')
        if table.get('table_alias'):
            table_alias = table.get('table_alias').get('identifier', '')
        else:
            table_alias = ''
        table_name = normalize_tablename(table_name)
        table_metadata = TableMetaData()
        table_metadata.name = table_name

        try:
            existing_table = self._tables[table_name]
        except KeyError:
            table_identifier = table_name
        else:
            existing_table.instances += 1
            table_identifier = '{0}^{1}'.format(
                existing_table.name,
                existing_table.instances)

        table_metadata.identifier = table_identifier
        table_metadata.alias = table_alias
        self._tables[table_metadata.identifier] = table_metadata
        if table_alias:
            self.table_aliases[table_alias] = table_metadata

        return Table(table_identifier)

    def _parse_JOIN(self, ast, ctx):
        assert ctx.relation
        for join in ast['join']:
            table = join.get('table')
            relation = self._register_table(table)
            col1 = self._parse_column(join['on']['expression']['args'][0]['identifier'], relation)
            col2 = self._parse_column(join['on']['expression']['args'][1]['identifier'], relation)
            if join['on']['expression']['name'] != '=':
                raise Exception('JOIN operator unsupported')
            if col1.table_identifier != ctx.relation.table_identifier:
                col1, col2 = col2, col1
            ctx.relation = Join(
                Theta(ctx.relation, col1),
                Theta(relation, col2))

    def _parse_GROUPBY(self, ast, ctx):
        columns = [
            self._parse_column(groupby['identifier'], ctx)
            for groupby in ast['groupby']]
        return GroupBy(ctx.relation, *columns)

    def _parse_SELECT_query(self, ast, ctx=None):
        if not ctx:
            ctx = QueryContext()
        else:
            ctx = ctx.clone()

        self._parse_SELECT(ast, ctx)
        self._parse_FROM(ast, ctx)
        self._parse_JOIN(ast, ctx)

        if ast['where']:
            wops = self._build_where(ast['where'], ctx)
            assert(len(wops) == 1)
            relation = wops[0]
        else:
            relation = ctx.relation

        if ast['groupby']:
            return self._parse_GROUPBY(ast, ctx)

        return relation

    def parse(self):
        """
        Convert AST into RA
        """
        ast = self.ast
        return self._parse_SELECT_query(ast)

    def optimize(self, algebra):
        from .relation_algebra_optimizers import optimize
        return optimize(algebra)

    def _parse_expression(self, exp, ctx):
        if exp['name'] == 'EXISTS':
            return Project(ctx.relation, self._parse_SELECT_query(exp['args'][0], ctx))

        args = [self._parse_arg(arg, ctx) for arg in exp['args']]

        try:
            function = exp2op(exp['name'])(*args)
        except UnknownOp:
            function = self._parse_function(exp, ctx)

        return Select(ctx.relation, function)

    def _build_where(self, node, ctx):
        if not node:
            return []

        wops = []

        for clause_name, value in node.items():
            if 'expression' == clause_name:
                wops.append(self._parse_expression(value, ctx))

            elif 'AND' == clause_name:
                sub_wops = [
                    s
                    for sub_node in value
                    for s in self._build_where(sub_node, ctx)
                ]
                wops.append(Intersection(*sub_wops))

            elif 'OR' == clause_name:
                sub_wops = [
                    s
                    for sub_node in value
                    for s in self._build_where(sub_node, ctx)
                ]
                wops.append(Union(*sub_wops))

            elif 'NOT' == clause_name:
                raise Exception()
                sub_wops = [
                    s
                    for sub_node in value
                    for s in self._build_where(sub_node, ctx)
                ]
                wops.append(Intersection(*sub_wops))
                wops.append(Not(*sub_wops))

            elif 'parseinfo' == clause_name:
                pass
            else:
                raise Exception(clause_name, value)

        return wops


def pretty_print(ra):
    ra_string = str(ra)

    # TestA . val -> TestA.val
    ra_string = re.sub(r'(\w+?) \. (\w+?)', r'\1.\2', ra_string)

    # (TestA.val) -> TestA.val
    ra_string = re.sub(r'\((\w+?\.\w+?)\)', r'\1', ra_string)

    ra_string = re.sub(r'(\w+\()', r'\1\n\t\t', ra_string)

    # ra_string = ra_string.replace(',', ',\n\t\t')

    # ra_string = ra_string.replace('((', '(\n\t\t(')
    # ra_string = ra_string.replace('))', ')\n\t\t)')

    # Open onto new line for certain operators
    # ra_string = ra_string.replace('Join(', 'Join(\n\t\t')
    # ra_string = ra_string.replace('⋂', '⋂\n\t\t')
    # ra_string = ra_string.replace('⋃', '⋃\n\t\t')

    # Indentation
    ra_string = autopep8.fix_code(ra_string, options={'select': [
        'E11',
        'E101',
        'E121',
        'E122',
        'E123',
        'E124',
        'E125',
        'E126',
        'E127',
        'E128',
        'E129',
        'E131',
        'E133',
        ]})
    return ra_string
