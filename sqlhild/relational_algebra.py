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


class Number(Operation):
    name = 'N'
    arity = Arity.unary


class Value(Symbol):
    def __init__(self, val):
        super().__init__(val)
        self.val = val


class String(Operation):
    name = 'S'
    arity = Arity.unary


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
