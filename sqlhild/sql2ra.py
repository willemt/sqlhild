import logging
import re

from antlr4 import CommonTokenStream, InputStream, Token
from antlr4.error.ErrorListener import ErrorListener
from functools import singledispatch

from .exception import (
    AmbiguousColumn,
    JoinHasNoOnClause,
    TableDoesNotExist,
    UnknownColumn,
)
from .grammar.mysql.MySqlLexer import MySqlLexer
from .grammar.mysql.MySqlParser import MySqlParser
from .relational_algebra import (
    BoolFalse,
    BoolTrue,
    Column,
    ColumnMeta,
    ColumnName,
    Cross,
    Distinct,
    EmptySet,
    Equal,
    Function,
    Intersection,
    Join,
    In,
    LeftJoin,
    Like,
    Limit,
    List,
    Null,
    Number,
    Offset,
    OneRowSet,
    Project,
    QueryContext,
    RightJoin,
    Select,
    String,
    Table,
    TableMetaData,
    Theta,
    Union,
    UniverseSet,
    exp2op,
    pretty_print,
)


logger = logging.getLogger(__name__)


class MyErrorListener(ErrorListener):
    def __init__(self):
        super(MyErrorListener, self).__init__()

    def syntaxError(self, recognizer, offendingSymbol, line, column, msg, e):
        logger.error('{} {} {} {}'.format(line, column, msg, e))
        raise SyntaxError('{line}:{column}\n\t{msg}\n\t{sql}\n\t{error_cursor}'.format(
            line=line,
            column=column,
            msg=msg,
            sql=e.input.getText(),
            error_cursor=' ' * column + '^',
            ))

    # def reportAmbiguity(self, recognizer, dfa, startIndex, stopIndex, exact, ambigAlts, configs):
    #     raise Exception()

    # def reportAttemptingFullContext(self, recognizer, dfa, startIndex, stopIndex, conflictingAlts, configs):
    #     raise Exception()

    # def reportContextSensitivity(self, recognizer, dfa, startIndex, stopIndex, prediction, configs):
    #     raise Exception()


class CaseChangingCharStream(InputStream):
    """
    Allows grammar to parse case-insensitively
    """
    def LA(self, offset: int):
        if offset == 0:
            return 0  # undefined
        if offset < 0:
            offset += 1  # e.g., translate LA(-1) to use offset=0
        pos = self._index + offset - 1
        if pos < 0 or pos >= self._size:  # invalid
            return Token.EOF
        return ord(chr(self.data[pos]).upper())


@singledispatch
def convert_where(ra, ctx):
    """
    Convert WHERE clause in Relational Algebra
    """
    raise Exception(ra)


@convert_where.register
def _(where: MySqlParser.BinaryComparasionPredicateContext, ctx):
    left, op, right = where.getChildren()
    func = exp2op(op.getText())
    func = func(
        convert_where(left, ctx),
        convert_where(right, ctx),
    )
    return Select(ctx.relation, func)


@convert_where.register
def _(where: MySqlParser.PredicateExpressionContext, ctx):
    return convert_where(where.predicate(), ctx)


@convert_where.register
def _(where: MySqlParser.ExpressionAtomPredicateContext, ctx):
    return convert_where(where.expressionAtom(), ctx)


@convert_where.register
def _(where: MySqlParser.FullColumnNameExpressionAtomContext, ctx):
    column = where.fullColumnName()
    assert(isinstance(column, MySqlParser.FullColumnNameContext))
    return ctx.instance._parse_column(column.getText(), ctx.relation)


@convert_where.register
def _(where: MySqlParser.ConstantExpressionAtomContext, ctx):
    constant = where.constant()
    if constant.decimalLiteral():
        return Number(constant.getText())

    elif constant.stringLiteral():
        text = re.sub(r"^'(.*)'$", r'\1', constant.getText())
        return String(text)

    elif constant.booleanLiteral():
        if constant.booleanLiteral().FALSE():
            return BoolFalse()
        elif constant.booleanLiteral().TRUE():
            return BoolTrue()
        else:
            raise NotImplementedError

    elif constant.nullLiteral:
        return Null()
    else:
        raise NotImplementedError


@convert_where.register
def _(where: MySqlParser.LogicalExpressionContext, ctx):
    if where.orLogicalOperator():
        left, right = where.expression()
        return Union(
            convert_where(left, ctx),
            convert_where(right, ctx),
        )
    elif where.andLogicalOperator():
        left, right = where.expression()
        return Intersection(
            convert_where(left, ctx),
            convert_where(right, ctx),
        )
    else:
        raise NotImplementedError


@convert_where.register
def _(where: MySqlParser.NestedExpressionAtomContext, ctx):
    expression = where.expression()
    assert(len(expression) == 1)
    return convert_where(expression[0], ctx)


@convert_where.register
def _(where: MySqlParser.InPredicateContext, ctx):
    """
    X in (...)
    """
    left = convert_where(where.predicate(), ctx)
    right = List(*[
        convert_where(exp, ctx)
        for exp in where.expressions().expression()
    ])
    return Select(ctx.relation, In(left, right))


@convert_where.register
def _(where: MySqlParser.LikePredicateContext, ctx):
    """
    eg. relname like '%'
    """

    left = convert_where(where.predicate()[0], ctx)
    right = convert_where(where.predicate()[1], ctx)

    # Optimization: If the right has no wildcard convert to Equal
    if '%' not in right.val:
        return Select(ctx.relation, Equal(left, right))

    return Select(ctx.relation, Like(left, right))


@convert_where.register
def _(where: MySqlParser.ScalarFunctionCallContext, ctx):
    func_name = where.scalarFunctionName().getText()
    # TODO: parse args
    return Function(String(func_name))


class RelationalAlgebraParser(object):
    def __init__(self, sql_txt, available_tables):
        self.sql_txt = sql_txt

        self.available_tables = available_tables

        # Track the tables in the SQL query
        self._tables_encountered = {}

        self.table_aliases = {}

        self.table_order = []

    def _parse_table_source(self, node):
        if node.tableSourceItem().alias:
            table_alias = node.tableSourceItem().alias.getText()
        else:
            table_alias = ''

        table_name = node.tableSourceItem().tableName().getText()
        table_name = table_name.replace('`', '')

        return self._register_table(table_name, table_alias)

    def _parse_function_call(self, node):
        func_name = node.fullId().getText()
        return Function(String(func_name))
        # node.functionArgs()

    def _parse_SELECT(self, select, ctx):
        from_ = select.querySpecification().fromClause()

        if from_:
            relation = self._parse_FROM(from_, ctx)

        # There is NO FROM clause
        else:
            relation = EmptySet()

        # Get columns from SELECT
        select_columns = []
        for element in select.querySpecification().selectElements().selectElement():

            # TODO: move into dispatch

            # Functions: SELECT ABS(1)
            if isinstance(element, MySqlParser.SelectFunctionElementContext):

                if isinstance(relation, EmptySet):
                    relation = OneRowSet()

                select_columns.append(convert_where(element.functionCall(), ctx))

            # Constants: SELECT '1'
            elif isinstance(element, MySqlParser.SelectExpressionElementContext):
                # TODO: replace with actual constant
                func = Function(String("constant"), convert_where(element.expression(), ctx))
                select_columns.append(func)
                if isinstance(relation, EmptySet):
                    relation = OneRowSet()

            # Star: SELECT a.*
            elif isinstance(element, MySqlParser.SelectStarElementContext):
                table_alias = element.fullId().getText()
                table = self.table_aliases[table_alias]
                table = self.available_tables.get(table.name)
                select_columns.extend(map(ColumnName, [c.identifier for c in table.columns.columns]))

            else:
                try:
                    # column_name = element.fullColumnName().uid().simpleId().getText()
                    column_name = element.fullColumnName().getText()
                except AttributeError:
                    # It's possible to be given NULL as the column name
                    if element.getText() == 'NULL':
                        column_name = 'NULL'
                    else:
                        raise

                select_columns.append(ColumnName(column_name))

        if select_columns:
            relation = Project(relation, *select_columns)
        else:
            # If there are no columns specified in SELECT then we want to base
            # the order of the columns on the table order.
            select_columns = []
            for table in self.table_order:
                table = self.available_tables.get(table.name)
                select_columns.extend([c.identifier for c in table.columns.columns])

            assert(select_columns)

            relation = Project(relation, *map(ColumnName, select_columns))

        # Apply DISTINCT
        if select.querySpecification().selectSpec():
            assert(len(select.querySpecification().selectSpec()) == 1)
            if select.querySpecification().selectSpec()[0].getText().upper() == 'DISTINCT':
                relation = Distinct(relation)

        # select.querySpecification().orderByClause

        # LIMIT
        limit_clause = select.querySpecification().limitClause()
        if limit_clause:
            try:
                offset, limit = map(lambda x: int(x.getText()), limit_clause.decimalLiteral())
            except ValueError:
                limit = int(limit_clause.decimalLiteral()[0].getText())
            else:
                relation = Offset(relation, Number(offset))
            relation = Limit(relation, Number(limit))

        return relation

    def _parse_JOIN(self, node, ctx):
        # alias_obj.simpleId().getText()

        try:
            join_on_expr = node.expression().predicate()
        except AttributeError:
            raise JoinHasNoOnClause

        if join_on_expr.comparisonOperator().getText() != '=':
            raise NotImplementedError('{} operator unsupported for joins'.format(
                join_on_expr.comparisonOperator().getText()))

        column_identifier = join_on_expr.left.expressionAtom().fullColumnName().getText()
        col1 = self._parse_column(column_identifier, None)

        column_identifier = join_on_expr.right.expressionAtom().fullColumnName().getText()
        col2 = self._parse_column(column_identifier, None)

        if hasattr(ctx.relation, 'table_identifier'):
            if col1.table_identifier != ctx.relation.table_identifier:
                col1, col2 = col2, col1

        return col1, col2

    def _parse_FROM(self, node, ctx):
        # Get FROM  tables

        relation = UniverseSet()

        for table_source in node.tableSources().tableSource():
            table = self._parse_table_source(table_source)
            self.table_order.append(table)
            relation = Cross(relation, table)

        ctx.relation = relation

        # Parse JOINs
        joins = node.tableSources().tableSource()[0].joinPart()
        if joins:
            for join in joins:
                if isinstance(join, MySqlParser.OuterJoinContext):
                    if join.LEFT():
                        join_relation = self._parse_table_source(join)
                        try:
                            col1, col2 = self._parse_JOIN(join, ctx)
                        except JoinHasNoOnClause:
                            ctx.relation = Cross(relation, join_relation)
                        else:
                            relation = LeftJoin(
                                Theta(relation, col1),
                                Theta(join_relation, col2))
                            ctx.relation = relation
                    elif join.RIGHT():
                        join_relation = self._parse_table_source(join)
                        try:
                            col1, col2 = self._parse_JOIN(join, ctx)
                        except JoinHasNoOnClause:
                            ctx.relation = Cross(relation, join_relation)
                        else:
                            relation = RightJoin(
                                Theta(relation, col1),
                                Theta(join_relation, col2))
                            ctx.relation = relation
                    else:
                        raise NotImplementedError()
                elif isinstance(join, MySqlParser.InnerJoinContext):
                    if join.INNER() or join.JOIN():
                        join_relation = self._parse_table_source(join)
                        try:
                            col1, col2 = self._parse_JOIN(join, ctx)
                        except JoinHasNoOnClause:
                            ctx.relation = Cross(relation, join_relation)
                        else:
                            if col1.table_identifier == join_relation.table_identifier:
                                col1, col2 = col2, col1
                            relation = Join(
                                Theta(relation, col1),
                                Theta(join_relation, col2))
                            ctx.relation = relation

                    elif join.CROSS():
                        join_relation = self._parse_table_source(join)
                        try:
                            col1, col2 = self._parse_JOIN(join, ctx)
                        except JoinHasNoOnClause:
                            ctx.relation = Cross(relation, join_relation)
                        else:
                            relation = Join(
                                Theta(relation, col1),
                                Theta(join_relation, col2))
                            ctx.relation = relation
                    else:
                        raise NotImplementedError(join.getText())
                else:
                    raise NotImplementedError(join.getText())

            # TODO: multiple joins
            # TODO: LEFT OUTER JOIN

        # Do WHERE
        if not node.whereExpr:
            return ctx.relation

        return self._parse_WHERE(node.whereExpr, ctx)

    def _parse_WHERE(self, where, ctx):
        ctx.instance = self
        return convert_where(where, ctx)

    def parse(self):
        """
        Convert AST into RA
        """
        sql_txt = self.sql_txt

        input = CaseChangingCharStream(sql_txt)
        lexer = MySqlLexer(input)
        stream = CommonTokenStream(lexer)
        parser = MySqlParser(stream)
        parser._listeners = [MyErrorListener()]

        tree = parser.root()

        ctx = QueryContext()
        self.ctx = ctx

        select = tree.sqlStatements().sqlStatement()[0].dmlStatement().selectStatement()
        return self._parse_SELECT(select, ctx)

    def _register_table(self, table_name, table_alias=''):
        # table_name = normalize_tablename(table_name)
        table_metadata = TableMetaData()
        table_metadata.name = table_name

        # Check if we need to create another instance of this table
        try:
            existing_table = self._tables_encountered[table_name]
        except KeyError:
            table_identifier = table_name
        else:
            existing_table.instances += 1
            table_identifier = '{0}^{1}'.format(
                existing_table.name,
                existing_table.instances)

        # Save table metadata
        table_metadata.identifier = table_identifier
        table_metadata.alias = table_alias
        self._tables_encountered[table_metadata.identifier] = table_metadata
        if table_alias:
            self.table_aliases[table_alias] = table_metadata

        return Table(table_identifier)

    def _parse_where_expression(self, where, relation):
        raise NotImplementedError

    def _get_table_from_column_name(self, column_name):
        found_table = None
        for table_name, _ in self._tables_encountered.items():
            tabl = self.available_tables.get(table_name)
            if tabl.columns.contains(column_name):
                if found_table:
                    raise AmbiguousColumn(column_name)
                else:
                    found_table = tabl

        if not found_table:
            raise TableDoesNotExist

        return found_table

    def _parse_column(self, col_identifier, relation):
        """
        Convert column identifier (AST) into RA
        """
        col = ColumnMeta()

        # FIXME: this is probably bad
        col_identifier = col_identifier.replace('`', '')

        if '.' in col_identifier:
            col.alias, col.name = col_identifier.split('.')
        else:
            col.alias = None
            col.name = col_identifier

        # If no table alias then assume we are using one table only
        try:
            table = self.table_aliases[col.alias]
        except KeyError:
            try:
                table = self.available_tables.get(col.alias)
            except TableDoesNotExist:
                # logging.error("unknown table alias '{0}'".format(col.alias))
                try:
                    table = self._get_table_from_column_name(col.name)
                except TableDoesNotExist:
                    raise UnknownColumn(col.name)

            col.table_identifier = table.name
        else:
            col.table_identifier = table.identifier

        return Column(Table(col.table_identifier), ColumnName(col.name))


def sql2ra(antlr, available_tables):
    from .relational_algebra_optimizers import remove_universe_set

    parser = RelationalAlgebraParser(antlr, available_tables)
    ra = parser.parse()
    ra = remove_universe_set(ra)
    ra._tables = parser._tables_encountered

    logger.debug("RA:\n{}".format(pretty_print(ra)))

    return ra
