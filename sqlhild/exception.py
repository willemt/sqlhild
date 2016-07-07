class UnknownColumn(Exception):
    pass


class HasNoColumns(Exception):
    pass


class TableDoesNotExist(Exception):
    pass


class TableExists(Exception):
    pass


class EmptySet(Exception):
    pass


class AmbiguousColumn(Exception):
    pass


class UnknownOperator(Exception):
    pass


class ColumnMetadataUndefined(Exception):
    """
    When the "column_metadata" field is not implemented by a table
    """
    pass


class JoinHasNoOnClause(Exception):
    """
    Example: from a inner join b
    """
    pass
