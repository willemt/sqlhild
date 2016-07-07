import attr
import collections
import json
import lmdb
import logging
import os
import uuid
import typing

from . import column
from . import iterator
from . import optimizer
from . import sql2ra
from . import relational_algebra_optimizers
from . import ra2iter
from . import table
from .exception import (
    TableDoesNotExist,
    TableExists,
)


logger = logging.getLogger(__name__)


@attr.s(auto_attribs=True)
class TableRegistry(dict):
    tables: typing.Dict[str, table.Table] = attr.Factory(dict)
    table_aliases: typing.Dict[str, str] = attr.Factory(dict)

    def __setitem__(self, key, item):
        self.tables[key] = item

    def __getitem__(self, key):
        try:
            return self.tables[key]
        except KeyError:
            try:
                return self.tables[self.table_aliases[key]]
            except KeyError:
                raise TableDoesNotExist(key)

    def append(self, identifier, name, alias):
        if not alias:
            alias = '_'

        self.table_aliases[alias] = identifier
        self.table_aliases[identifier] = identifier

        try:
            module, new_identifier = table.extract_module_and_name(identifier)
        except AttributeError:
            pass
        else:
            self.table_aliases[new_identifier] = identifier

        # logger.debug("Registering table: {0} (AKA {1}) as {2}".format(identifier, name, alias))

        tabl = table.get(name)

        # Check if we're provided a row of dicts
        # try:
        #     tabl.column_metadata
        # except AttributeError:
        #     tabl = iterator.DictRows2Tuples(tabl)

        # if not tabl.tuples:
        #     tabl = iterator.Tuplize(tabl)

        # TODO: only structize if rows are tuples
        # tabl = iterator.Structize(tabl)

        # if not tabl.has_column_identifier('id'):
        #     tabl = iterator.Idize(tabl)

        self.tables[identifier] = tabl

    def get(self, identifier):
        return self.tables[identifier]


class QueryPlan(object):
    def __init__(self):
        self.ast = None
        self.source = None
        self.table_aliases = {}
        self.tables = TableRegistry()
        self.db = lmdb.open('sqlhild.lmdb', max_dbs=255)

    @property
    def columns(self):
        return self.source.columns

    def _get_db_table(self, identifier, table_name):
        """
        Load a table from our LMDB database
        """
        table_name_ = bytes(table_name.encode('utf8'))
        table_meta_db = self.db.open_db(b'__tables')
        with self.db.begin(write=True) as txn:
            table_metadata = txn.get(table_name_, db=table_meta_db)
            if table_metadata:
                return table.LMDBTable(identifier, table_name, json.loads(table_metadata))
            else:
                return None

    def _select(self, ra, dumpast=False):
        # if dumpast:
        #     logger.debug(json.dumps(ast.asjson(), indent=2))

        for table_name, tabl in ra._tables.items():
            self.tables.append(tabl.identifier, tabl.name, tabl.alias)

        source = ra2iter.ra2iter(ra, self.tables)

        source = iterator.Stringify(source)
        # source = iterator.Tuplize(source)

        # if ast['select']['columns'] != '*':
        #     column_identifiers = [
        #         column['identifier']
        #         for column in ast['select']['columns']
        #         if 'identifier' in column]
        #     source = iterator.SelectColumns(source, column_identifiers)
        #
        # if ast['select']['distinct']:
        #     source = iterator.Distinct(iterator.Sorted(source))
        #
        # if ast['orderby']:
        #     columns = [x['expression'] for x in ast['orderby']]
        #     source = iterator.OrderBy(source, columns)

        self.source = source

        # TODO: optimizer should run multiple times
        optimizers = [
            optimizer.TeeRemover,
            # optimizer.TeeRemover,
            # optimizer.TeeRemover,
            # optimizer.TeeRemover,
            # optimizer.JITify,
        ]
        for opti in optimizers:
            o = opti()
            self.source = o.process(self.source)

    def _create_table(self, ast):
        table_name = bytes(ast['table']['table_name'].encode('utf8'))
        table_meta_db = self.db.open_db(b'__tables')
        table_meta = {
            'columns': {
                column['name']: {
                    'type': {
                        'name': column['type']['name'],
                        'length': column['type']['length'],
                        }
                    }
                for column in ast['columns']
            }
        }
        logger.debug("Creating table: {0}".format(ast['table']['table_name']))
        with self.db.begin(write=True) as txn:
            if txn.get(table_name, db=table_meta_db):
                raise TableExists(table_name)
            txn.put(table_name, json.dumps(table_meta).encode('utf8'), db=table_meta_db)

        # Insert columns
        tabl = table.SQLHildColumn()
        db = self.db.open_db(b'SQLHildColumn')
        with self.db.begin(write=True) as txn:
            for i, col in enumerate(ast['columns']):
                column_identifier = '{0}.{1}'.format(
                    ast['table']['table_name'],
                    col['name'],
                )
                row = tabl.column_registry.row_struct(
                    table_name,
                    bytes(col['name'].encode('utf8')),
                    bytes(col['type']['name'].encode('utf8')),
                    col['type']['length'] or 0,
                    i,
                )
                txn.put(bytes(column_identifier.encode('utf8')), row, db=db)

    def _drop_table(self, ast):
        table_name = bytes(ast['table']['table_name'].encode('utf8'))
        table_meta_db = self.db.open_db(b'__tables')
        table_db = self.db.open_db(table_name)
        with self.db.begin(write=True) as txn:
            # if not txn.get(table_name, db=table_meta_db):
            #     raise TableDoesNotExist(table_name)
            txn.delete(table_name, db=table_meta_db)
            txn.drop(table_db, delete=False)

    def __get_db_table_columns(self, table_name):
        """
        Query the SQLHildCOlumn for all table columns
        """
        source = iterator.Stringify(iterator.Tuplize(table.SQLHildColumn()))
        ordered_source = iterator.OrderBy(source, ['position'])
        for row in iterator.Tuple2Dict(ordered_source).produce():
            if row['table_name'] == table_name:
                yield row

    def _insert(self, ast):
        columns = list(self.__get_db_table_columns(ast['table']['table_name']))

        table_name = bytes(ast['table']['table_name'].encode('utf8'))
        table_db = self.db.open_db(table_name)

        table_meta_db = self.db.open_db(b'__tables')

        with self.db.begin(write=True) as txn:

            # TODO: remove table_meta
            table_meta = txn.get(table_name, db=table_meta_db)
            if not table_meta:
                raise TableDoesNotExist(table_name)

            # registry = column.ColumnRegistry(json.loads(table_meta))
            registry = column.ColumnRegistry()
            for col in columns:
                registry.append(
                    col['name'],
                    column.DataType(col['type'], col['type_length'])
                )

            for row in ast['tuples']:
                # TODO: Replace with JITified cast()
                row = registry.cast_row(row)
                key = uuid.uuid4()
                row_data = registry.row_struct(*row)
                txn.put(key.bytes, row_data, db=table_db)
                # TODO: calculate max length of item

    def process(self, sql_text, dumpast=False):
        """
        Process SQL string and prepare Query Plan
        """

        ra = sql2ra.sql2ra(sql_text, table)

        if 0 < int(os.environ.get('SQLHILD_OPTIMIZATION_LEVEL', 5)):
            ra = relational_algebra_optimizers.optimize(ra)

        # self.ast = ast

        self._select(ra, dumpast=dumpast)

        # if ast.get('select', None):
        #     self._select(ra, dumpast=dumpast)
        # elif ast.get('unlock_tables', None):
        #     pass
        # elif ast.get('lock_tables', None):
        #     pass
        # elif ast.get('create_table', None):
        #     self._create_table(ast['create_table'])
        # elif ast.get('drop_table', None):
        #     self._drop_table(ast['drop_table'])
        # elif ast.get('insert', None):
        #     self._insert(ast['insert'])
        # else:
        #     assert False

    def produce(self):
        """
        Yield all rows
        """
        self.source.finalize()
        return self.source.produce()

    def _show(self, it, direction, indent=1):
        for it2 in getattr(it, 'sources', []):
            direction[it].add(it2)
            self._show(it2, direction, indent=indent + 1)

    def output_queryplan(self):
        """
        Produce human readable Query Plan
        """
        try:
            __import__('graphviz')
        except ImportError:
            return

        direction = collections.defaultdict(set)
        self._show(self.source, direction)

        from graphviz import Digraph
        dot = Digraph(comment='')
        for f in direction.keys():
            dot.node(f.label, f.pretty_print())

        for k, v in direction.items():
            for other in v:
                dot.edge(k.label, other.label)

        dot.render('queryplan.gv', view=True)


def do_sqlite_run(sql_text):
    """
    Run this query over a sqlite database.
    This is just for debugging purposes
    """
    from terminaltables import GithubFlavoredMarkdownTable
    from sqlalchemy import (
        MetaData,
        create_engine,
    )
    engine = create_engine('sqlite:///:memory:', echo=False)
    metadata = MetaData()
    tables = []
    for name, t in table._tables.items():
        if name not in ['OneToTen', 'TestB', 'TwoToTwentyInTwos']:
            continue
        if name in ['Table']:
            continue
        t_sqla = t().to_sqlalchemy(metadata)
        t_sqla.sqlhild = t()
        tables.append(t_sqla)
    metadata.create_all(engine)

    conn = engine.connect()

    # Populate tables
    for t in tables:
        fields = [column.name for column in t.columns]
        for row in list(t.sqlhild.produce()):
            conn.execute(t.insert().values(dict(zip(fields, row))))

    r = list(conn.execute(sql_text))

    print(GithubFlavoredMarkdownTable(r).table)
    return r


def go(
        sql_text,
        pretty_print=False,
        queryplan=False,
        dumpast=False,
        output_csv=False,
        sqlite_run=False,
        ):

    if sqlite_run:
        return do_sqlite_run(sql_text)

    q = QueryPlan()
    q.process(sql_text, dumpast=dumpast)

    if not pretty_print:
        return list(q.produce())

    # Figure out final iterator for the destination
    try:
        get_ipython  # NOQA
        # q.source = iterator.Tuple2Dict(iterator.Tuple2Dict(q.source))
    except:
        if output_csv:
            q.source = iterator.CSVOutput(iterator.Tuple2Dict(q.source))
        else:
            q.source = iterator.TableOutput(q.source)
        if not pretty_print:
            return list(q.produce())

    # Output
    try:
        get_ipython  # NOQA
    except:
        for row in q.produce():
            print(row)
    else:
        from IPython.display import HTML, display
        import tabulate
        display(HTML(tabulate.tabulate(
            q.produce(),
            headers=[c.identifier for c in q.source.columns.columns],
            tablefmt='html')))

    if queryplan:
        q.output_queryplan()

    logger.info('{0} row(s)'.format(q.source.seen))

    return None
