import operator
import unittest

from sqlalchemy import (
    MetaData,
    create_engine,
    text,
)
from sqlalchemy.sql import and_, or_, not_, select

from hypothesis import given, settings
from hypothesis.strategies import (
    fixed_dictionaries,
    integers,
    just,
    lists,
    one_of,
    recursive,
    sampled_from,
)

from sqlhild.query import go
import sqlhild.query
import sqlhild.table
import sqlhild.example


settings.register_profile("core", deadline=1000)
settings.load_profile("core")


class OneToTen(sqlhild.table.Table):
    sorted = True

    @property
    def column_metadata(self):
        return [('val', int)]

    def produce(self):
        return [(i,) for i in range(1, 11)]


class SQLFuzzHose(object):
    """
    """
    def __init__(self, sqlhild_tables):
        engine = create_engine('sqlite:///:memory:', echo=False)

        class Table(object):
            def __str__(self):
                return self.sqlhild.name

            def __init__(self, sqlhild, sqla):
                self.sqlhild = sqlhild
                self.sqla = sqla

        self.tables = []

        metadata = MetaData()

        for sqlhild_table in sqlhild_tables:
            sqla_table = sqlhild_table.to_sqlalchemy(metadata)
            table = Table(sqlhild_table, sqla_table)
            self.tables.append(table)

        self.fields = []
        for table in self.tables:
            self.fields.extend([just(column) for column in table.sqla.columns] + [integers()])

        self.ops = [
            # operator.lt,
            # operator.le,
            operator.eq,
            # operator.ne,
            # operator.ge,
            # operator.gt,
        ]

        metadata.create_all(engine)

        self.conn = engine.connect()

        # Populate tables
        for table in self.tables:
            fields = [column.name for column in table.sqla.columns]
            for row in list(table.sqlhild.produce()):
                self.conn.execute(table.sqla.insert().values(dict(zip(fields, row))))

    def strategy(self):
        return fixed_dictionaries({
            'table': sampled_from(self.tables),
            'where': recursive(
                fixed_dictionaries({
                    'left': one_of(*self.fields),
                    'op': sampled_from(self.ops),
                    'right': one_of(*self.fields)
                    }),
                lambda children:
                    fixed_dictionaries({'or': lists(children, min_size=1, max_size=3)}) |
                    fixed_dictionaries({'and': lists(children, min_size=1, max_size=3)}),
                    # fixed_dictionaries({'not': lists(children, min_size=1, max_size=1)}),
                )
            }).map(sqlify)

    # def inject_rows(self, data):
    #     fields = ["id", "name", "fullname", "age"]
    #     for row in data:
    #         conn.execute(users.insert().values(dict(zip(fields, row))))


def where_data_to_sqlalchemy(data):
    if 'or' in data:
        return or_(*where_data_to_sqlalchemy(data['or']))
    elif 'and' in data:
        return and_(*where_data_to_sqlalchemy(data['and']))
    elif 'not' in data:
        return not_(*where_data_to_sqlalchemy(data['not']))
    elif isinstance(data, list):
        return [where_data_to_sqlalchemy(d) for d in data]
    else:
        left = data['left']
        right = data['right']

        # if isinstance(left, (int, long)):
        if isinstance(left, (int)):
            left = text("'{0}'".format(left))
        if isinstance(right, (int)):
            right = text("'{0}'".format(right))

        return data['op'](left, right)


sqlhose = SQLFuzzHose([
    OneToTen(),
    sqlhild.example.TwoToTwentyInTwos(),
    sqlhild.example.TestB(),
])


def sqlify(sql):
    return select('*').select_from(sql['table'].sqla).where(where_data_to_sqlalchemy(sql['where']))


class CoreTestCase(unittest.TestCase):
    # @given(sqlhose.strategy().map(sqlify))
    @given(sqlhose.strategy())
    def test_where(self, query):
        self.maxDiff = None

        # Fetch SQLite results
        sqllite_rows = list(sqlhose.conn.execute(query))

        # Convert to query SQLhild format
        query = str(query).replace('"', '`')

        # Fetch SQLhild results
        rows = list(go(query))
        rows = [tuple(row) for row in rows]

        # Compare
        # print('sqlhild:', rows)
        # print('sqllite:', sqllite_rows)
        # TODO: consider if sorting is correct
        #       it probably is correct unless there's an ORDER BY
        try:
            self.assertEqual(sorted(rows), sorted(sqllite_rows))
        except:
            print(query)
            raise


if __name__ == '__main__':
    unittest.main()
