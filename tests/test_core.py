# -*- coding: utf-8 -*-
import unittest

from sqlhild.exception import (
    TableDoesNotExist,
    UnknownColumn,
)
from sqlhild.query import go
from sqlhild.table import Table


"""
TODO: AmbiguousColumn
TODO: SyntaxError
TODO: add inner join test with duplicates
TODO: multi JOIN
TODO: JOIN without ON clause
TODO: WHERE X in (1, 2, 3)
TODO: WHERE X like Y
TODO: SELECT NULL
TODO: SELECT a.*
"""


class OneToFive(Table):
    sorted = True
    tuples = True

    @property
    def column_metadata(self):
        return [('val', int)]

    def produce(self):
        return ((i,) for i in range(1, 6))


class OneToTen(Table):
    sorted = True
    tuples = True

    @property
    def column_metadata(self):
        return [('val', int)]

    def produce(self):
        return ((i,) for i in range(1, 11))


class TableA(Table):
    sorted = True
    tuples = True

    @property
    def column_metadata(self):
        return [('val', int)]

    def produce(self):
        return ((i,) for i in range(1, 10))


class TableB(Table):
    sorted = True
    tuples = True

    @property
    def column_metadata(self):
        return [('val', int)]

    def produce(self):
        return ((i,) for i in range(5, 15))


class TableC(Table):
    tuples = True

    @property
    def column_metadata(self):
        return [('val', str)]

    def produce(self):
        return iter([
            ('A',),
            ('A',),
            ('B',),
            ('C',),
            ('D',),
        ])


class CoreTests(unittest.TestCase):
    def test_list_all_items(self):
        rows = list(go(u"SELECT * FROM OneToFive"))
        self.assertEqual(len(rows), 5)
        self.assertEqual(rows[0][0], 1)
        self.assertEqual(rows[1][0], 2)
        self.assertEqual(rows[2][0], 3)
        self.assertEqual(rows[3][0], 4)
        self.assertEqual(rows[4][0], 5)

    def test_select_column(self):
        rows = list(go(u"SELECT val FROM OneToFive"))
        self.assertEqual(len(rows), 5)
        self.assertEqual(len(rows[0]), 1)
        self.assertEqual(rows[0][0], 1)
        self.assertEqual(rows[1][0], 2)
        self.assertEqual(rows[2][0], 3)
        self.assertEqual(rows[3][0], 4)
        self.assertEqual(rows[4][0], 5)

    def test_select_nonexisting_column_gives_error(self):
        with self.assertRaises(UnknownColumn):
            list(go(u"SELECT nonexisting FROM OneToFive"))

    def test_select_nonexisting_table(self):
        with self.assertRaises(TableDoesNotExist):
            list(go(u"SELECT * FROM NonExistingTable"))

    def test_gt(self):
        rows = list(go(u"SELECT * FROM OneToFive WHERE 3 < val"))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][0], 4)
        self.assertEqual(rows[1][0], 5)

    def test_gt2(self):
        rows = list(go(u"SELECT * FROM OneToFive WHERE val > 3"))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][0], 4)
        self.assertEqual(rows[1][0], 5)

    def test_lt(self):
        rows = list(go(u"SELECT * FROM OneToFive WHERE 3 > val"))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][0], 1)
        self.assertEqual(rows[1][0], 2)

    def test_lt2(self):
        rows = list(go(u"SELECT * FROM OneToFive WHERE val < 3"))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][0], 1)
        self.assertEqual(rows[1][0], 2)

    def test_and(self):
        rows = list(go("""
        SELECT *
        FROM OneToTen
        WHERE
            val > 4
            AND val < 6
        """))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], 5)

    def test_or(self):
        rows = list(go("""
        SELECT *
        FROM OneToTen
        WHERE
            val = 5
            OR val = 6
        """))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][0], 5)
        self.assertEqual(rows[1][0], 6)

    def test_alias(self):
        rows = list(go("""
        SELECT *
        FROM OneToTen as a
        WHERE a.val = 5
        """))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], 5)

    def test_table_name_as_alias(self):
        rows = list(go("""
        SELECT *
        FROM OneToTen as a
        WHERE OneToTen.val = 5
        """))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], 5)

    def test_nested_or(self):
        rows = list(go("""
        SELECT *
        FROM OneToTen
        WHERE
            (val = 5 OR val = 6) OR val = 1
        """))
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0][0], 1)
        self.assertEqual(rows[1][0], 5)
        self.assertEqual(rows[2][0], 6)

    def test_gt_and_lt(self):
        rows = list(go("""
        SELECT *
        FROM OneToTen
        WHERE val < 9 and 5 < val
        """))
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0][0], 6)
        self.assertEqual(rows[1][0], 7)
        self.assertEqual(rows[2][0], 8)

    def test_column_equal_column(self):
        rows = list(go("""
        SELECT *
        FROM OneToTen
        WHERE
            `OneToTen`.val = `OneToTen`.val
        """))
        self.assertEqual(len(rows), 10)
        self.assertEqual(rows[0][0], 1)
        self.assertEqual(rows[1][0], 2)
        self.assertEqual(rows[2][0], 3)
        self.assertEqual(rows[3][0], 4)
        self.assertEqual(rows[4][0], 5)
        self.assertEqual(rows[5][0], 6)
        self.assertEqual(rows[6][0], 7)
        self.assertEqual(rows[7][0], 8)
        self.assertEqual(rows[8][0], 9)
        self.assertEqual(rows[9][0], 10)

    def test_distinct(self):
        rows = list(go("""
        SELECT distinct *
        FROM TableC
        """))
        self.assertEqual(len(rows), 4)
        self.assertEqual(rows[0], ['A', ])
        self.assertEqual(rows[1], ['B', ])
        self.assertEqual(rows[2], ['C', ])
        self.assertEqual(rows[3], ['D', ])

    def test_distinct_on_absolute_val(self):
        rows = list(go("""
        SELECT distinct TableC.val
        FROM TableC
        """))
        self.assertEqual(len(rows), 4)
        self.assertEqual(rows[0], ['A', ])
        self.assertEqual(rows[1], ['B', ])
        self.assertEqual(rows[2], ['C', ])
        self.assertEqual(rows[3], ['D', ])

    def test_distinct_on_short_val(self):
        rows = list(go("""
        SELECT distinct val
        FROM TableC
        """))
        self.assertEqual(len(rows), 4)
        self.assertEqual(rows[0], ['A', ])
        self.assertEqual(rows[1], ['B', ])
        self.assertEqual(rows[2], ['C', ])
        self.assertEqual(rows[3], ['D', ])

    def test_quoted_table_name(self):
        rows = list(go(u'SELECT * FROM `OneToFive` WHERE 2 < val'))
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0][0], 3)
        self.assertEqual(rows[1][0], 4)
        self.assertEqual(rows[2][0], 5)

    def test_false(self):
        rows = list(go("""
        SELECT *
        FROM TableC
        WHERE false
        """))
        self.assertEqual(len(rows), 0)

    def test_or_false(self):
        rows = list(go("""
        SELECT *
        FROM TableC
        WHERE `TableC`.val = `TableC`.val OR false
        """))
        self.assertEqual(len(rows), 5)
        self.assertEqual(rows[0], ['A', ])
        self.assertEqual(rows[1], ['A', ])
        self.assertEqual(rows[2], ['B', ])
        self.assertEqual(rows[3], ['C', ])
        self.assertEqual(rows[4], ['D', ])

    def test_and_false(self):
        rows = list(go("""
        SELECT *
        FROM TableC
        WHERE `TableC`.val = `TableC`.val AND false
        """))
        self.assertEqual(len(rows), 0)

    def test_xxx(self):
        rows = list(go("""
        SELECT *
        FROM OneToTen
        WHERE `OneToTen`.val = `OneToTen`.val OR `OneToTen`.val = '0' OR `OneToTen`.val = '0'
        """))
        self.assertEqual(len(rows), 10)

    def test_inner_join(self):
        rows = list(go("""
        SELECT *
        FROM OneToFive as a
        INNER JOIN OneToTen as b on a.val = b.val
        """))
        self.assertEqual(rows, [[1, 1], [2, 2], [3, 3], [4, 4], [5, 5]])

    def test_left_outer_join(self):
        rows = list(go("""
        SELECT *
        FROM OneToFive as a
        LEFT OUTER JOIN OneToTen as b on a.val = b.val
        """))
        self.assertEqual(rows, [[1, 1], [2, 2], [3, 3], [4, 4], [5, 5]])

    def test_right_outer_join(self):
        rows = list(go("""
        SELECT *
        FROM OneToFive as a
        RIGHT OUTER JOIN OneToTen as b on a.val = b.val
        """))
        self.assertEqual(rows, [
            [1, 1],
            [2, 2],
            [3, 3],
            [4, 4],
            [5, 5],
            [None, 6],
            [None, 7],
            [None, 8],
            [None, 9],
            [None, 10]
        ])

    def test_limit(self):
        rows = list(go("""
        SELECT *
        FROM OneToFive as a
        LIMIT 3
        """))
        self.assertEqual(rows, [[1], [2], [3]])

    def test_offset(self):
        rows = list(go("""
        SELECT *
        FROM OneToFive as a
        LIMIT 3, 2
        """))
        self.assertEqual(rows, [[4], [5]])

    def test_integer_column_coerces_string_into_integer(self):
        rows = list(go("""
        SELECT *
        FROM `OneToTen`
        WHERE `OneToTen`.val = '1'
        """))
        self.assertEqual(rows, [[1]])


if __name__ == "__main__":
    unittest.main()
