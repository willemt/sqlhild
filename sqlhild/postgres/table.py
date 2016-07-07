from .. import table
from . import function  # NOQA - put functions in namespace


class pg_type(table.Table):
    @property
    def column_metadata(self):
        return [
            ('oid', str),  # NOTE: added for sqlhild
            ('typname', str),  # 'name'),
            ('typnamespace', str),  # 'oid'),
            ('typowner', str),  # 'oid'),
            ('typlen', str),  # 'int2'),
            ('typbyval', str),  # 'bool'),
            ('typtype', str),  # 'char'),
            ('typcategory', str),  # 'char'),
            ('typispreferred', str),  # 'bool'),
            ('typisdefined', str),  # 'bool'),
            ('typdelim', str),  # 'char'),
            ('typrelid', str),  # 'oid'),
            ('typelem', str),  # 'oid'),
            ('typarray', str),  # 'oid'),
            ('typinput', str),  # 'regproc'),
            ('typoutput', str),  # 'regproc'),
            ('typreceive', str),  # 'regproc'),
            ('typsend', str),  # 'regproc'),
            ('typmodin', str),  # 'regproc'),
            ('typmodout', str),  # 'regproc'),
            ('typanalyze', str),  # 'regproc'),
            ('typalign', str),  # 'char'),
            ('typstorage', str),  # 'char'),
            ('typnotnull', str),  # 'bool'),
            ('typbasetype', str),  # 'oid'),
            ('typtypmod', str),  # 'int4'),
            ('typndims', str),  # 'int4'),
            ('typdefaultbin', str),  # 'text'),
            ('typdefault', str),  # 'text'),
        ]

    def produce(self):
        return []


class pg_namespace(table.Table):
    _name = 'pg_catalog.pg_namespace'

    @property
    def column_metadata(self):
        return [
            ('oid', str),
            ('nspname', str),
            ('nspowner', str),
            ('nspacl', str),
        ]

    def produce(self):
        return []


class pg_class(table.Table):
    _name = 'pg_catalog.pg_class'

    @property
    def column_metadata(self):
        return [
            ('oid', str),
            ('relname', str),
            ('relnamespace', str),
            ('reltype', str),
            ('reloftype', str),
            ('relowner', str),
            ('relam', str),
            ('relfilenode', str),
            ('reltablespace', str),
            ('relpages', str),
            ('reltuples', str),
            ('relallvisible', str),
            ('reltoastrelid', str),
            ('relhasindex', str),
            ('relisshared', str),
            ('relpersistence', str),
            ('relkind', str),
            ('relnatts', str),
            ('relchecks', str),
            ('relhasoids', str),
            ('relhaspkey', str),
            ('relhasrules', str),
            ('relhastriggers', str),
            ('relhassubclass', str),
            ('relispopulated', str),
            ('relreplident', str),
            ('relfrozenxid', str),
            ('relminmxid', str),
            ('relacl', str),
            ('reloptions', str),
        ]

    def produce(self):
        return []
