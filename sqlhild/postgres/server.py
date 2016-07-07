"""
Run a postgres server facade
"""

import logging
import re

from protlib import (
    LoggingTCPServer,
    TCPHandler,
)

from . import message
from .message import (
    AuthenticationOk,
    ColumnValue,
    CommandComplete,
    DataRow,
    ErrorResponse,
    Length,
    ParameterStatus,
    ReadyForQuery,
    Row,
    RowDescription,
    al,
)

from sqlhild.postgres import table  # NOQA: register postgress tables
from sqlhild.query import QueryPlan


class PostgresHandler(TCPHandler):
    """
    Pretend to be Postgres
    """

    LOG_TO_SCREEN = False
    STRUCT_MOD = message

    def handle(self):
        self.handshake()
        super().handle()

    def handshake(self):
        length = Length.parse(self.request.recv(4))
        self.request.recv(length.length)

        # TODO: we should authenticate this
        # startup = Startup.parse(data)

        self.request.sendall(AuthenticationOk().serialize())
        self.request.sendall(ReadyForQuery().serialize())

    def query(self, s):
        logging.debug('Received query:\n\t{}'.format(s.query))

        query = s.query.decode('utf-8').strip()

        if query.lower().startswith('set'):
            name, value = re.match(r'^set ([a-zA-Z_]+) to (.*)$', query, re.IGNORECASE).groups()
            self.set_parameter(name, value)
            response = al(ParameterStatus(
                parameter_name=name,
                parameter_value=value,
            ))
            self.request.sendall(response.serialize())
            self.request.sendall(al(CommandComplete(tag='SET')).serialize())
            self.request.sendall(al(ReadyForQuery()).serialize())
        else:
            self.process_query(s.query.decode('utf-8'))

    def process_query(self, sql_text):
        """
        1. Read query
        2. Pull data from query
        3. Respond with data
        """

        # TODO: send row metadata before we do list(q.produce).
        #       use iterator through to the very end

        q = QueryPlan()

        try:
            q.process(sql_text)
            rows = list(q.produce())
        except Exception as e:
            logging.error(e)
            msg = b'M' + str(e).encode('utf8') + b'\0'
            error = ErrorResponse(length=4 + len(msg), message=msg)
            self.request.sendall(error.serialize())
            self.request.sendall(ReadyForQuery().serialize())
            return

        # 1. Send row metadata
        fields = []
        for col in q.columns.columns:
            fields.append(
                Row(
                    name=col.name,
                    oid=1,
                    attr_num=0,
                    datatype_oid=0,
                    datatype_size=0,
                    type_modifier=0,
                    format_code=0,
                )
            )
        cmd = RowDescription(
            length=sum(map(Row.sizeof, fields)) + 4 + 2,
            num_fields=len(fields),
            fields=fields,
        )
        self.request.sendall(cmd.serialize())

        # 2. Send rows
        for row in rows:
            logging.debug(row)
            column_values = []
            for val in row:
                encoded = str(val).encode('utf8')
                colvalue = ColumnValue(length=len(encoded), bytes=encoded)
                column_values.append(colvalue)

            cmd = DataRow(
                length=sum(map(ColumnValue.sizeof, column_values)) + 4 + 2,
                num_fields=len(column_values),
                column_values=column_values,
            )
            self.request.sendall(cmd.serialize())

        # 3. Clean up
        tag = 'SELECT {}'.format(len(rows))
        self.request.sendall(al(CommandComplete(tag=tag)).serialize())
        self.request.sendall(ReadyForQuery().serialize())

    def set_parameter(self, name, value):
        # TODO: set parameters
        pass


def start_server(host):
    # TODO: validate host string
    host, port = host.split(':')
    LoggingTCPServer((host, int(port)), PostgresHandler).serve_forever()
