"""
Messages as per postgres' spec
https://www.postgresql.org/docs/9.3/static/protocol-flow.html
https://www.postgresql.org/docs/9.3/static/protocol-message-formats.html
"""

from protlib import (
    AUTOSIZED,
    CArray,
    CInt,
    CShort,
    CString,
    CStruct,
    CUChar,
    CUShort,
)


def al(x):
    """
    Automatically set the length field
    """
    length = x.__class__.sizeof(x)
    x.length = length - 1
    return x


class Length(CStruct):
    length = CInt()


class Startup(CStruct):
    protocol_version = CInt()
    parameter_name = CString(length=AUTOSIZED)
    parameter_value = CString(length=AUTOSIZED)


class AuthenticationOk(CStruct):
    type = CUChar(default=b'R')
    length = CInt(always=8)
    success = CInt(always=0)


class ReadyForQuery(CStruct):
    type = CUChar(default=b'Z')
    length = CInt(always=5)
    status = CUChar(default=b'I')


class Query(CStruct):
    type = CUChar(always=ord('Q'))
    length = CInt()
    query = CString(length=AUTOSIZED)


class EmptyQueryResponse(CStruct):
    type = CUChar(always=ord('I'))
    length = CInt(always=4)


class ParameterStatus(CStruct):
    type = CUChar(always=ord('S'))
    length = CInt()
    parameter_name = CString(length=AUTOSIZED)
    parameter_value = CString(length=AUTOSIZED)


class CommandComplete(CStruct):
    type = CUChar(always=ord('C'))
    length = CInt()
    tag = CString(length=AUTOSIZED)


class Row(CStruct):
    """
    Examples:
        Column name: oid
            Table OID: 1247
            Column index: 65534
            Type OID: 26
            Column length: 4
            Type modifier: -1
            Format: Text (0)
        Column name: typbasetype
            Table OID: 1247
            Column index: 24
            Type OID: 26
            Column length: 4
            Type modifier: -1
            Format: Text (0)
    """
    name = CString(length=AUTOSIZED)
    oid = CInt()
    attr_num = CUShort()
    datatype_oid = CInt()
    datatype_size = CShort()
    type_modifier = CInt()
    format_code = CShort()


class RowDescription(CStruct):
    type = CUChar(always=ord('T'))
    length = CInt()
    num_fields = CUShort()
    fields = CArray("num_fields", Row)


class ColumnValue(CStruct):
    length = CInt()
    bytes = CString(length="length")


class DataRow(CStruct):
    type = CUChar(always=ord('D'))
    length = CInt()
    num_fields = CUShort()
    column_values = CArray("num_fields", ColumnValue)


class ErrorResponse(CStruct):
    type = CUChar(always=ord('E'))
    length = CInt()
    message = CString(length=AUTOSIZED)


class NoticeResponse(CStruct):
    type = CUChar(always=ord('N'))
    length = CInt()
    message = CString(length=AUTOSIZED)
