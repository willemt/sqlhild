import math
import random


_functions = {}


class FunctionWatcher(type):
    """
    Register tables for use in FROM and JOIN clauses
    """

    def __init__(cls, name, bases, clsdict):
        # logging.debug("Registering table: {0}".format(name))
        try:
            _functions[cls._name] = cls
        except AttributeError:
            _functions[name] = cls
        super(FunctionWatcher, cls).__init__(name, bases, clsdict)


class Function(metaclass=FunctionWatcher):
    __metaclass__ = FunctionWatcher


class constant(Function):
    def __call__(self, arg):
        return arg


class Aggregate(Function):
    pass


class AVG(Aggregate):
    pass


class COUNT(Aggregate):
    pass


class MAX(Aggregate):
    pass


class MIN(Aggregate):
    pass


class SUM(Aggregate):
    pass


# class ABS(Function):
#     pass
#
#
# class ACOS(Function):
#     pass
#
#
# class ADD_MONTHS(Function):
#     pass
#
#
# class ASCII(Function):
#     pass
#
#
# class ASIN(Function):
#     pass
#
#
# class ATAN(Function):
#     pass
#
#
# class ATAN2(Function):
#     pass
#
#
# class CASE(Function):
#     pass
#
#
# class CAST(Function):
#     pass
#
#
# class CEILING(Function):
#     pass
#
#
# class CHAR(Function):
#     pass
#
#
# class CHR(Function):
#     pass
#
#
# class COALESCE(Function):
#     pass
#
#
# class CONCAT(Function):
#     pass
#
#
# class CONVERT(Function):
#     pass
#
#
# class COS(Function):
#     pass
#
#
# class CURDATE(Function):
#     pass
#
#
# class CURTIME(Function):
#     pass
#
#
# class DATABASE(Function):
#     pass
#
#
# class DAYNAME(Function):
#     pass
#
#
# class DAYOFMONTH(Function):
#     pass
#
#
# class DAYOFWEEK(Function):
#     pass
#
#
# class DAYOFYEAR(Function):
#     pass
#
#
# class DB_NAME(Function):
#     pass
#
#
# class DECODE(Function):
#     pass
#
#
# class DEGREES(Function):
#     pass
#
#
# class EXP(Function):
#     pass
#
#
# class FLOOR(Function):
#     pass
#
#
# class GREATEST(Function):
#     pass
#
#
# class HOUR(Function):
#     pass
#
#
# class IFNULL(Function):
#     pass
#
#
# class INITCAP(Function):
#     pass
#
#
# class INSERT(Function):
#     pass
#
#
# class INSTR(Function):
#     pass
#
#
# class LAST_DAY(Function):
#     pass
#
#
# class LCASE(Function):
#     pass
#
#
# class LEAST(Function):
#     pass
#
#
# class LEFT(Function):
#     pass
#
#
# class LENGTH(Function):
#     pass
#
#
# class LOCATE(Function):
#     pass
#
#
# class LOG10(Function):
#     pass
#
#
# class LOWER(Function):
#     pass
#
#
# class LPAD(Function):
#     pass
#
#
# class LTRIM(Function):
#     pass
#
#
# class MINUTE(Function):
#     pass
#
#
# class MOD(Function):
#     pass
#
#
# class MONTH (Function):
#     pass


def ABS(num):
    return abs(num)


def ACOS(num):
    return math.acos(num)


def ADD_MONTHS():
    raise NotImplementedError()


def ASCII():
    raise NotImplementedError()


def ASIN(num):
    return math.asin(num)


def ATAN(num):
    return math.atan(num)


def ATAN2(num):
    return math.atan2(num)


def CASE():
    raise NotImplementedError()


def CAST():
    raise NotImplementedError()


def CEILING(num):
    return math.ceil(num)


def CHAR(x):
    raise NotImplementedError()


def CHR():
    raise NotImplementedError()


def COALESCE():
    raise NotImplementedError()


def CONCAT(a, b):
    return a + b


def CONVERT():
    raise NotImplementedError()


def COS(num):
    return math.cos(num)


def CURDATE():
    raise NotImplementedError()


def CURTIME():
    raise NotImplementedError()


def DATABASE():
    raise NotImplementedError()


def DAYNAME():
    raise NotImplementedError()


def DAYOFMONTH():
    raise NotImplementedError()


def DAYOFWEEK():
    raise NotImplementedError()


def DAYOFYEAR():
    raise NotImplementedError()


def DB_NAME():
    raise NotImplementedError()


def DECODE():
    raise NotImplementedError()


def DEGREES():
    raise NotImplementedError()


def EXP(num):
    return math.exp(num)


def FLOOR(n):
    return math.floor(n)


def GREATEST():
    raise NotImplementedError()


def HOUR():
    raise NotImplementedError()


def IFNULL():
    raise NotImplementedError()


def INITCAP():
    raise NotImplementedError()


def INSERT():
    raise NotImplementedError()


def INSTR():
    raise NotImplementedError()


def LAST_DAY():
    raise NotImplementedError()


def LCASE(s):
    return s.lower()


def LEAST():
    raise NotImplementedError()


def LEFT():
    raise NotImplementedError()


def LENGTH(s):
    return len(s)


def LOCATE():
    raise NotImplementedError()


def LOG10(n):
    return math.log10(n)


def LOWER():
    raise NotImplementedError()


def LPAD():
    raise NotImplementedError()


def LTRIM():
    raise NotImplementedError()


def MINUTE():
    raise NotImplementedError()


def MOD(x, y):
    return x % y


def MONTH():
    raise NotImplementedError()


def MONTHNAME():
    raise NotImplementedError()


def MONTHS_BETWEENNEXT_DAY():
    raise NotImplementedError()


def NOW():
    raise NotImplementedError()


def NULLIF():
    raise NotImplementedError()


def NVL():
    raise NotImplementedError()


def PI():
    return math.pi


def POWER(x, y):
    return x ** y


def PREFIX():
    raise NotImplementedError()


def QUARTER():
    raise NotImplementedError()


def RADIANS():
    raise NotImplementedError()


class rand(Function):
    def __call__(self):
        return random.random()


def REPEAT():
    raise NotImplementedError()


def REPLACE():
    raise NotImplementedError()


def RIGHT():
    raise NotImplementedError()


def ROUND(n, i):
    return round(n, i)


def ROWID():
    raise NotImplementedError()


def RPAD():
    raise NotImplementedError()


def RTRIM():
    raise NotImplementedError()


def SECOND():
    raise NotImplementedError()


def SIGN():
    raise NotImplementedError()


def SIN(n):
    return math.sin(n)


def SQRT(n):
    return math.sqrt(n)


def SUBSTR():
    raise NotImplementedError()


def SUBSTRING():
    raise NotImplementedError()


def SUFFIX():
    raise NotImplementedError()


def SYSDATE():
    raise NotImplementedError()


def SYSTIME():
    raise NotImplementedError()


def SYSTIMESTAMP():
    raise NotImplementedError()


def TAN(n):
    return math.tan(n)


def TO_CHAR():
    raise NotImplementedError()


def TO_DATETO_NUMBER():
    raise NotImplementedError()


def TO_TIME():
    raise NotImplementedError()


def TO_TIMESTAMP():
    raise NotImplementedError()


def TRANSLATE():
    raise NotImplementedError()


def UCASE():
    raise NotImplementedError()


def UPPER():
    raise NotImplementedError()


def USER():
    raise NotImplementedError()


def WEEK():
    raise NotImplementedError()


def YEAR():
    raise NotImplementedError()


def get(name):
    try:
        function_class = _functions[name]
    except KeyError:
        raise Exception(name)

    func = function_class()

    return func
