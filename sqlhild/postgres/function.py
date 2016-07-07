import sqlhild


class current_schema(sqlhild.function.Function):
    def __call__(self):
        return 'PUBLIC'
