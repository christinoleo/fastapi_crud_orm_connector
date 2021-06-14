class CannotCrud(Exception):
    def __init__(self, fields):
        super().__init__(f'Columns {fields} of not present in dataframe')


class CannotGroupBy(CannotCrud):
    pass


class CannotFilterFields(CannotCrud):
    pass
