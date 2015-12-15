class MetaTable(object):
    """"""

    def __init__(self, name, comment=None, fields=list()):
        """"""

        self.name = name
        self.comment = comment
        self.fields = fields

    def add_field(self, field):
        """"""

        self.fields.append(field)


class MetaField(object):
    """"""

    def __init__(self, name, comment=None, data_type=None):
        """"""

        self.name = name
        self.comment = comment
        self.data_type = data_type

