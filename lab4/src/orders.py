import json

from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model


class Order(Model):
    __table_name__ = 'orders'
    #__keyspace__ = 'lab4_cassandra'
    customer_name = columns.Text(primary_key=True)
    date = columns.Date(primary_key=True)
    id = columns.Integer(primary_key=True)
    sum = columns.Integer()
    items_id = columns.List(columns.Integer(), index=True)

    @property
    def json(self):
        # Perform the same thing as before.
        json_dict = dict(self)
        json_dict['date'] = str(json_dict['date'])
        return json.dumps(json_dict, indent=4)

