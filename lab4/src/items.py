from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
import json

class Item(Model):
    __table_name__ = 'item'
    #__keyspace__ = 'lab4_cassandra'
    category = columns.Text(primary_key=True)
    price = columns.Integer(primary_key=True)
    producer = columns.Text(primary_key=True)
    id = columns.Integer()
    model = columns.Text(index=True)
    info = columns.Map(columns.Text(), columns.Text())

    @property
    def json(self):
        # Perform the same thing as before.
        json_dict = dict(self)
        return json.dumps(json_dict, indent=4)
