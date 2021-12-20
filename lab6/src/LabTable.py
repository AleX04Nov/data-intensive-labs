from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
import json


class LabTable(Model):
    __table_name__ = 'lab6_table'
    id = columns.Integer(primary_key=True)
    text = columns.Text()

    @property
    def json(self):
        # Perform the same thing as before.
        json_dict = dict(self)
        return json.dumps(json_dict, indent=4)
