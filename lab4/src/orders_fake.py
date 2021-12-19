import datetime
from faker import Faker

fake = Faker()


class OrdersFake:
    customer_name: str = ""
    date: datetime
    id: int = 0
    sum: int = 0
    items_id: list = []

    def __init__(self, customer_name, items, ids):
        self.customer_name = " "
        self.customer_name = customer_name
        self.id = ids
        self.date = fake.date_time_between(start_date='-2y', end_date='now', tzinfo=None)
        self.items_id = []
        self.sum = 0
        for item in items:
            self.items_id.append(item[0])
            self.sum += item[1]
