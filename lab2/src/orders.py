import datetime
from faker import Faker
import random

from src.customer import Customer


fake = Faker()


class Payment:
    card_owner: str
    card_id: int

    def __init__(self, name):
        self.card_owner = name if random.uniform(0, 1) > 0.3 else fake.name()
        self.card_id = int(fake.credit_card_number())


class OrderMongo:
    order_number: int
    date: datetime
    total_sum: float
    customer: Customer.__dict__
    payment: Payment.__dict__
    order_items_id: list

    def __init__(self, customer, items_info):
        self.order_number = random.randint(1000, 99999999)
        self.date = fake.date_time_between(start_date='-5y', end_date='now', tzinfo=None)
        self.customer = customer.__dict__
        self.payment = Payment(f"{customer.name} {customer.surname}").__dict__
        self.total_sum = 0
        self.order_items_id = list()
        for item in items_info:
            self.total_sum += item.get('price')
            self.order_items_id.append(
                {
                    '$ref': 'items',
                    '$id': item.get('_id')
                }
            )



