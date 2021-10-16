from faker import Faker
from random import randint


fake = Faker()

class Customer:
    name: str
    surname: str
    phones: list
    address: str

    def __init__(self, mongo_res=None):
        if mongo_res is not None:
            self.name = mongo_res.get('name')
            self.surname = mongo_res.get('surname')
            self.phones = mongo_res.get('phones')
            self.address = mongo_res.get('address')
        else:
            self.name = fake.first_name()
            self.surname = fake.last_name()
            self.phones = list()
            for phone_num in range(randint(1, 4)):
                self.phones.append(int(fake.msisdn()))
            self.address = fake.address()
