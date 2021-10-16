from datetime import datetime
from faker import Faker

fake = Faker()


class Comment:
    timestamp: datetime
    text: str

    def __init__(self):
        self.date = fake.date_time_between(start_date='-5y', end_date='now', tzinfo=None)
        self.text = fake.paragraph(nb_sentences=5)
