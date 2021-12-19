from faker import Faker
import random

companies_list = list()
phone_os_names = list()
tv_os_names = list()
resolutions = list()
fake = Faker()

def init_fake_items():
    global companies_list, phone_os_names, resolutions, tv_os_names
    for i in range(5):
        companies_list.append(fake.company())
    phone_os_names = ["Android", "iOS", "Symbian", "BlackberryOS", "Java"]
    resolutions = ["320x240", "640x480", "1280x720", "1920x1080", "2560x1440", "2048x1080", "3840x2160", "7680x4320"]
    tv_os_names = ["None", "Samsung Smart TV", "Google Smart TV", "Apple TV", "Philips Smart TV", "Amazon TV-Box"]


class ItemFake:
    category: str = ""
    model: str = ""
    producer: str = ""
    price: int = 0
    id: int = 0
    info: dict = {}

    def __init__(self, category="", model="", producer="", price=0, id3=0):
        self.category = category
        self.model = model
        self.producer = producer
        self.price = price
        self.id = id3
        self.info = dict()


class Phone(ItemFake):
    def __init__(self):
        super().__init__(
            category="Phone",
            model=fake.last_name(),
            producer=random.choice(companies_list),
            price=random.randint(300, 9999),
            id3=random.randint(0, 9999999)
        )
        self.info['os_name'] = random.choice(phone_os_names)
        self.info['hasGsm'] = str(True if random.uniform(0, 1) > 0.1 else False)
        self.info['has4g'] = str(True if random.uniform(0, 1) > 0.25 else False)
        self.info['has5g'] = str(True if random.uniform(0, 1) > 0.7 else False)
        self.info['resolution'] = random.choice(resolutions)
        self.info['battery'] = f"{ random.randint(1001, 9999) }mAh"


class TV(ItemFake):
    def __init__(self):
        super().__init__(
            category="TV",
            model=fake.last_name(),
            producer=random.choice(companies_list),
            price=random.randint(3000, 20999),
            id3=random.randint(0, 9999999)
        )
        self.info['os_name'] = random.choice(tv_os_names)
        self.info['smartTV'] = str(True if (self.info['os_name'] != "None") or (random.uniform(0, 1) > 0.5) else False)
        self.info['wifi'] = str(True if random.uniform(0, 1) > 0.3 else False)
        self.info['resolution'] = random.choice(resolutions[2:])


class Microphone(ItemFake):
    def __init__(self):
        super().__init__(
            category="Microphone",
            model=fake.last_name(),
            producer=random.choice(companies_list),
            price=random.randint(20, 900),
            id3=random.randint(0, 9999999)
        )
        self.info['frequencies'] = f"{random.randint(15, 50)}Hz - {random.randint(10000, 30000)}Hz"
        self.info['sensitivity'] = f"-{random.randint(20, 50)}dB ± {random.randint(1, 8)}"


class Display(ItemFake):
    def __init__(self):
        super().__init__(
            category="Display",
            model=fake.last_name(),
            producer=random.choice(companies_list),
            price=random.randint(80, 15000),
            id3=random.randint(0, 9999999)
        )
        self.info['resolution'] = random.choice(resolutions)

