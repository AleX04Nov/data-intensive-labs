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


class Item:
    category: str = ""
    model: str = ""
    producer: str = ""
    price: int = 0

    def __init__(self, category="", model="", producer="", price=0):
        self.category = category
        self.model = model
        self.producer = producer
        self.price = price


class Phone(Item):
    os_name: str
    hasGsm: bool
    has4g: bool
    has5g: bool
    resolution: str
    battery: str

    def __init__(self, category="", model="", producer="", price=0):
        super().__init__(category, model, producer, price)

    def __init__(self):
        super().__init__(
            category="Phone",
            model=fake.last_name(),
            producer=random.choice(companies_list),
            price=random.randint(300, 9999)
        )
        self.os_name = random.choice(phone_os_names)
        self.hasGsm = True if random.uniform(0, 1) > 0.1 else False
        self.has4g = True if random.uniform(0, 1) > 0.25 else False
        self.has5g = True if random.uniform(0, 1) > 0.7 else False
        self.resolution = random.choice(resolutions)
        self.battery = f"{ random.randint(1001, 9999) }mAh"


class TV(Item):
    os_name: str
    resolution: str
    smartTV: bool
    wifi: bool

    def __init__(self):
        super().__init__(
            category="TV",
            model=fake.last_name(),
            producer=random.choice(companies_list),
            price=random.randint(3000, 20999)
        )
        self.os_name = random.choice(tv_os_names)
        self.smartTV = True if (self.os_name != "None") or (random.uniform(0, 1) > 0.5) else False
        self.wifi = True if random.uniform(0, 1) > 0.3 else False
        self.resolution = random.choice(resolutions[2:])


class Microphone(Item):
    frequencies: str
    sensitivity: str

    def __init__(self):
        super().__init__(
            category="Microphone",
            model=fake.last_name(),
            producer=random.choice(companies_list),
            price=random.randint(20, 900)
        )
        self.frequencies = f"{random.randint(15, 50)}Hz - {random.randint(10000, 30000)}Hz"
        self.sensitivity = f"-{random.randint(20, 50)}dB ± {random.randint(1, 8)}"


class Display(Item):
    resolution: str

    def __init__(self):
        super().__init__(
            category="Display",
            model=fake.last_name(),
            producer=random.choice(companies_list),
            price=random.randint(80, 15000)
        )
        self.resolution = random.choice(resolutions)

