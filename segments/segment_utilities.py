from datetime import date, timedelta
import random

def create_obr_time():
    random_days_ago = random.randint(1, 7)
    random_date = date.today() - timedelta(days=random_days_ago)
    return random_date.strftime("%Y%m%d%H%M")

