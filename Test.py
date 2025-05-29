from datetime import datetime
from datetime import timezone, timedelta
from dateutil.relativedelta import relativedelta

if __name__ == "__main__":
    today = datetime.now(timezone(timedelta(hours=10))).date()
    yesterday = today - relativedelta(days=1)
    print(yesterday)