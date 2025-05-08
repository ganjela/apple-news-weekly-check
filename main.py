from monitor import NewsDataMonitor
from publisher import Publisher

def main(request):
    missing_dates = NewsDataMonitor().run()
    Publisher().publish(missing_dates)

    return {"status": "success"}



