#!/usr/bin/python3

import datetime
from statistics import mean
import dateutil.relativedelta
import requests
from concurrent.futures import ThreadPoolExecutor 

conn = None

class FlightRecord():
    def __init__(self, **kwargs):
        self.date_flight = kwargs.get("date_flight")
        self.destination = kwargs.get("destination")
        self.price = kwargs.get("price")
        self.date_search = kwargs.get("date_search")
        self.external_id = kwargs.get("external_id")
        pass

    def __repr__(self):
        return f"{self.date_flight}: {self.price}€"

def get_departure_dates(start_date: datetime.date, end_date):
    departure_dates = []
    while start_date <= end_date:
        if start_date.weekday() in (3, 4):
            departure_dates.append(start_date)
        start_date += datetime.timedelta(days = 1)

    return departure_dates

def get_offers(ddate):
    url = f"https://www.waya.travel/api/graphql?query=%20%20%20%20query%20searchOutbound($partner:%20Partner!,%20$origin:%20String!,%20$destination:%20String!,%20$passengerAges:%20[PositiveInt!]!,%20$metadata:%20Metadata!,%20$departureDateString:%20String!,%20$returnDateString:%20String,%20$sort:%20Sort,%20$limit:%20PositiveInt,%20$filters:%20OfferFiltersInput,%20$utmSource:%20String)%20{{%20%20boundSearch:%20searchOutbound(%20%20%20%20partner:%20$partner%20%20%20%20origin:%20$origin%20%20%20%20destination:%20$destination%20%20%20%20passengerAges:%20$passengerAges%20%20%20%20metadata:%20$metadata%20%20%20%20departureDateString:%20$departureDateString%20%20%20%20returnDateString:%20$returnDateString%20%20%20%20sort:%20$sort%20%20%20%20limit:%20$limit%20%20%20%20filters:%20$filters%20%20%20%20utmSource:%20$utmSource%20%20)%20{{%20%20%20%20offers%20{{%20%20%20%20%20%20...Offer%20%20%20%20}}%20%20%20%20offersFilters%20{{%20%20%20%20%20%20...OffersFilters%20%20%20%20}}%20%20}}}}%20%20%20%20%20%20%20%20fragment%20Offer%20on%20Offer%20{{%20%20id%20%20journeyId%20%20price%20%20pricePerPerson%20%20outboundPricePerPerson%20%20homeboundPricePerPerson%20%20currency%20%20transferURL%20%20duration%20%20itinerary%20{{%20%20%20%20...Itinerary%20%20}}%20%20passengerAges%20%20isOneWay}}%20%20%20%20%20%20%20%20fragment%20Itinerary%20on%20Itinerary%20{{%20%20outbound%20{{%20%20%20%20...Route%20%20}}%20%20homebound%20{{%20%20%20%20...Route%20%20}}}}%20%20%20%20%20%20%20%20fragment%20Route%20on%20Route%20{{%20%20id%20%20origin%20{{%20%20%20%20code%20%20%20%20name%20%20%20%20city%20%20%20%20country%20%20}}%20%20destination%20{{%20%20%20%20code%20%20%20%20name%20%20%20%20city%20%20%20%20country%20%20}}%20%20departure%20%20arrival%20%20duration%20%20operatingCarrier%20{{%20%20%20%20name%20%20%20%20code%20%20%20%20flightNumber%20%20}}%20%20marketingCarrier%20{{%20%20%20%20name%20%20%20%20code%20%20%20%20flightNumber%20%20}}%20%20legs%20{{%20%20%20%20...Leg%20%20}}}}%20%20%20%20%20%20%20%20fragment%20Leg%20on%20Leg%20{{%20%20id%20%20duration%20%20origin%20{{%20%20%20%20code%20%20%20%20name%20%20%20%20city%20%20%20%20country%20%20}}%20%20destination%20{{%20%20%20%20code%20%20%20%20name%20%20%20%20city%20%20%20%20country%20%20}}%20%20departure%20%20arrival%20%20carrierType%20%20operatingCarrier%20{{%20%20%20%20name%20%20%20%20code%20%20%20%20flightNumber%20%20}}%20%20marketingCarrier%20{{%20%20%20%20name%20%20%20%20code%20%20%20%20flightNumber%20%20}}}}%20%20%20%20%20%20%20%20fragment%20OffersFilters%20on%20OfferFilters%20{{%20%20overnightStay%20%20overnightFlight%20%20maxNumberOfStops%20%20carrierCodes%20%20cabinClass%20%20connectionTime%20{{%20%20%20%20min%20%20%20%20max%20%20}}%20%20landing%20{{%20%20%20%20outbound%20{{%20%20%20%20%20%20min%20%20%20%20%20%20max%20%20%20%20}}%20%20%20%20homebound%20{{%20%20%20%20%20%20min%20%20%20%20%20%20max%20%20%20%20}}%20%20}}%20%20takeoff%20{{%20%20%20%20outbound%20{{%20%20%20%20%20%20min%20%20%20%20%20%20max%20%20%20%20}}%20%20%20%20homebound%20{{%20%20%20%20%20%20min%20%20%20%20%20%20max%20%20%20%20}}%20%20}}}}%20%20%20%20&variables={{%22departureDateString%22:%22{ddate}%22,%22destination%22:%22TIA%22,%22filters%22:{{%22cabinClass%22:null,%22carrierCodes%22:[]}},%22limit%22:1,%22metadata%22:{{%22country%22:%22BE%22,%22currency%22:%22EUR%22,%22language%22:%22en%22}},%22origin%22:%22CRL%22,%22partner%22:%22waya%22,%22passengerAges%22:[25,25,25,25],%22returnDateString%22:%22{ddate}%22,%22sort%22:%22CHEAPEST%22,%22utmSource%22:%22%22}}"
    headers = {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9,fr-FR;q=0.8,fr;q=0.7,de;q=0.6,pl;q=0.5",
        "baggage": "sentry-environment=production,sentry-public_key=73acf5a7b70842d99ff63bd037b55ed2,sentry-trace_id=955b149f5ae2419b94c47a6e96c6b8bc,sentry-sample_rate=0,sentry-transaction=%2F%5Bpartner%5D,sentry-sampled=false",
        "content-type": "application/json",
        "cookie": "x-dohop-user-id=b2155ebf-64c1-4898-8d85-f18e9a5f7835; _sp_id.7728=02f0e6cc-a1b0-47c7-9c7a-560f3cbab832.1737294593.2.1738263505.1737295586.e9b469b1-3145-4375-8621-dff91b2a86d7.68250d63-99ba-451d-b294-f908ae3adb4f.fc5e9815-a411-4819-9034-be6c761383f9.1738263125033.7; SIBYU68J_secret=P7REHYE5EOXL; 3EYYE6E5_secret=OXFDYRS65I57; GLYIA7ME_secret=2WTGIHLYKUIQ; datadome=OcoEvjPR~17gxwB7Xg~pjvjwiq_MNe4xUuhMb9i6wpGcCFKQtmy5o9wyMszOWBjc07gbAOq5SyFzkQRTZYb32hhiUxAz9Uorf5k0o9QQQ4kc2HIQu9Dqxwa1ZOPb5onB",
        "priority": "u=1, i",
        "referer": "https://www.waya.travel/",
        "sec-ch-device-memory": "8",
        "sec-ch-ua": '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
        "sec-ch-ua-arch": '"x86"',
        "sec-ch-ua-full-version-list": '"Not A(Brand";v="8.0.0.0", "Chromium";v="132.0.6834.197", "Google Chrome";v="132.0.6834.197"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-model": '""',
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "sentry-trace": "955b149f5ae2419b94c47a6e96c6b8bc-a66619a43ab930b5-0",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        "x-datadome-clientid": "8bD4OfOxtrltMHGI1UfL8wzVK_dweUAKkQhqyEFaOMykDVKksNYLRVhNGSFyWMSidD1lq24DX5sQFFAObqy4TTjXh9ks1QSVGJsMYhiAteev1oUyvmPS8dIRCs3gxjEp",
        "x-dato-cms-token": "63fff59ede383be629d6510f9bddfb"
    }

    res = requests.get(url, headers=headers)
    if (res.json().get("errors")):
        return None
    return res.json()['data']['boundSearch']['offers']

def compare_offer(offer):
    previous_offers = get_previous_offers(offer)
    previous_prices = [offer.price for offer in previous_offers]
    if not previous_prices:
        return
    mean_price = mean(previous_prices)
    lowest_price = min(previous_prices)

    print(f"found an offer for date {offer.date_flight.isoformat()}.")
    if (offer.price < lowest_price):
        print(f"it is the cheapest it has ever been! previous low: {lowest_price}€ now \033[1;32m{offer.price}\033[0m€")
    elif (offer.price < mean_price):
        print(f"it is cheaper than before! average historical price: {mean_price}€ now \033[1;32m{offer.price}\033[0m€")

def get_price_info(ddate: datetime.date):
    date_str = ddate.strftime("%Y-%m-%d")
    offers = get_offers(date_str)
    if offers:
        offer = offers[0]
        offer_record = FlightRecord(
            date_flight=ddate,
            destination="TIA",
            price=offer["outboundPricePerPerson"],
            date_search=datetime.datetime.now(),
            external_id=offer["id"]
        )
        compare_offer(offer_record)
        return offer_record
    return None

def save_price_info(flight_records):
    cursor = conn.cursor()

    query = "INSERT INTO data (date_flight, destination, price, date_search, external_id) values "
    for rec in flight_records:
        query += f"('{rec.date_flight}', '{rec.destination}', {rec.price}, '{rec.date_search}', '{rec.external_id}'), "
    if len(flight_records):
        query = query[:-2]

        query += """
            ON CONFLICT(external_id) DO UPDATE SET
            price = excluded.price,
            date_search = excluded.date_search
        """
    
    cursor.execute(query)
    conn.commit()
    print(f"Saved {len(flight_records)} offers.")

def scan_holidays():
    departure_dates = get_departure_dates(datetime.date.today(), datetime.date.today() + dateutil.relativedelta.relativedelta(months = 3))
    flight_records = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for ddate in departure_dates:
            futures.append(executor.submit(get_price_info, ddate))

        for future in futures:
            offer = future.result()
            if offer:
                flight_records.append(offer)
    save_price_info(flight_records)

def init_database():
    global conn
    conn = sqlite3.connect("data.db")
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            external_id TEXT UNIQUE,
            date_flight TEXT,
            destination TEXT,
            price REAL,
            date_search TEXT
        )
    """)

    conn.commit()

def get_previous_offers(offer):
    local_conn = sqlite3.connect("data.db")
    cursor = local_conn.cursor()

    previous_offers = cursor.execute("""
        SELECT * FROM data WHERE
            destination = ? AND
            date_flight = ?
    """, (offer.destination, offer.date_flight.isoformat()))

    return [FlightRecord(
        external_id = offer[1],
        date_flight = offer[2],
        destination = offer[3],
        price = offer[4],
        date_search = offer[5]
    ) for offer in previous_offers.fetchall()]

def get_locations_offers():
    local_conn = sqlite3.connect("data.db")
    cursor = local_conn.cursor()

    previous_offers = cursor.execute("""
        SELECT * FROM data WHERE
            destination = 'TIA'
    """)

    return [FlightRecord(
        external_id = offer[1],
        date_flight = offer[2],
        destination = offer[3],
        price = offer[4],
        date_search = offer[5]
    ) for offer in previous_offers.fetchall()]

def show_best_dates():
    location_offers = get_locations_offers()
    sorted_offers = list(sorted(location_offers, key=lambda offer: offer.price))
    print("RANKING:")
    for offer in sorted_offers:
        print(offer)

if __name__ == "__main__":
    init_database()
    scan_holidays()
    show_best_dates()
    if conn:
        conn.close()
