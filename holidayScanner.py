#!/usr/bin/python3

import datetime
import json
from statistics import mean
import requests
import sqlite3
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
    
class Trip():
    def __init__(self, **kwargs):
        self.name = kwargs.get("name")
        self.airport = kwargs.get("airport")
        self.dates = kwargs.get("dates")
        self.date_start = kwargs.get("date_start")
        self.date_end = kwargs.get("date_end")
        self.days_of_week = kwargs.get("days_of_week")

    def __repr__(self):
        return f"{self.name}: {self.airport}"

def get_departure_dates(trip: Trip):
    departure_dates = set(datetime.date.fromisoformat(jsondate) for jsondate in trip.dates)
    date_start = datetime.datetime.strptime(trip.date_start, "%Y-%m-%d").date() if trip.date_start else datetime.date.today()
    date_end = datetime.datetime.strptime(trip.date_end, "%Y-%m-%d").date()

    while date_start <= date_end:
        if date_start.weekday() in trip.days_of_week:
            departure_dates.add(date_start)
        date_start += datetime.timedelta(days = 1)

    return departure_dates

def get_offers(trip: Trip, ddate):
    url = f'https://www.waya.travel/api/graphql?query=    query searchOutbound($partner: Partner!, $origin: String!, $destination: String!, $passengerAges: [PositiveInt!]!, $metadata: Metadata!, $departureDateString: String!, $returnDateString: String, $sort: Sort, $limit: PositiveInt, $filters: OfferFiltersInput, $utmSource: String) {{  boundSearch: searchOutbound(    partner: $partner    origin: $origin    destination: $destination    passengerAges: $passengerAges    metadata: $metadata    departureDateString: $departureDateString    returnDateString: $returnDateString    sort: $sort    limit: $limit    filters: $filters    utmSource: $utmSource  ) {{    offers {{      ...Offer    }}    offersFilters {{      ...OffersFilters    }}  }}}}        fragment Offer on Offer {{  id  journeyId  price  pricePerPerson  outboundPricePerPerson  homeboundPricePerPerson  currency  transferURL  duration  itinerary {{    ...Itinerary  }}  passengerAges  isOneWay}}        fragment Itinerary on Itinerary {{  outbound {{    ...Route  }}  homebound {{    ...Route  }}}}        fragment Route on Route {{  id  origin {{    code    name    city    country  }}  destination {{    code    name    city    country  }}  departure  arrival  duration  operatingCarrier {{    name    code    flightNumber  }}  marketingCarrier {{    name    code    flightNumber  }}  legs {{    ...Leg  }}}}        fragment Leg on Leg {{  id  duration  origin {{    code    name    city    country  }}  destination {{    code    name    city    country  }}  departure  arrival  carrierType  operatingCarrier {{    name    code    flightNumber  }}  marketingCarrier {{    name    code    flightNumber  }}}}        fragment OffersFilters on OfferFilters {{  overnightStay  overnightFlight  maxNumberOfStops  carrierCodes  cabinClass  connectionTime {{    min    max  }}  landing {{    outbound {{      min      max    }}    homebound {{      min      max    }}  }}  takeoff {{    outbound {{      min      max    }}    homebound {{      min      max    }}  }}}}    &variables={{"departureDateString":"{ddate}","destination":"{trip.airport}","filters":{{"cabinClass":null,"carrierCodes":[]}},"limit":1,"metadata":{{"country":"BE","currency":"EUR","language":"en"}},"origin":"CRL","partner":"waya","passengerAges":[25,25,25,25],"returnDateString":"{ddate}","sort":"CHEAPEST","utmSource":""}}'
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
    print(f"found an offer for date {offer.date_flight.isoformat()}.")
    previous_offers = get_previous_offers(offer)
    previous_prices = [offer.price for offer in previous_offers]
    if not previous_prices:
        return
    mean_price = mean(previous_prices)
    lowest_price = min(previous_prices)

    if (offer.price < lowest_price):
        print(f"it is the cheapest it has ever been! previous low: \033[1;31m{lowest_price:.2f}\033[0m€ now \033[1;32m{offer.price:.2f}\033[0m€")
    elif (offer.price < mean_price):
        print(f"it is cheaper than before! average historical price: \033[1;31m{mean_price:.2f}\033[0m€ now \033[1;32m{offer.price:.2f}\033[0m€")

def get_price_info(trip: Trip, ddate: datetime.date):
    date_str = ddate.strftime("%Y-%m-%d")
    offers = get_offers(trip, date_str)
    if offers:
        offer = offers[0]
        offer_record = FlightRecord(
            date_flight=ddate,
            destination=trip.airport,
            price=offer["outboundPricePerPerson"],
            date_search=datetime.datetime.now(),
            external_id=offer["id"]
        )
        compare_offer(offer_record)
        return offer_record
    return None

def save_price_info(flight_records):
    cursor = conn.cursor()

    query = """
        INSERT INTO data (date_flight, destination, price, date_search, external_id) 
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(external_id) DO UPDATE SET
            price = excluded.price,
            date_search = excluded.date_search
    """
    values = [(rec.date_flight, rec.destination, rec.price, rec.date_search, rec.external_id) for rec in flight_records]
    if values:
        cursor.executemany(query, values)
        print(f"Saved {len(flight_records)} offers.")
    conn.commit()

def get_trips_to_watch():
    with open("trips.json", "r+") as fd:
        return [Trip(**tripjson) for tripjson in json.loads(fd.read())]

def scan_holidays(trips):
    flight_records = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        for trip in trips:
            print(f'Searching offers for trip {trip.name}')
            departure_dates = get_departure_dates(trip)

            futures = []
            for ddate in departure_dates:
                futures.append(executor.submit(get_price_info, trip, ddate))

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

def get_locations_offers(trip: Trip):
    local_conn = sqlite3.connect("data.db")
    cursor = local_conn.cursor()

    previous_offers = cursor.execute("""
        SELECT * FROM data WHERE
            destination = ?
    """, [trip.airport]).fetchall()

    reduced_offers = {}
    for offer in previous_offers: # 2 = date_flight, 4 = price
        if offer[2] not in reduced_offers:
            reduced_offers[offer[2]] = offer
        else:
            if offer[4] < reduced_offers[offer[2]][4]:
                reduced_offers[offer[2]] = offer

    return [FlightRecord(
        external_id = offer[1],
        date_flight = offer[2],
        destination = offer[3],
        price = offer[4],
        date_search = offer[5]
    ) for offer in list(reduced_offers.values())]

def show_best_dates(trips):
    for trip in trips:
        location_offers = get_locations_offers(trip)
        sorted_offers = list(sorted(location_offers, key=lambda offer: offer.price))
        print(f"Best flights for {trip.name}:")
        for offer in sorted_offers[:10]:
            print(offer)

if __name__ == "__main__":
    init_database()
    trips = get_trips_to_watch()
    scan_holidays(trips)
    show_best_dates(trips)
    if conn:
        conn.close()
