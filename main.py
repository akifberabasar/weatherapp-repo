import requests
import time
from datetime import date, timedelta

TELEGRAM_TOKEN = "8436085274:AAH78VaM9i1JLISBGApGye9FZ2q7p2_Vkro"
CHAT_ID = "6100462157"

CITIES = {
    "nyc":           {"lat": 40.7128,  "lon": -74.0060,  "tz": "America/New_York",                "unit": "F"},
    "dallas":        {"lat": 32.7767,  "lon": -96.7970,  "tz": "America/Chicago",                 "unit": "F"},
    "atlanta":       {"lat": 33.7490,  "lon": -84.3880,  "tz": "America/New_York",                "unit": "F"},
    "chicago":       {"lat": 41.8781,  "lon": -87.6298,  "tz": "America/Chicago",                 "unit": "F"},
    "miami":         {"lat": 25.7617,  "lon": -80.1918,  "tz": "America/New_York",                "unit": "F"},
    "seattle":       {"lat": 47.6062,  "lon": -122.3321, "tz": "America/Los_Angeles",             "unit": "F"},
    "los-angeles":   {"lat": 34.0522,  "lon": -118.2437, "tz": "America/Los_Angeles",             "unit": "F"},
    "houston":       {"lat": 29.7604,  "lon": -95.3698,  "tz": "America/Chicago",                 "unit": "F"},
    "denver":        {"lat": 39.7392,  "lon": -104.9903, "tz": "America/Denver",                  "unit": "F"},
    "austin":        {"lat": 30.2672,  "lon": -97.7431,  "tz": "America/Chicago",                 "unit": "F"},
    "san-francisco": {"lat": 37.7749,  "lon": -122.4194, "tz": "America/Los_Angeles",             "unit": "F"},
    "toronto":       {"lat": 43.6532,  "lon": -79.3832,  "tz": "America/Toronto",                 "unit": "C"},
    "london":        {"lat": 51.5074,  "lon": -0.1278,   "tz": "Europe/London",                   "unit": "C"},
    "paris":         {"lat": 48.8566,  "lon": 2.3522,    "tz": "Europe/Paris",                    "unit": "C"},
    "madrid":        {"lat": 40.4168,  "lon": -3.7038,   "tz": "Europe/Madrid",                   "unit": "C"},
    "milan":         {"lat": 45.4642,  "lon": 9.1900,    "tz": "Europe/Rome",                     "unit": "C"},
    "warsaw":        {"lat": 52.2297,  "lon": 21.0122,   "tz": "Europe/Warsaw",                   "unit": "C"},
    "munich":        {"lat": 48.1351,  "lon": 11.5820,   "tz": "Europe/Berlin",                   "unit": "C"},
    "amsterdam":     {"lat": 52.3676,  "lon": 4.9041,    "tz": "Europe/Amsterdam",                "unit": "C"},
    "helsinki":      {"lat": 60.1699,  "lon": 24.9384,   "tz": "Europe/Helsinki",                 "unit": "C"},
    "moscow":        {"lat": 55.7558,  "lon": 37.6173,   "tz": "Europe/Moscow",                   "unit": "C"},
    "istanbul":      {"lat": 41.0082,  "lon": 28.9784,   "tz": "Europe/Istanbul",                 "unit": "C"},
    "ankara":        {"lat": 39.9334,  "lon": 32.8597,   "tz": "Europe/Istanbul",                 "unit": "C"},
    "tel-aviv":      {"lat": 32.0853,  "lon": 34.7818,   "tz": "Asia/Jerusalem",                  "unit": "C"},
    "seoul":         {"lat": 37.5665,  "lon": 126.9780,  "tz": "Asia/Seoul",                      "unit": "C"},
    "busan":         {"lat": 35.1796,  "lon": 129.0756,  "tz": "Asia/Seoul",                      "unit": "C"},
    "tokyo":         {"lat": 35.6762,  "lon": 139.6503,  "tz": "Asia/Tokyo",                      "unit": "C"},
    "beijing":       {"lat": 39.9042,  "lon": 116.4074,  "tz": "Asia/Shanghai",                   "unit": "C"},
    "shanghai":      {"lat": 31.2304,  "lon": 121.4737,  "tz": "Asia/Shanghai",                   "unit": "C"},
    "hong-kong":     {"lat": 22.3193,  "lon": 114.1694,  "tz": "Asia/Hong_Kong",                  "unit": "C"},
    "singapore":     {"lat": 1.3521,   "lon": 103.8198,  "tz": "Asia/Singapore",                  "unit": "C"},
    "taipei":        {"lat": 25.0330,  "lon": 121.5654,  "tz": "Asia/Taipei",                     "unit": "C"},
    "kuala-lumpur":  {"lat": 3.1390,   "lon": 101.6869,  "tz": "Asia/Kuala_Lumpur",               "unit": "C"},
    "jakarta":       {"lat": -6.2088,  "lon": 106.8456,  "tz": "Asia/Jakarta",                    "unit": "C"},
    "buenos-aires":  {"lat": -34.6037, "lon": -58.3816,  "tz": "America/Argentina/Buenos_Aires",  "unit": "C"},
    "sao-paulo":     {"lat": -23.5505, "lon": -46.6333,  "tz": "America/Sao_Paulo",               "unit": "C"},
    "mexico-city":   {"lat": 19.4326,  "lon": -99.1332,  "tz": "America/Mexico_City",             "unit": "C"},
    "panama-city":   {"lat": 8.9936,   "lon": -79.5197,  "tz": "America/Panama",                  "unit": "C"},
    "wellington":    {"lat": -41.2866, "lon": 174.7756,  "tz": "Pacific/Auckland",                "unit": "C"},
    "lucknow":       {"lat": 26.8467,  "lon": 80.9462,   "tz": "Asia/Kolkata",                    "unit": "C"},
    "wuhan":         {"lat": 30.5928,  "lon": 114.3052,  "tz": "Asia/Shanghai",                   "unit": "C"},
    "chengdu":       {"lat": 30.5728,  "lon": 104.0668,  "tz": "Asia/Shanghai",                   "unit": "C"},
    "chongqing":     {"lat": 29.5630,  "lon": 106.5516,  "tz": "Asia/Shanghai",                   "unit": "C"},
    "shenzhen":      {"lat": 22.5431,  "lon": 114.0579,  "tz": "Asia/Shanghai",                   "unit": "C"},
}

def send_telegram(msg):
    url = "https://api.telegram.org/bot" + TELEGRAM_TOKEN + "/sendMessage"
    try:
        requests.post(url, data={"chat_id": CHAT_ID, "text": msg}, timeout=10)
    except:
        pass

def make_slug(city, target_date):
    month = target_date.strftime("%B").lower()
    day = target_date.day
    year = target_date.year
    return "highest-temperature-in-" + city + "-on-" + month + "-" + str(day) + "-" + str(year)

def get_polymarket_prices(slug):
    try:
        url = "https://gamma-api.polymarket.com/events?slug=" + slug
        r = requests.get(url, timeout=10)
        data = r.json()
        if not data:
            return []
        markets = data[0].get("markets", [])
        results = []
        for m in markets:
            question = m.get("question", "")
            price = float(m.get("lastTradePrice", 0)) * 100
            results.append((question, round(price, 1)))
        return results
    except:
        return []

def get_forecast(city_key, target_date):
    if city_key not in CITIES:
        return None, None
    c = CITIES[city_key]
    try:
        unit_param = "fahrenheit" if c["unit"] == "F" else "celsius"
        days_ahead = (target_date - date.today()).days + 1
        days_ahead = max(1, min(days_ahead, 7))
        url = (
            "https://api.open-meteo.com/v1/forecast"
            "?latitude=" + str(c["lat"]) +
            "&longitude=" + str(c["lon"]) +
            "&daily=temperature_2m_max" +
            "&temperature_unit=" + unit_param +
            "&timezone=" + c["tz"] +
            "&forecast_days=" + str(days_ahead)
        )
        r = requests.get(url, timeout=10)
        data = r.json()
        temps = data["daily"]["temperature_2m_max"]
        return round(temps[-1], 1), c["unit"]
    except Exception as e:
        return None, None

def find_best_bucket(prices, forecast_temp):
    best = None
    best_diff = 999
    for question, price in prices:
        for t in range(-20, 150):
            if str(t) in question:
                diff = abs(t - forecast_temp)
                if diff < best_diff:
                    best_diff = diff
                    best = (question, price, t)
    return best, best_diff

def analyze(city, target_date):
    slug = make_slug(city, target_date)
    prices = get_polymarket_prices(slug)
    if not prices:
        return None

    forecast_temp, unit = get_forecast(city, target_date)
    if not forecast_temp:
        return None

    match, diff = find_best_bucket(prices, forecast_temp)
    if not match:
        return None

    question, price, bucket = match
    opportunity = price < 35 and diff <= 1

    return {
        "city": city,
        "date": target_date.isoformat(),
        "forecast": str(forecast_temp) + unit,
        "bucket": str(bucket) + unit,
        "price": price,
        "diff": diff,
        "opportunity": opportunity,
        "slug": slug
    }

def main():
    send_telegram("WeatherBot FULL basladi! Tum sehirler taraniyor...")
    print("Bot basladi.")

    while True:
        today = date.today()
        targets = [today, today + timedelta(days=1)]

        opportunities = []
        scanned = 0

        for target_date in targets:
            for city in CITIES:
                result = analyze(city, target_date)
                if result:
                    scanned += 1
                    if result["opportunity"]:
                        opportunities.append(result)
                time.sleep(0.3)

        if opportunities:
            msg = "*** FIRSAT ALARMI ***\n\n"
            for r in opportunities:
                msg += r["city"].upper() + " (" + r["date"] + ")\n"
                msg += "Tahmin: " + r["forecast"] + "\n"
                msg += "Market: " + r["bucket"] + " -> %" + str(r["price"]) + "\n\n"
            send_telegram(msg)
        else:
            send_telegram("Tarama tamam. " + str(scanned) + " market analiz edildi. Belirgin firsat yok.")

        print("Sonraki tarama 3 dakika sonra...")
        time.sleep(180)

if __name__ == "__main__":
    main()
