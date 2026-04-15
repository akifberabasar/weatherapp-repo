import requests
import time
from datetime import date, timedelta

TELEGRAM_TOKEN = "8436085274:AAH78VaM9i1JLISBGApGye9FZ2q7b2_Vkro"
CHAT_ID = "6100462157"

# Havalimanı koordinatları - Polymarket'ın kullandığı istasyonlar
CITIES = {
    "nyc":           {"lat": 40.7769,  "lon": -73.8740,  "tz": "America/New_York",                "unit": "F"},  # LaGuardia
    "dallas":        {"lat": 32.8481,  "lon": -96.8512,  "tz": "America/Chicago",                 "unit": "F"},  # Dallas Love Field
    "atlanta":       {"lat": 33.6407,  "lon": -84.4277,  "tz": "America/New_York",                "unit": "F"},  # Hartsfield-Jackson
    "chicago":       {"lat": 41.9742,  "lon": -87.9073,  "tz": "America/Chicago",                 "unit": "F"},  # O'Hare
    "miami":         {"lat": 25.7959,  "lon": -80.2870,  "tz": "America/New_York",                "unit": "F"},  # Miami Intl
    "seattle":       {"lat": 47.4502,  "lon": -122.3088, "tz": "America/Los_Angeles",             "unit": "F"},  # Sea-Tac
    "los-angeles":   {"lat": 33.9425,  "lon": -118.4081, "tz": "America/Los_Angeles",             "unit": "F"},  # LAX
    "houston":       {"lat": 29.9902,  "lon": -95.3368,  "tz": "America/Chicago",                 "unit": "F"},  # William P. Hobby
    "denver":        {"lat": 39.7170,  "lon": -104.7508, "tz": "America/Denver",                  "unit": "F"},  # Buckley SFB
    "austin":        {"lat": 30.1975,  "lon": -97.6664,  "tz": "America/Chicago",                 "unit": "F"},  # Austin-Bergstrom
    "san-francisco": {"lat": 37.6213,  "lon": -122.3790, "tz": "America/Los_Angeles",             "unit": "F"},  # SFO
    "toronto":       {"lat": 43.6772,  "lon": -79.6306,  "tz": "America/Toronto",                 "unit": "C"},  # Pearson
    "london":        {"lat": 51.5033,  "lon": 0.0551,    "tz": "Europe/London",                   "unit": "C"},  # London City Airport
    "paris":         {"lat": 49.0097,  "lon": 2.5479,    "tz": "Europe/Paris",                    "unit": "C"},  # Charles de Gaulle
    "madrid":        {"lat": 40.4983,  "lon": -3.5676,   "tz": "Europe/Madrid",                   "unit": "C"},  # Barajas
    "milan":         {"lat": 45.6306,  "lon": 8.7281,    "tz": "Europe/Rome",                     "unit": "C"},  # Malpensa
    "warsaw":        {"lat": 52.1657,  "lon": 20.9671,   "tz": "Europe/Warsaw",                   "unit": "C"},  # Chopin
    "munich":        {"lat": 48.3537,  "lon": 11.7750,   "tz": "Europe/Berlin",                   "unit": "C"},  # Munich Airport
    "amsterdam":     {"lat": 52.3086,  "lon": 4.7639,    "tz": "Europe/Amsterdam",                "unit": "C"},  # Schiphol
    "helsinki":      {"lat": 60.3172,  "lon": 24.9633,   "tz": "Europe/Helsinki",                 "unit": "C"},  # Vantaa
    "moscow":        {"lat": 55.5983,  "lon": 37.2611,   "tz": "Europe/Moscow",                   "unit": "C"},  # Vnukovo
    "istanbul":      {"lat": 41.2608,  "lon": 28.7418,   "tz": "Europe/Istanbul",                 "unit": "C"},  # Istanbul Airport (IST)
    "ankara":        {"lat": 40.1281,  "lon": 32.9951,   "tz": "Europe/Istanbul",                 "unit": "C"},  # Esenboga
    "tel-aviv":      {"lat": 32.0114,  "lon": 34.8867,   "tz": "Asia/Jerusalem",                  "unit": "C"},  # Ben Gurion
    "seoul":         {"lat": 37.4602,  "lon": 126.4407,  "tz": "Asia/Seoul",                      "unit": "C"},  # Incheon
    "busan":         {"lat": 35.1795,  "lon": 128.9382,  "tz": "Asia/Seoul",                      "unit": "C"},  # Gimhae
    "tokyo":         {"lat": 35.5494,  "lon": 139.7798,  "tz": "Asia/Tokyo",                      "unit": "C"},  # Haneda
    "beijing":       {"lat": 40.0799,  "lon": 116.5844,  "tz": "Asia/Shanghai",                   "unit": "C"},  # Capital Intl
    "shanghai":      {"lat": 31.1443,  "lon": 121.8083,  "tz": "Asia/Shanghai",                   "unit": "C"},  # Pudong
    "hong-kong":     {"lat": 22.3080,  "lon": 113.9185,  "tz": "Asia/Hong_Kong",                  "unit": "C"},  # HK Observatory area
    "singapore":     {"lat": 1.3644,   "lon": 103.9915,  "tz": "Asia/Singapore",                  "unit": "C"},  # Changi
    "taipei":        {"lat": 25.0697,  "lon": 121.5524,  "tz": "Asia/Taipei",                     "unit": "C"},  # Songshan
    "kuala-lumpur":  {"lat": 2.7456,   "lon": 101.7099,  "tz": "Asia/Kuala_Lumpur",               "unit": "C"},  # KLIA
    "jakarta":       {"lat": -6.2661,  "lon": 106.8908,  "tz": "Asia/Jakarta",                    "unit": "C"},  # Halim
    "buenos-aires":  {"lat": -34.8222, "lon": -58.5358,  "tz": "America/Argentina/Buenos_Aires",  "unit": "C"},  # Pistarini
    "sao-paulo":     {"lat": -23.4356, "lon": -46.4731,  "tz": "America/Sao_Paulo",               "unit": "C"},  # Guarulhos
    "mexico-city":   {"lat": 19.4363,  "lon": -99.0721,  "tz": "America/Mexico_City",             "unit": "C"},  # Benito Juarez
    "panama-city":   {"lat": 9.0714,   "lon": -79.3836,  "tz": "America/Panama",                  "unit": "C"},  # Marcos A. Gelabert
    "wellington":    {"lat": -41.3272, "lon": 174.8052,  "tz": "Pacific/Auckland",                "unit": "C"},  # Wellington Intl
    "lucknow":       {"lat": 26.7606,  "lon": 80.8893,   "tz": "Asia/Kolkata",                    "unit": "C"},  # Chaudhary Charan Singh
    "wuhan":         {"lat": 30.7838,  "lon": 114.2081,  "tz": "Asia/Shanghai",                   "unit": "C"},  # Tianhe
    "chengdu":       {"lat": 30.5785,  "lon": 103.9473,  "tz": "Asia/Shanghai",                   "unit": "C"},  # Shuangliu
    "chongqing":     {"lat": 29.7192,  "lon": 106.6419,  "tz": "Asia/Shanghai",                   "unit": "C"},  # Jiangbei
    "shenzhen":      {"lat": 22.6329,  "lon": 113.8108,  "tz": "Asia/Shanghai",                   "unit": "C"},  # Bao'an
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
    except:
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
    send_telegram("WeatherBot AIRPORT koordinatlari ile basladi!")
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
