import requests
import time
import json
import sqlite3
import re
import os
import threading
from datetime import date, datetime, timedelta

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "6100462157")

DB_PATH = "/mnt/data/bot.db" if os.path.isdir("/mnt/data") else "bot.db"

CITIES = {
    "nyc":           {"lat": 40.7769,  "lon": -73.8740,  "tz": "America/New_York",                "unit": "F"},
    "dallas":        {"lat": 32.8481,  "lon": -96.8512,  "tz": "America/Chicago",                 "unit": "F"},
    "atlanta":       {"lat": 33.6407,  "lon": -84.4277,  "tz": "America/New_York",                "unit": "F"},
    "chicago":       {"lat": 41.9742,  "lon": -87.9073,  "tz": "America/Chicago",                 "unit": "F"},
    "miami":         {"lat": 25.7959,  "lon": -80.2870,  "tz": "America/New_York",                "unit": "F"},
    "seattle":       {"lat": 47.4502,  "lon": -122.3088, "tz": "America/Los_Angeles",             "unit": "F"},
    "los-angeles":   {"lat": 33.9425,  "lon": -118.4081, "tz": "America/Los_Angeles",             "unit": "F"},
    "houston":       {"lat": 29.9902,  "lon": -95.3368,  "tz": "America/Chicago",                 "unit": "F"},
    "denver":        {"lat": 39.7170,  "lon": -104.7508, "tz": "America/Denver",                  "unit": "F"},
    "austin":        {"lat": 30.1975,  "lon": -97.6664,  "tz": "America/Chicago",                 "unit": "F"},
    "san-francisco": {"lat": 37.6213,  "lon": -122.3790, "tz": "America/Los_Angeles",             "unit": "F"},
    "toronto":       {"lat": 43.6772,  "lon": -79.6306,  "tz": "America/Toronto",                 "unit": "C"},
    "london":        {"lat": 51.5033,  "lon": 0.0551,    "tz": "Europe/London",                   "unit": "C"},
    "paris":         {"lat": 49.0097,  "lon": 2.5479,    "tz": "Europe/Paris",                    "unit": "C"},
    "madrid":        {"lat": 40.4983,  "lon": -3.5676,   "tz": "Europe/Madrid",                   "unit": "C"},
    "milan":         {"lat": 45.6306,  "lon": 8.7281,    "tz": "Europe/Rome",                     "unit": "C"},
    "warsaw":        {"lat": 52.1657,  "lon": 20.9671,   "tz": "Europe/Warsaw",                   "unit": "C"},
    "munich":        {"lat": 48.3537,  "lon": 11.7750,   "tz": "Europe/Berlin",                   "unit": "C"},
    "amsterdam":     {"lat": 52.3086,  "lon": 4.7639,    "tz": "Europe/Amsterdam",                "unit": "C"},
    "helsinki":      {"lat": 60.3172,  "lon": 24.9633,   "tz": "Europe/Helsinki",                 "unit": "C"},
    "moscow":        {"lat": 55.5983,  "lon": 37.2611,   "tz": "Europe/Moscow",                   "unit": "C"},
    "istanbul":      {"lat": 41.2608,  "lon": 28.7418,   "tz": "Europe/Istanbul",                 "unit": "C"},
    "ankara":        {"lat": 40.1281,  "lon": 32.9951,   "tz": "Europe/Istanbul",                 "unit": "C"},
    "tel-aviv":      {"lat": 32.0114,  "lon": 34.8867,   "tz": "Asia/Jerusalem",                  "unit": "C"},
    "seoul":         {"lat": 37.4602,  "lon": 126.4407,  "tz": "Asia/Seoul",                      "unit": "C"},
    "busan":         {"lat": 35.1795,  "lon": 128.9382,  "tz": "Asia/Seoul",                      "unit": "C"},
    "tokyo":         {"lat": 35.5494,  "lon": 139.7798,  "tz": "Asia/Tokyo",                      "unit": "C"},
    "beijing":       {"lat": 40.0799,  "lon": 116.5844,  "tz": "Asia/Shanghai",                   "unit": "C"},
    "shanghai":      {"lat": 31.1443,  "lon": 121.8083,  "tz": "Asia/Shanghai",                   "unit": "C"},
    "hong-kong":     {"lat": 22.3080,  "lon": 113.9185,  "tz": "Asia/Hong_Kong",                  "unit": "C"},
    "singapore":     {"lat": 1.3644,   "lon": 103.9915,  "tz": "Asia/Singapore",                  "unit": "C"},
    "taipei":        {"lat": 25.0697,  "lon": 121.5524,  "tz": "Asia/Taipei",                     "unit": "C"},
    "kuala-lumpur":  {"lat": 2.7456,   "lon": 101.7099,  "tz": "Asia/Kuala_Lumpur",               "unit": "C"},
    "jakarta":       {"lat": -6.2661,  "lon": 106.8908,  "tz": "Asia/Jakarta",                    "unit": "C"},
    "buenos-aires":  {"lat": -34.8222, "lon": -58.5358,  "tz": "America/Argentina/Buenos_Aires",  "unit": "C"},
    "sao-paulo":     {"lat": -23.4356, "lon": -46.4731,  "tz": "America/Sao_Paulo",               "unit": "C"},
    "mexico-city":   {"lat": 19.4363,  "lon": -99.0721,  "tz": "America/Mexico_City",             "unit": "C"},
    "panama-city":   {"lat": 9.0714,   "lon": -79.3836,  "tz": "America/Panama",                  "unit": "C"},
    "wellington":    {"lat": -41.3272, "lon": 174.8052,  "tz": "Pacific/Auckland",                "unit": "C"},
    "lucknow":       {"lat": 26.7606,  "lon": 80.8893,   "tz": "Asia/Kolkata",                    "unit": "C"},
    "wuhan":         {"lat": 30.7838,  "lon": 114.2081,  "tz": "Asia/Shanghai",                   "unit": "C"},
    "chengdu":       {"lat": 30.5785,  "lon": 103.9473,  "tz": "Asia/Shanghai",                   "unit": "C"},
    "chongqing":     {"lat": 29.7192,  "lon": 106.6419,  "tz": "Asia/Shanghai",                   "unit": "C"},
    "shenzhen":      {"lat": 22.6329,  "lon": 113.8108,  "tz": "Asia/Shanghai",                   "unit": "C"},
}

RELIABLE_CITIES = {
    "london", "paris", "madrid", "milan", "tokyo",
    "seoul", "nyc", "atlanta", "chicago", "miami",
    "istanbul", "amsterdam", "munich", "singapore"
}

# -------------------- DB --------------------

def db_init():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            city TEXT NOT NULL,
            target_date TEXT NOT NULL,
            forecast REAL NOT NULL,
            unit TEXT NOT NULL,
            bucket_title TEXT NOT NULL,
            bucket_low REAL,
            bucket_high REAL,
            price REAL NOT NULL,
            event_slug TEXT NOT NULL,
            market_id TEXT,
            actual_temp REAL,
            actual_bucket TEXT,
            won INTEGER
        )
    """)
    c.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_alert_unique
        ON alerts(city, target_date, bucket_title)
    """)
    conn.commit()
    conn.close()

def db_save_alert(row):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute("""
            INSERT INTO alerts
                (ts, city, target_date, forecast, unit, bucket_title,
                 bucket_low, bucket_high, price, event_slug, market_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            datetime.utcnow().isoformat(),
            row["city"], row["target_date"], row["forecast"], row["unit"],
            row["bucket_title"], row["bucket_low"], row["bucket_high"],
            row["price"], row["event_slug"], row["market_id"],
        ))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()

def db_get_stats():
    """Stats özeti döner, /stats komutu için."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT city, target_date, forecast, unit, bucket_title, price, won FROM alerts ORDER BY ts DESC LIMIT 20")
    rows = c.fetchall()
    c.execute("SELECT COUNT(*), SUM(CASE WHEN won=1 THEN 1 ELSE 0 END), SUM(CASE WHEN won=0 THEN 1 ELSE 0 END) FROM alerts")
    total, won, lost = c.fetchone()
    conn.close()
    return rows, (total or 0, won or 0, lost or 0)

# -------------------- Telegram --------------------

def send_telegram(msg):
    if not TELEGRAM_TOKEN:
        print("TELEGRAM_TOKEN yok, mesaj atlandi")
        return
    try:
        url = "https://api.telegram.org/bot" + TELEGRAM_TOKEN + "/sendMessage"
        requests.post(url, data={"chat_id": CHAT_ID, "text": msg}, timeout=10)
    except Exception as e:
        print("Telegram hatasi:", e)

def build_stats_message():
    rows, (total, won, lost) = db_get_stats()
    if total == 0:
        return "Henuz uyari yok."

    pending = total - won - lost
    msg = "== BOT STATS ==\n"
    msg += "Toplam uyari: " + str(total) + "\n"
    msg += "Kazanan: " + str(won) + " | Kaybeden: " + str(lost) + " | Bekleyen: " + str(pending) + "\n"
    if won + lost > 0:
        win_rate = won / (won + lost) * 100
        msg += "Tutma orani: %" + str(round(win_rate, 1)) + "\n"
    msg += "\n-- Son 20 uyari --\n"
    for r in rows:
        city, tdate, forecast, unit, bucket, price, w = r
        if w == 1:
            mark = "+"
        elif w == 0:
            mark = "-"
        else:
            mark = "?"
        msg += mark + " " + city[:8] + " " + tdate[5:] + " " + str(forecast) + unit + " -> " + bucket + " %" + str(price) + "\n"
    return msg

# -------------------- Telegram komut dinleyici --------------------

def telegram_listener():
    """Ayrı thread'de çalışır, /stats komutunu dinler."""
    last_update_id = 0
    # Başlangıçta eski mesajları atlamak için son update_id'yi al
    try:
        url = "https://api.telegram.org/bot" + TELEGRAM_TOKEN + "/getUpdates"
        r = requests.get(url, timeout=10)
        updates = r.json().get("result", [])
        if updates:
            last_update_id = updates[-1]["update_id"]
    except Exception as e:
        print("Listener init hatasi:", e)

    while True:
        try:
            url = "https://api.telegram.org/bot" + TELEGRAM_TOKEN + "/getUpdates"
            params = {"offset": last_update_id + 1, "timeout": 30}
            r = requests.get(url, params=params, timeout=40)
            updates = r.json().get("result", [])

            for u in updates:
                last_update_id = u["update_id"]
                msg = u.get("message", {})
                text = msg.get("text", "").strip().lower()
                chat_id = str(msg.get("chat", {}).get("id", ""))

                # Sadece senin chat'inden komut kabul et
                if chat_id != str(CHAT_ID):
                    continue

                if text == "/stats":
                    send_telegram(build_stats_message())
                elif text == "/ping":
                    send_telegram("Bot ayakta. DB: " + DB_PATH)

        except Exception as e:
            print("Listener hatasi:", e)
            time.sleep(5)

# -------------------- Bucket parsing --------------------

def parse_bucket(title, unit):
    t = title.replace("°F", "").replace("°C", "").replace("\u00b0F", "").replace("\u00b0C", "").strip()
    tl = t.lower()
    if "or below" in tl:
        n = re.search(r"-?\d+", t)
        if not n:
            return None
        return (-999, int(n.group()))
    if "or higher" in tl or "or above" in tl:
        n = re.search(r"-?\d+", t)
        if not n:
            return None
        return (int(n.group()), 999)
    m = re.match(r"^(-?\d+)\s*-\s*(\d+)$", t)
    if m:
        return (int(m.group(1)), int(m.group(2)))
    m = re.match(r"^(-?\d+)$", t)
    if m:
        n = int(m.group(1))
        return (n, n)
    return None

def bucket_contains(bucket, temp):
    low, high = bucket
    return low <= temp <= high

# -------------------- Polymarket --------------------

def make_slug(city, target_date):
    month = target_date.strftime("%B").lower()
    return "highest-temperature-in-" + city + "-on-" + month + "-" + str(target_date.day) + "-" + str(target_date.year)

def get_polymarket_markets(slug):
    try:
        url = "https://gamma-api.polymarket.com/events?slug=" + slug
        r = requests.get(url, timeout=15)
        data = r.json()
        if not data:
            return []
        return data[0].get("markets", [])
    except Exception as e:
        print("Polymarket hatasi (" + slug + "):", e)
        return []

def extract_yes_price(market):
    try:
        prices = market.get("outcomePrices", "[]")
        if isinstance(prices, str):
            prices = json.loads(prices)
        if not prices:
            return None
        return round(float(prices[0]) * 100, 2)
    except Exception:
        return None

# -------------------- Forecast --------------------

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
        r = requests.get(url, timeout=15)
        data = r.json()
        temps = data["daily"]["temperature_2m_max"]
        return round(temps[-1], 1), c["unit"]
    except Exception as e:
        print("Forecast hatasi", city_key, e)
        return None, None

# -------------------- Analiz --------------------

def analyze(city, target_date):
    slug = make_slug(city, target_date)
    markets = get_polymarket_markets(slug)
    if not markets:
        return None
    forecast_temp, unit = get_forecast(city, target_date)
    if forecast_temp is None:
        return None
    matched = None
    for m in markets:
        title = m.get("groupItemTitle") or ""
        bucket = parse_bucket(title, unit)
        if not bucket:
            continue
        if bucket_contains(bucket, forecast_temp):
            price = extract_yes_price(m)
            if price is None:
                continue
            matched = {
                "title": title,
                "low": bucket[0],
                "high": bucket[1],
                "price": price,
                "market_id": str(m.get("id", "")),
            }
            break
    if not matched:
        return None
    opportunity = (
        city in RELIABLE_CITIES and
        8 <= matched["price"] <= 30
    )
    return {
        "city": city,
        "target_date": target_date.isoformat(),
        "forecast": forecast_temp,
        "unit": unit,
        "bucket_title": matched["title"],
        "bucket_low": matched["low"],
        "bucket_high": matched["high"],
        "price": matched["price"],
        "event_slug": slug,
        "market_id": matched["market_id"],
        "opportunity": opportunity,
    }

# -------------------- Ana döngü --------------------

def main():
    db_init()

    # Telegram listener'ı ayrı thread'de başlat
    listener_thread = threading.Thread(target=telegram_listener, daemon=True)
    listener_thread.start()

    send_telegram("WeatherBot v6.1 basladi. /stats ve /ping komutlari aktif.")
    print("Bot basladi. DB:", DB_PATH)

    while True:
        try:
            today = date.today()
            targets = [today, today + timedelta(days=1)]
            new_opportunities = []
            scanned = 0
            market_found = 0
            for target_date in targets:
                for city in CITIES:
                    try:
                        result = analyze(city, target_date)
                        scanned += 1
                        if result:
                            market_found += 1
                            if result["opportunity"]:
                                saved = db_save_alert(result)
                                if saved:
                                    new_opportunities.append(result)
                    except Exception as e:
                        print("Sehir hatasi:", city, e)
                    time.sleep(0.3)
            if new_opportunities:
                msg = "*** GERCEK FIRSAT ***\n\n"
                for r in new_opportunities:
                    msg += r["city"].upper() + " (" + r["target_date"] + ")\n"
                    msg += "Tahmin: " + str(r["forecast"]) + r["unit"] + "\n"
                    msg += "Bucket: " + r["bucket_title"] + " -> %" + str(r["price"]) + "\n\n"
                msg += "Not: Paper trading modu. /stats ile ozet al."
                send_telegram(msg)
            else:
                print("Tarama: " + str(scanned) + " sehir, " + str(market_found) + " market, yeni firsat yok.")
        except Exception as e:
            print("Ana dongu hatasi:", e)
        print("Sonraki tarama 10 dakika sonra...")
        time.sleep(600)

if __name__ == "__main__":
    main()
