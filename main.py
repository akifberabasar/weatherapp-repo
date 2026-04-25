import requests
import time
import json
import sqlite3
import re
import os
import threading
import statistics
from datetime import date, datetime, timedelta

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "6100462157")

DB_PATH = "/mnt/data/bot.db" if os.path.isdir("/mnt/data") else "bot.db"

# Ensemble'da kullanacağımız modeller
# v6.4: JMA çıkarıldı (backtest'te %42 ile en kötü performans)
FORECAST_MODELS = ["ecmwf_ifs025", "gfs_seamless", "icon_seamless"]

# Modeller arası standart sapma bu değerden büyükse fırsat verme
# (yani modeller dağınıksa güven düşük)
MAX_MODEL_SPREAD_C = 1.5  # Celsius şehirler için
MAX_MODEL_SPREAD_F = 2.7  # Fahrenheit şehirler için (yaklaşık 1.5°C)

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
    # v6.3'te eklenen kolonlar - ALTER TABLE ile eski DB'ye de eklenebilir
    try:
        c.execute("ALTER TABLE alerts ADD COLUMN model_ecmwf REAL")
        c.execute("ALTER TABLE alerts ADD COLUMN model_gfs REAL")
        c.execute("ALTER TABLE alerts ADD COLUMN model_icon REAL")
        c.execute("ALTER TABLE alerts ADD COLUMN model_jma REAL")
        c.execute("ALTER TABLE alerts ADD COLUMN model_spread REAL")
    except sqlite3.OperationalError:
        pass  # Kolonlar zaten var
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
                 bucket_low, bucket_high, price, event_slug, market_id,
                 model_ecmwf, model_gfs, model_icon, model_jma, model_spread)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            datetime.utcnow().isoformat(),
            row["city"], row["target_date"], row["forecast"], row["unit"],
            row["bucket_title"], row["bucket_low"], row["bucket_high"],
            row["price"], row["event_slug"], row["market_id"],
            row.get("model_ecmwf"), row.get("model_gfs"),
            row.get("model_icon"), row.get("model_jma"),
            row.get("model_spread"),
        ))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()

def db_get_pending_alerts():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    today_str = date.today().isoformat()
    c.execute("""
        SELECT id, city, target_date, forecast, unit, bucket_title,
               bucket_low, bucket_high, price, event_slug
        FROM alerts
        WHERE won IS NULL AND target_date < ?
    """, (today_str,))
    rows = c.fetchall()
    conn.close()
    return rows

def db_update_result(alert_id, actual_temp, actual_bucket, won):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        UPDATE alerts
        SET actual_temp = ?, actual_bucket = ?, won = ?
        WHERE id = ?
    """, (actual_temp, actual_bucket, won, alert_id))
    conn.commit()
    conn.close()

def db_get_stats():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        SELECT city, target_date, forecast, unit, bucket_title, price,
               won, actual_bucket, model_spread
        FROM alerts ORDER BY ts DESC LIMIT 20
    """)
    rows = c.fetchall()
    c.execute("SELECT COUNT(*), SUM(CASE WHEN won=1 THEN 1 ELSE 0 END), SUM(CASE WHEN won=0 THEN 1 ELSE 0 END) FROM alerts")
    total, won, lost = c.fetchone()
    c.execute("SELECT price, won FROM alerts WHERE won IS NOT NULL")
    pnl = 0.0
    for price, w in c.fetchall():
        if w == 1:
            pnl += (100.0 / price - 1) * 3
        else:
            pnl -= 3
    conn.close()
    return rows, (total or 0, won or 0, lost or 0), pnl

# -------------------- Telegram --------------------

def send_telegram(msg):
    if not TELEGRAM_TOKEN:
        return
    try:
        url = "https://api.telegram.org/bot" + TELEGRAM_TOKEN + "/sendMessage"
        requests.post(url, data={"chat_id": CHAT_ID, "text": msg}, timeout=10)
    except Exception as e:
        print("Telegram hatasi:", e)

def build_stats_message():
    rows, (total, won, lost), pnl = db_get_stats()
    if total == 0:
        return "Henuz uyari yok."
    pending = total - won - lost
    msg = "== BOT STATS ==\n"
    msg += "Toplam uyari: " + str(total) + "\n"
    msg += "Kazanan: " + str(won) + " | Kaybeden: " + str(lost) + " | Bekleyen: " + str(pending) + "\n"
    if won + lost > 0:
        win_rate = won / (won + lost) * 100
        msg += "Tutma orani: %" + str(round(win_rate, 1)) + "\n"
        msg += "Hipotetik PnL ($3/bahis): $" + str(round(pnl, 2)) + "\n"
    msg += "\n-- Son 20 uyari --\n"
    for r in rows:
        city, tdate, forecast, unit, bucket, price, w, actual, spread = r
        if w == 1:
            mark = "+"
        elif w == 0:
            mark = "-"
        else:
            mark = "?"
        line = mark + " " + city[:8] + " " + tdate[5:] + " " + str(forecast) + unit + " -> " + bucket + " %" + str(price)
        if spread is not None:
            line += " (spread:" + str(round(spread, 1)) + ")"
        if actual and w is not None:
            line += " gercek:" + actual
        msg += line + "\n"
    return msg

# -------------------- Telegram listener --------------------

def telegram_listener():
    last_update_id = 0
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
                if chat_id != str(CHAT_ID):
                    continue
                if text == "/stats":
                    send_telegram(build_stats_message())
                elif text == "/ping":
                    send_telegram("Bot ayakta. DB: " + DB_PATH)
                elif text == "/check":
                    send_telegram("Sonuc kontrolu baslatildi...")
                    count = check_results()
                    send_telegram("Kontrol bitti. " + str(count) + " uyari guncellendi.")
        except Exception as e:
            print("Listener hatasi:", e)
            time.sleep(5)

# -------------------- Bucket parsing --------------------

def parse_bucket(title, unit):
    t = title.replace("°F", "").replace("°C", "").replace("\u00b0F", "").replace("\u00b0C", "").strip()
    tl = t.lower()
    if "or below" in tl:
        n = re.search(r"-?\d+", t)
        return (-999, int(n.group())) if n else None
    if "or higher" in tl or "or above" in tl:
        n = re.search(r"-?\d+", t)
        return (int(n.group()), 999) if n else None
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

def get_polymarket_winner(markets):
    for m in markets:
        try:
            prices = m.get("outcomePrices", "[]")
            if isinstance(prices, str):
                prices = json.loads(prices)
            if len(prices) >= 2 and float(prices[0]) == 1.0 and float(prices[1]) == 0.0:
                return m.get("groupItemTitle") or ""
        except Exception:
            continue
    return None

# -------------------- Ensemble Forecast --------------------

def get_ensemble_forecast(city_key, target_date):
    """
    4 modelin tahminini tek API çağrısı ile çeker.
    Döner: (ortalama, dict[model->tahmin], spread, unit) veya (None, None, None, None)
    """
    if city_key not in CITIES:
        return None, None, None, None
    c = CITIES[city_key]
    try:
        unit_param = "fahrenheit" if c["unit"] == "F" else "celsius"
        days_ahead = (target_date - date.today()).days + 1
        days_ahead = max(1, min(days_ahead, 7))
        models_str = ",".join(FORECAST_MODELS)
        url = (
            "https://api.open-meteo.com/v1/forecast"
            "?latitude=" + str(c["lat"]) +
            "&longitude=" + str(c["lon"]) +
            "&daily=temperature_2m_max" +
            "&temperature_unit=" + unit_param +
            "&timezone=" + c["tz"] +
            "&forecast_days=" + str(days_ahead) +
            "&models=" + models_str
        )
        r = requests.get(url, timeout=20)
        data = r.json()
        daily = data.get("daily", {})

        model_temps = {}
        for model in FORECAST_MODELS:
            key = "temperature_2m_max_" + model
            values = daily.get(key, [])
            if values and values[-1] is not None:
                model_temps[model] = round(float(values[-1]), 1)

        if len(model_temps) < 2:
            # En az 2 model lazım ensemble için
            return None, None, None, None

        temps = list(model_temps.values())
        avg = round(sum(temps) / len(temps), 1)
        spread = round(max(temps) - min(temps), 2) if len(temps) > 1 else 0.0

        return avg, model_temps, spread, c["unit"]
    except Exception as e:
        print("Ensemble forecast hatasi", city_key, e)
        return None, None, None, None

def get_archive_temp(city_key, target_date):
    if city_key not in CITIES:
        return None
    c = CITIES[city_key]
    try:
        unit_param = "fahrenheit" if c["unit"] == "F" else "celsius"
        date_str = target_date.isoformat()
        url = (
            "https://archive-api.open-meteo.com/v1/archive"
            "?latitude=" + str(c["lat"]) +
            "&longitude=" + str(c["lon"]) +
            "&start_date=" + date_str +
            "&end_date=" + date_str +
            "&daily=temperature_2m_max" +
            "&temperature_unit=" + unit_param +
            "&timezone=" + c["tz"]
        )
        r = requests.get(url, timeout=15)
        data = r.json()
        temps = data.get("daily", {}).get("temperature_2m_max", [])
        if not temps or temps[0] is None:
            return None
        return round(float(temps[0]), 1)
    except Exception as e:
        print("Archive hatasi", city_key, e)
        return None

# -------------------- Result check --------------------

def check_results():
    pending = db_get_pending_alerts()
    if not pending:
        return 0
    updated = 0
    notifications = []
    for row in pending:
        (alert_id, city, target_date_str, forecast, unit,
         bucket_title, bucket_low, bucket_high, price, event_slug) = row
        target_date_obj = date.fromisoformat(target_date_str)
        winner_title = None
        try:
            markets = get_polymarket_markets(event_slug)
            if markets:
                winner_title = get_polymarket_winner(markets)
        except Exception as e:
            print("check_results Polymarket hatasi:", city, e)
        actual_temp = None
        actual_bucket = None
        won = None
        if winner_title:
            actual_bucket = winner_title
            won = 1 if winner_title == bucket_title else 0
        else:
            days_old = (date.today() - target_date_obj).days
            if days_old >= 4:
                real_temp = get_archive_temp(city, target_date_obj)
                if real_temp is not None:
                    actual_temp = real_temp
                    try:
                        markets = get_polymarket_markets(event_slug)
                        for m in markets:
                            title = m.get("groupItemTitle") or ""
                            b = parse_bucket(title, unit)
                            if b and bucket_contains(b, real_temp):
                                actual_bucket = title
                                break
                    except Exception:
                        pass
                    if actual_bucket:
                        won = 1 if actual_bucket == bucket_title else 0
        if won is not None:
            db_update_result(alert_id, actual_temp, actual_bucket, won)
            updated += 1
            mark = "KAZANDI" if won == 1 else "KAYBETTI"
            line = city.upper() + " " + target_date_str + " " + mark
            line += " | tahmin: " + bucket_title
            if actual_bucket:
                line += " | gercek: " + actual_bucket
            if actual_temp:
                line += " (" + str(actual_temp) + unit + ")"
            notifications.append(line)
        time.sleep(0.3)
    if notifications:
        msg = "== SONUCLAR ==\n\n" + "\n".join(notifications)
        send_telegram(msg)
    return updated

# -------------------- Analiz --------------------

def analyze(city, target_date):
    slug = make_slug(city, target_date)
    markets = get_polymarket_markets(slug)
    if not markets:
        return None

    avg_temp, model_temps, spread, unit = get_ensemble_forecast(city, target_date)
    if avg_temp is None:
        return None

    # Modellerin ne kadar hemfikir olduğu — güven metriği
    max_spread = MAX_MODEL_SPREAD_F if unit == "F" else MAX_MODEL_SPREAD_C
    low_confidence = spread > max_spread

    # Ortalamanın düştüğü bucket'ı bul
    matched = None
    for m in markets:
        title = m.get("groupItemTitle") or ""
        bucket = parse_bucket(title, unit)
        if not bucket:
            continue
        if bucket_contains(bucket, avg_temp):
            price = extract_yes_price(m)
            if price is None:
                continue
            matched = {
                "title": title, "low": bucket[0], "high": bucket[1],
                "price": price, "market_id": str(m.get("id", "")),
            }
            break
    if not matched:
        return None

    # Filtre: guvenilir sehir + %8-30 fiyat + modeller hemfikir
    opportunity = (
        city in RELIABLE_CITIES and
        8 <= matched["price"] <= 30 and
        not low_confidence
    )

    return {
        "city": city,
        "target_date": target_date.isoformat(),
        "forecast": avg_temp,
        "unit": unit,
        "bucket_title": matched["title"],
        "bucket_low": matched["low"],
        "bucket_high": matched["high"],
        "price": matched["price"],
        "event_slug": slug,
        "market_id": matched["market_id"],
        "model_ecmwf": model_temps.get("ecmwf_ifs025"),
        "model_gfs": model_temps.get("gfs_seamless"),
        "model_icon": model_temps.get("icon_seamless"),
        "model_jma": model_temps.get("jma_gsm"),
        "model_spread": spread,
        "low_confidence": low_confidence,
        "opportunity": opportunity,
    }

# -------------------- Ana döngü --------------------

def main():
    db_init()
    listener_thread = threading.Thread(target=telegram_listener, daemon=True)
    listener_thread.start()

    send_telegram("WeatherBot v6.4 basladi. Ensemble: ECMWF+GFS+ICON (JMA cikarildi).")
    print("Bot basladi. DB:", DB_PATH)

    loop_count = 0
    while True:
        try:
            if loop_count % 6 == 0:
                try:
                    updated = check_results()
                    if updated > 0:
                        print("Sonuc kontrolu: " + str(updated) + " uyari guncellendi.")
                except Exception as e:
                    print("check_results hatasi:", e)

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
                    time.sleep(0.4)  # 4 model => biraz daha yavaş gidelim
            if new_opportunities:
                msg = "*** GERCEK FIRSAT ***\n\n"
                for r in new_opportunities:
                    msg += r["city"].upper() + " (" + r["target_date"] + ")\n"
                    msg += "Ensemble: " + str(r["forecast"]) + r["unit"] + "\n"
                    # Her modelin ayrı tahmini
                    mt = []
                    if r.get("model_ecmwf") is not None:
                        mt.append("ECMWF:" + str(r["model_ecmwf"]))
                    if r.get("model_gfs") is not None:
                        mt.append("GFS:" + str(r["model_gfs"]))
                    if r.get("model_icon") is not None:
                        mt.append("ICON:" + str(r["model_icon"]))
                    if r.get("model_jma") is not None:
                        mt.append("JMA:" + str(r["model_jma"]))
                    msg += "Modeller: " + " ".join(mt) + "\n"
                    msg += "Spread: " + str(r["model_spread"]) + r["unit"] + "\n"
                    msg += "Bucket: " + r["bucket_title"] + " -> %" + str(r["price"]) + "\n\n"
                msg += "Paper trading. /stats ile ozet."
                send_telegram(msg)
            else:
                print("Tarama: " + str(scanned) + " sehir, " + str(market_found) + " market, yeni firsat yok.")
            loop_count += 1
        except Exception as e:
            print("Ana dongu hatasi:", e)
        print("Sonraki tarama 10 dakika sonra...")
        time.sleep(600)

if __name__ == "__main__":
    main()
