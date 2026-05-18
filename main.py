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

# v6.6 değişiklikleri:
# - Şehir koordinatları Polymarket resolution istasyonlarına göre güncellendi
# - Slug eşleşme kontrolü eklendi (API yanlış event dönerse alert üretmez)
# - Tarama 5 dakikaya indirildi (rekabet için)
# - DB'ye bias_offset kolonu eklendi (gelecekte otomatik bias düzeltme için)
# - Min spread filtresi eklendi (≤0.3 spread alertleri ele)
# - /verify komutu eklendi (Polymarket UI'daki fiyatla bot fiyatını karşılaştır)

FORECAST_MODELS = ["ecmwf_ifs025", "gfs_seamless", "icon_seamless"]

MAX_MODEL_SPREAD_C = 1.5
MAX_MODEL_SPREAD_F = 2.7

# Yeni: minimum spread (çok hemfikir model = market da hemfikir = edge yok)
MIN_MODEL_SPREAD_C = 0.3
MIN_MODEL_SPREAD_F = 0.5

# Tarama her N saniyede bir
SCAN_INTERVAL_SECONDS = 300  # 5 dk (eski: 600 = 10 dk)

# Polymarket resolution istasyonları
# Kaynak: her market sayfasında "resolution source" bölümünde yazıyor
# Buradaki koordinatlar GERÇEK havalimanı koordinatları
# Eğer bir şehrin istasyonu bilinmiyor/değişken ise STATION=None ve
# o şehir RELIABLE_CITIES'e eklenmemeli
CITIES = {
    # ABD — KLGA/KMIA/KORD vb. NOAA istasyonları
    "nyc":           {"lat": 40.7769, "lon": -73.8740, "tz": "America/New_York",     "unit": "F", "station": "KLGA"},        # LaGuardia
    "miami":         {"lat": 25.7959, "lon": -80.2870, "tz": "America/New_York",     "unit": "F", "station": "KMIA"},        # Miami Intl
    "chicago":       {"lat": 41.9742, "lon": -87.9073, "tz": "America/Chicago",      "unit": "F", "station": "KORD"},        # O'Hare
    "los-angeles":   {"lat": 33.9425, "lon": -118.4081,"tz": "America/Los_Angeles",  "unit": "F", "station": "KLAX"},        # LAX
    "dallas":        {"lat": 32.8471, "lon": -96.8518, "tz": "America/Chicago",      "unit": "F", "station": "KDAL"},        # Dallas Love Field — DFW değil!
    "atlanta":       {"lat": 33.6407, "lon": -84.4277, "tz": "America/New_York",     "unit": "F", "station": "KATL"},        # Hartsfield-Jackson
    "houston":       {"lat": 29.6452, "lon": -95.2789, "tz": "America/Chicago",      "unit": "F", "station": "KHOU"},        # Hobby
    "denver":        {"lat": 39.8617, "lon": -104.6731,"tz": "America/Denver",       "unit": "F", "station": "KDEN"},        # Denver Intl
    "san-francisco": {"lat": 37.6213, "lon": -122.3790,"tz": "America/Los_Angeles",  "unit": "F", "station": "KSFO"},        # SFO
    "seattle":       {"lat": 47.4502, "lon": -122.3088,"tz": "America/Los_Angeles",  "unit": "F", "station": "KSEA"},        # Sea-Tac
    "austin":        {"lat": 30.1975, "lon": -97.6664, "tz": "America/Chicago",      "unit": "F", "station": "KAUS"},        # Austin-Bergstrom

    # Avrupa
    "london":        {"lat": 51.5048, "lon":   0.0495, "tz": "Europe/London",        "unit": "C", "station": "EGLC"},        # London City Airport
    "paris":         {"lat": 48.7233, "lon":   2.3795, "tz": "Europe/Paris",         "unit": "C", "station": "LFPO"},        # Orly — bazen LFPB Le Bourget, sayfayı kontrol et!
    "madrid":        {"lat": 40.4983, "lon":  -3.5676, "tz": "Europe/Madrid",        "unit": "C", "station": "LEMD"},        # Barajas
    "milan":         {"lat": 45.6306, "lon":   8.7281, "tz": "Europe/Rome",          "unit": "C", "station": "LIMC"},        # Malpensa
    "munich":        {"lat": 48.3537, "lon":  11.7750, "tz": "Europe/Berlin",        "unit": "C", "station": "EDDM"},        # MUC
    "amsterdam":     {"lat": 52.3086, "lon":   4.7639, "tz": "Europe/Amsterdam",     "unit": "C", "station": "EHAM"},        # Schiphol
    "warsaw":        {"lat": 52.1657, "lon":  20.9671, "tz": "Europe/Warsaw",        "unit": "C", "station": "EPWA"},        # Chopin
    "helsinki":      {"lat": 60.3172, "lon":  24.9633, "tz": "Europe/Helsinki",      "unit": "C", "station": "EFHK"},        # Vantaa
    "moscow":        {"lat": 55.9726, "lon":  37.4146, "tz": "Europe/Moscow",        "unit": "C", "station": "UUEE"},        # Sheremetyevo
    "istanbul":      {"lat": 41.2608, "lon":  28.7418, "tz": "Europe/Istanbul",      "unit": "C", "station": "LTFM"},        # New Istanbul Airport
    "ankara":        {"lat": 40.1281, "lon":  32.9951, "tz": "Europe/Istanbul",      "unit": "C", "station": "LTAC"},        # Esenboga

    # Asya — büyük havalimanları, market sayfasından doğrula
    "tel-aviv":      {"lat": 32.0114, "lon":  34.8867, "tz": "Asia/Jerusalem",       "unit": "C", "station": "LLBG"},
    "seoul":         {"lat": 37.4602, "lon": 126.4407, "tz": "Asia/Seoul",           "unit": "C", "station": "RKSI"},        # Incheon
    "busan":         {"lat": 35.1795, "lon": 128.9382, "tz": "Asia/Seoul",           "unit": "C", "station": "RKPK"},
    "tokyo":         {"lat": 35.5494, "lon": 139.7798, "tz": "Asia/Tokyo",           "unit": "C", "station": "RJTT"},        # Haneda
    "beijing":       {"lat": 40.0799, "lon": 116.5844, "tz": "Asia/Shanghai",        "unit": "C", "station": "ZBAA"},
    "shanghai":      {"lat": 31.1443, "lon": 121.8083, "tz": "Asia/Shanghai",        "unit": "C", "station": "ZSPD"},        # Pudong
    "hong-kong":     {"lat": 22.3080, "lon": 113.9185, "tz": "Asia/Hong_Kong",       "unit": "C", "station": "VHHH"},
    "singapore":     {"lat":  1.3644, "lon": 103.9915, "tz": "Asia/Singapore",       "unit": "C", "station": "WSSS"},        # Changi
    "taipei":        {"lat": 25.0697, "lon": 121.2333, "tz": "Asia/Taipei",          "unit": "C", "station": "RCTP"},
    "kuala-lumpur":  {"lat":  2.7456, "lon": 101.7099, "tz": "Asia/Kuala_Lumpur",    "unit": "C", "station": "WMKK"},
    "jakarta":       {"lat": -6.1256, "lon": 106.6558, "tz": "Asia/Jakarta",         "unit": "C", "station": "WIII"},

    # Güney Yarımküre / Latin Amerika / Afrika
    "buenos-aires":  {"lat": -34.8222,"lon": -58.5358, "tz": "America/Argentina/Buenos_Aires", "unit": "C", "station": "SAEZ"},
    "cape-town":     {"lat": -33.9694,"lon":  18.6022, "tz": "Africa/Johannesburg",  "unit": "C", "station": "FACT"},        # Cape Town Intl
    "sao-paulo":     {"lat": -23.4356,"lon": -46.4731, "tz": "America/Sao_Paulo",    "unit": "C", "station": "SBGR"},
    "mexico-city":   {"lat": 19.4363, "lon": -99.0721, "tz": "America/Mexico_City",  "unit": "C", "station": "MMMX"},
    "panama-city":   {"lat":  9.0714, "lon": -79.3836, "tz": "America/Panama",       "unit": "C", "station": "MPTO"},
    "wellington":    {"lat": -41.3272,"lon": 174.8052, "tz": "Pacific/Auckland",     "unit": "C", "station": "NZWN"},

    # Çin diğer şehirler
    "wuhan":         {"lat": 30.7838, "lon": 114.2081, "tz": "Asia/Shanghai",        "unit": "C", "station": "ZHHH"},
    "chengdu":       {"lat": 30.5785, "lon": 103.9473, "tz": "Asia/Shanghai",        "unit": "C", "station": "ZUUU"},
    "chongqing":     {"lat": 29.7192, "lon": 106.6419, "tz": "Asia/Shanghai",        "unit": "C", "station": "ZUCK"},
    "shenzhen":      {"lat": 22.6329, "lon": 113.8108, "tz": "Asia/Shanghai",        "unit": "C", "station": "ZGSZ"},
    "lucknow":       {"lat": 26.7606, "lon":  80.8893, "tz": "Asia/Kolkata",         "unit": "C", "station": "VILK"},
}

# v6.6: London kanıtlanmış (gerçek 1/1 + paper 5/7)
# Buenos Aires + Cape Town secondary marketler — rekabet az, edge window saatlerce sürebiliyor
# NYC/Miami/Chicago/Paris/Madrid PAPER — istasyon düzeltmesi sonrası performans test edilecek
RELIABLE_CITIES = {"london", "buenos-aires", "cape-town"}
PAPER_CITIES = {"nyc", "miami", "chicago", "paris", "madrid"}


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
    for col_sql in [
        "ALTER TABLE alerts ADD COLUMN model_ecmwf REAL",
        "ALTER TABLE alerts ADD COLUMN model_gfs REAL",
        "ALTER TABLE alerts ADD COLUMN model_icon REAL",
        "ALTER TABLE alerts ADD COLUMN model_jma REAL",
        "ALTER TABLE alerts ADD COLUMN model_spread REAL",
        "ALTER TABLE alerts ADD COLUMN station TEXT",          # v6.6: hangi istasyona göre
        "ALTER TABLE alerts ADD COLUMN bias_offset REAL",      # v6.6: o sehir için bias düzeltmesi
        "ALTER TABLE alerts ADD COLUMN is_paper INTEGER DEFAULT 0",  # v6.6: paper mode mu
    ]:
        try:
            c.execute(col_sql)
        except sqlite3.OperationalError:
            pass
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
                 model_ecmwf, model_gfs, model_icon, model_jma, model_spread,
                 station, bias_offset, is_paper)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            datetime.utcnow().isoformat(),
            row["city"], row["target_date"], row["forecast"], row["unit"],
            row["bucket_title"], row["bucket_low"], row["bucket_high"],
            row["price"], row["event_slug"], row["market_id"],
            row.get("model_ecmwf"), row.get("model_gfs"),
            row.get("model_icon"), row.get("model_jma"),
            row.get("model_spread"),
            row.get("station"), row.get("bias_offset", 0.0),
            1 if row.get("is_paper") else 0,
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
        UPDATE alerts SET actual_temp=?, actual_bucket=?, won=?
        WHERE id=?
    """, (actual_temp, actual_bucket, won, alert_id))
    conn.commit()
    conn.close()


def db_get_stats():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        SELECT city, target_date, forecast, unit, bucket_title, price,
               won, actual_bucket, model_spread, is_paper
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


def db_compute_city_bias(city, unit, min_samples=10):
    """
    v6.6: Geçmiş alertlerden bu şehir için sistematik bias hesapla.
    Eğer bot ortalama 1.5°F düşük tahmin ediyorsa, gelecek tahminlere +1.5°F ekleyebiliriz.
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        SELECT forecast, actual_temp FROM alerts
        WHERE city=? AND unit=? AND actual_temp IS NOT NULL
    """, (city, unit))
    rows = c.fetchall()
    conn.close()
    if len(rows) < min_samples:
        return 0.0
    diffs = [actual - forecast for forecast, actual in rows if actual is not None]
    if not diffs:
        return 0.0
    # Median bias — outlier'lara dirençli
    diffs.sort()
    return round(diffs[len(diffs) // 2], 2)


# -------------------- Telegram --------------------

def send_telegram(msg):
    if not TELEGRAM_TOKEN:
        return
    try:
        url = "https://api.telegram.org/bot" + TELEGRAM_TOKEN + "/sendMessage"
        requests.post(url, data={"chat_id": CHAT_ID, "text": msg}, timeout=10)
    except Exception as e:
        print("Telegram hatasi:", e)


def send_telegram_document(file_path, caption=""):
    if not TELEGRAM_TOKEN or not os.path.exists(file_path):
        return False
    try:
        url = "https://api.telegram.org/bot" + TELEGRAM_TOKEN + "/sendDocument"
        with open(file_path, "rb") as f:
            r = requests.post(url, data={"chat_id": CHAT_ID, "caption": caption},
                              files={"document": f}, timeout=60)
            return r.status_code == 200
    except Exception as e:
        print("Document hatasi:", e)
        return False


def build_stats_message():
    rows, (total, won, lost), pnl = db_get_stats()
    if total == 0:
        return "Henuz uyari yok."
    pending = total - won - lost
    msg = "== BOT STATS (v6.6) ==\n"
    msg += "Toplam: " + str(total) + " | Kazanan: " + str(won)
    msg += " | Kaybeden: " + str(lost) + " | Bekleyen: " + str(pending) + "\n"
    if won + lost > 0:
        win_rate = won / (won + lost) * 100
        msg += "Tutma orani: %" + str(round(win_rate, 1)) + "\n"
        msg += "Hipotetik PnL ($3/bahis): $" + str(round(pnl, 2)) + "\n"

    # v6.6: Sehir bazli bias raporu
    msg += "\n-- Sehir bias (forecast - actual) --\n"
    for city in sorted(RELIABLE_CITIES | PAPER_CITIES):
        if city in CITIES:
            bias = db_compute_city_bias(city, CITIES[city]["unit"])
            unit = CITIES[city]["unit"]
            if bias != 0.0:
                sign = "+" if bias > 0 else ""
                msg += city[:10] + ": " + sign + str(bias) + unit + "\n"

    msg += "\n-- Son 20 uyari --\n"
    for r in rows:
        city, tdate, forecast, unit, bucket, price, w, actual, spread, is_paper = r
        if w == 1:
            mark = "+"
        elif w == 0:
            mark = "-"
        else:
            mark = "?"
        prefix = "[P]" if is_paper else "   "
        line = mark + prefix + " " + city[:8] + " " + tdate[5:] + " "
        line += str(forecast) + unit + " -> " + bucket + " %" + str(price)
        if spread is not None:
            line += " (s:" + str(round(spread, 1)) + ")"
        if actual and w is not None:
            line += " ger:" + actual
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
                    send_telegram("Bot v6.6 ayakta. DB: " + DB_PATH + "\nTarama: " + str(SCAN_INTERVAL_SECONDS) + "s")
                elif text == "/check":
                    send_telegram("Sonuc kontrolu baslatildi...")
                    count = check_results()
                    send_telegram("Kontrol bitti. " + str(count) + " uyari guncellendi.")
                elif text == "/dump":
                    if os.path.exists(DB_PATH):
                        size_kb = round(os.path.getsize(DB_PATH) / 1024, 1)
                        send_telegram("DB gonderiliyor (" + str(size_kb) + " KB)...")
                        send_telegram_document(DB_PATH, caption="bot.db dump v6.6")
                    else:
                        send_telegram("DB bulunamadi.")
                elif text.startswith("/verify"):
                    # v6.6: /verify london 2026-05-20
                    # Polymarket'te marketin var mı, fiyat ne, kontrol et
                    parts = text.split()
                    if len(parts) == 3:
                        _, city, dstr = parts
                        try:
                            d = date.fromisoformat(dstr)
                            slug = make_slug(city, d)
                            markets = get_polymarket_markets(slug)
                            if not markets:
                                send_telegram("Market YOK veya arsivlenmis: " + slug)
                            else:
                                lines = ["Market BULUNDU: " + slug, ""]
                                for m in markets[:11]:
                                    title = m.get("groupItemTitle") or "?"
                                    price = extract_yes_price(m)
                                    lines.append(title + ": %" + str(price))
                                send_telegram("\n".join(lines))
                        except Exception as e:
                            send_telegram("Hata: " + str(e))
                    else:
                        send_telegram("Kullanim: /verify <sehir> <YYYY-MM-DD>")
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
    """
    v6.6: SLUG EŞLEŞME KONTROLÜ EKLENDİ.
    Polymarket API bazen yanlış event dönüyor (slug filtresi göz ardı ediliyor).
    Dönen event'ın slug'ı bizim sorduğumuzla eşleşmiyorsa boş döndür.
    """
    try:
        url = "https://gamma-api.polymarket.com/events?slug=" + slug
        r = requests.get(url, timeout=15)
        data = r.json()
        if not data:
            return []
        # v6.6: KRITIK — slug doğrulama
        returned_slug = data[0].get("slug", "")
        if returned_slug != slug:
            # API yanlış event döndü, sessizce boş döndür
            # (debug için isteğe bağlı log)
            # print(f"Slug mismatch: istenen={slug}, gelen={returned_slug}")
            return []
        # v6.6: arsivlenmis market kontrolü
        if data[0].get("archived") or data[0].get("closed"):
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
            return None, None, None, None
        temps = list(model_temps.values())
        avg = round(sum(temps) / len(temps), 1)
        spread = round(max(temps) - min(temps), 2) if len(temps) > 1 else 0.0
        return avg, model_temps, spread, c["unit"]
    except Exception as e:
        print("Ensemble hatasi", city_key, e)
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
            print("check_results hatasi:", city, e)
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

    # v6.6: Bias correction — eğer bu şehir için tutarlı sapma varsa düzelt
    bias = db_compute_city_bias(city, unit, min_samples=10)
    adjusted_temp = round(avg_temp + bias, 1)

    # Spread filtreleri
    max_spread = MAX_MODEL_SPREAD_F if unit == "F" else MAX_MODEL_SPREAD_C
    min_spread = MIN_MODEL_SPREAD_F if unit == "F" else MIN_MODEL_SPREAD_C
    low_confidence = spread > max_spread
    too_consensus = spread < min_spread  # v6.6: yeni filtre

    # Bucket'ı bul (düzeltilmiş sıcaklık üzerinden)
    matched = None
    for m in markets:
        title = m.get("groupItemTitle") or ""
        bucket = parse_bucket(title, unit)
        if not bucket:
            continue
        if bucket_contains(bucket, adjusted_temp):
            p = extract_yes_price(m)
            if p is None:
                continue
            matched = {"title": title, "low": bucket[0], "high": bucket[1],
                       "price": p, "market_id": str(m.get("id", ""))}
            break
    if not matched:
        return None

    in_reliable = city in RELIABLE_CITIES
    in_paper = city in PAPER_CITIES

    if not (in_reliable or in_paper):
        return None  # Hiçbir listede yoksa hiç alert üretme

    price_ok = 8 <= matched["price"] <= 30
    spread_ok = not low_confidence and not too_consensus

    # Reliable şehir tüm filtreleri geçerse opportunity = True
    # Paper şehir geçse bile opportunity = False (sadece istatistik için)
    opportunity = in_reliable and price_ok and spread_ok

    # Paper alerti — fiyat aralığı dışındaki PAPER şehirleri de kaydet ki istatistik birikir
    should_save_paper = in_paper and price_ok and spread_ok

    if not opportunity and not should_save_paper:
        return None

    return {
        "city": city,
        "target_date": target_date.isoformat(),
        "forecast": adjusted_temp,         # bias-düzeltilmiş
        "raw_forecast": avg_temp,           # orijinal ensemble
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
        "model_jma": None,
        "model_spread": spread,
        "station": CITIES[city].get("station"),
        "bias_offset": bias,
        "is_paper": not opportunity,
        "low_confidence": low_confidence,
        "too_consensus": too_consensus,
        "opportunity": opportunity,
    }


# -------------------- Ana döngü --------------------

def main():
    db_init()
    listener_thread = threading.Thread(target=telegram_listener, daemon=True)
    listener_thread.start()

    send_telegram("WeatherBot v6.6 basladi. /stats /ping /check /dump /verify")
    print("Bot v6.6 basladi. DB:", DB_PATH)
    print("Tarama: her", SCAN_INTERVAL_SECONDS, "saniyede")
    print("Reliable:", RELIABLE_CITIES)
    print("Paper:", PAPER_CITIES)

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
            # v6.6: 2 günlük pencere (eski 7, çok dar veya çok geniş tartismaliydi)
            # 0=bugün, 1=yarın, 2=ertesi gün → 3 tarih
            # Uzak tarihler edge'siz, bugün-yarin asil firsat penceresi
            targets = [today + timedelta(days=i) for i in range(0, 3)]

            # Sadece reliable + paper şehirleri tara, hepsini değil (rate limit)
            scan_cities = list(RELIABLE_CITIES | PAPER_CITIES)

            new_real = []
            new_paper = []
            scanned = 0
            market_found = 0
            for target_date in targets:
                for city in scan_cities:
                    if city not in CITIES:
                        continue
                    try:
                        result = analyze(city, target_date)
                        scanned += 1
                        if result:
                            market_found += 1
                            saved = db_save_alert(result)
                            if saved:
                                if result["opportunity"]:
                                    new_real.append(result)
                                elif result["is_paper"]:
                                    new_paper.append(result)
                    except Exception as e:
                        print("Sehir hatasi:", city, e)
                    time.sleep(0.4)

            if new_real:
                msg = "*** GERCEK FIRSAT ***\n\n"
                for r in new_real:
                    msg += _format_alert(r)
                msg += "/stats ile ozet."
                send_telegram(msg)

            if new_paper:
                msg = "[PAPER] yeni paper alert\n\n"
                for r in new_paper:
                    msg += _format_alert(r)
                send_telegram(msg)

            if not new_real and not new_paper:
                print("Tarama: " + str(scanned) + " sehir, " + str(market_found) + " market, yeni firsat yok.")

            loop_count += 1
        except Exception as e:
            print("Ana dongu hatasi:", e)
        print("Sonraki tarama " + str(SCAN_INTERVAL_SECONDS) + " saniye sonra...")
        time.sleep(SCAN_INTERVAL_SECONDS)


def _format_alert(r):
    msg = r["city"].upper() + " (" + r["target_date"] + ")\n"
    msg += "Ensemble: " + str(r["raw_forecast"]) + r["unit"]
    if r.get("bias_offset") and r["bias_offset"] != 0.0:
        sign = "+" if r["bias_offset"] > 0 else ""
        msg += " (bias " + sign + str(r["bias_offset"]) + " -> " + str(r["forecast"]) + r["unit"] + ")"
    msg += "\n"
    mt = []
    if r.get("model_ecmwf") is not None:
        mt.append("ECMWF:" + str(r["model_ecmwf"]))
    if r.get("model_gfs") is not None:
        mt.append("GFS:" + str(r["model_gfs"]))
    if r.get("model_icon") is not None:
        mt.append("ICON:" + str(r["model_icon"]))
    msg += "Modeller: " + " ".join(mt) + "\n"
    msg += "Spread: " + str(r["model_spread"]) + r["unit"] + "\n"
    msg += "Bucket: " + r["bucket_title"] + " -> %" + str(r["price"]) + "\n"
    if r.get("station"):
        msg += "Istasyon: " + r["station"] + "\n"
    msg += "\n"
    return msg


if __name__ == "__main__":
    main()
