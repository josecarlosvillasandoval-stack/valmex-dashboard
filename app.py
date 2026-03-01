import os
import time
import requests
import pandas as pd
import yfinance as yf
from datetime import date, timedelta, datetime
from flask import Flask, send_file, request, jsonify, redirect, url_for, session, send_from_directory

BASE = os.path.dirname(os.path.abspath(__file__))

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "valmex-secret-2024")

USERS = {
    "jvilla": {"password": "valmex",   "nombre": "José Carlos Villa", "iniciales": "JV", "rol": "admin"},
    "admin":  {"password": "admin123", "nombre": "Administrador",      "iniciales": "AD", "rol": "admin"},
}

PERFILES = {
    "0": {"VXGUBCP": 5.00,  "VXDEUDA": 90.00, "VXUDIMP": 5.00},
    "1": {"VXGUBCP": 25.00, "VXDEUDA": 12.00, "VXUDIMP": 7.00,  "VXGUBLP": 52.00, "VXTBILL": 4.00},
    "2": {"VXGUBCP": 26.93, "VXDEUDA": 18.00, "VXUDIMP": 5.83,  "VXGUBLP": 26.89, "VXTBILL": 2.35, "VALMX28": 17.00, "VALMX20": 3.00},
    "3": {"VXGUBCP": 20.40, "VXDEUDA": 5.76,  "VXUDIMP": 6.65,  "VXGUBLP": 25.11, "VXTBILL": 2.08, "VALMX28": 34.00, "VALMX20": 6.00},
    "4": {"VXGUBCP": 20.70, "VXDEUDA": 4.08,  "VXUDIMP": 5.37,  "VXGUBLP": 7.61,  "VXTBILL": 2.24, "VALMX28": 51.00, "VALMX20": 9.00},
}

FONDOS_DEUDA_MXN = {"VXREPO1", "VXGUBCP", "VXUDIMP", "VXDEUDA", "VXGUBLP", "VLMXETF"}
FONDOS_DEUDA_USD = {"VXTBILL", "VXCOBER", "VLMXDME"}
FONDOS_CRED_GLOBAL = {"VLMXETF"}
SP_RATING_MXN = "BBB"
SP_RATING_USD = "AA+"

FONDOS_DEUDA     = FONDOS_DEUDA_MXN | FONDOS_DEUDA_USD
FONDOS_RV        = {"VALMXA", "VALMX20", "VALMX28", "VALMXVL", "VALMXES", "VLMXTEC", "VLMXESG", "VALMXHC", "VXINFRA"}
FONDOS_CICLO     = {"VLMXJUB", "VLMXP24", "VLMXP31", "VLMXP38", "VLMXP45", "VLMXP52", "VLMXP59"}

CREDIT_SCALE = ["AAA", "AA+", "AA", "AA-", "A+", "A", "A-", "BBB+", "BBB", "BBB-", "BB+", "BB", "BB-", "B+", "B", "B-", "<B", "NR"]
CREDIT_SCORE = {r: i for i, r in enumerate(CREDIT_SCALE)}

MX_LOCAL_TO_GLOBAL = {
    "AAA": "BBB", "AA": "BBB-", "A": "BB+", "BBB": "BB",
    "BB": "BB-", "B": "B+", "<B": "B", "NR": "NR",
}

def weighted_credit_rating(cred_acc: dict, local_to_global: bool = False) -> str:
    if local_to_global:
        converted = {}
        for rating, weight in cred_acc.items():
            global_rating = MX_LOCAL_TO_GLOBAL.get(rating, rating)
            converted[global_rating] = converted.get(global_rating, 0) + weight
        cred_acc = converted
    total_weight = sum(cred_acc.values())
    if total_weight <= 0:
        return "—"
    score = sum(CREDIT_SCORE.get(r, len(CREDIT_SCALE)-1) * v for r, v in cred_acc.items()) / total_weight
    idx = round(score)
    idx = max(0, min(idx, len(CREDIT_SCALE) - 1))
    return CREDIT_SCALE[idx]

ISIN_MAP = {
  "VXREPO1": {"A":"MXP800461008","B0CF":"MX51VA2J00C5","B0CO":"MX51VA2J0058","B0FI":"MX51VA2J0074","B0NC":"MX51VA2J0041","B1CF":"MX51VA2J00D3","B1CO":"MX51VA2J0082","B1FI":"MX51VA2J00F8","B1NC":"MX51VA2J0066","B2FI":"MX51VA2J0090"},
  "VXGUBCP": {"A":"MXP800501001","B0CF":"MX51VA2L00B3","B0CO":"MX51VA2L0054","B0FI":"MX51VA2L0039","B0NC":"MX51VA2L0047","B1CF":"MX51VA2L00C1","B1CO":"MX51VA2L0088","B1FI":"MX51VA2L0062","B1NC":"MX51VA2L0070","B2CF":"MX51VA2L00D9","B2FI":"MX51VA2L00E7","B2NC":"MX51VA2L0096"},
  "VXUDIMP": {"A":"MX51VA2S0008","B0CF":"MX51VA2S00D4","B0CO":"MX51VA2S0065","B0FI":"MX51VA2S0040","B0NC":"MX51VA2S0057","B1CO":"MX51VA2S0099","B1FI":"MX51VA2S0073","B1NC":"MX51VA2S0081","B2CO":"MX51VA2S00C6","B2FI":"MX51VA2S00A0","B2NC":"MX51VA2S00B8"},
  "VXDEUDA": {"A":"MXP800521009","B0CF":"MX51VA2M0046","B0CO":"MX51VA2M00D7","B0FI":"MX51VA2M0061","B0NC":"MX51VA2M0079","B1CF":"MX51VA2M0095","B1CO":"MX51VA2M00E5","B1FI":"MX51VA2M00A3","B1NC":"MX51VA2M00B1","B2FI":"MX51VA2M00C9","B2NC":"MX51VA2M00H8"},
  "VXGUBLP": {"A":"MX51VA2R0009","B0CF":"MX51VA2R00C8","B0CO":"MX51VA2R00F1","B0FI":"MX51VA2R0041","B0NC":"MX51VA2R0058","B1CF":"MX51VA2R00D6","B1CO":"MX51VA2R0082","B1FI":"MX51VA2R0066","B1NC":"MX51VA2R0074","B2CO":"MX51VA2R00B0","B2FI":"MX51VA2R0090","B2NC":"MX51VA2R00A2"},
  "VXTBILL": {"A":"MX51VA1F0004","B0CF":"MX51VA1F0087","B0CO":"MX51VA1F0020","B0FI":"MX51VA1F0012","B0NC":"MX51VA1F0053"},
  "VXCOBER": {"A":"MXP800621007","B0FI":"MX51VA2N0037","B0NC":"MX51VA2N0045","B1CF":"MX51VA2N00D5","B1CO":"MX51VA2N0086","B1FI":"MX51VA2N0060","B1NC":"MX52FM080076","B2FI":"MX51VA2N0094"},
  "VLMXETF": {"A":"MX52VL060004","B0FI":"MX52VL060038","B1CO":"MX52VL060061","B1FI":"MX52VL060079"},
  "VLMXDME": {"A":"MX52VL0D0002","B0CF":"MX52VL0D0010","B0CO":"MX52VL0D0028","B0FI":"MX52VL0D0036","B0NC":"MX52VL0D0044","B1CF":"MX52VL0D0051","B1FI":"MX52VL0D00B0","B2FI":"MX52VL0D0093"},
  "VALMXA":  {"A":"MX52VA2W0000","B0":"MX52VA2W0018","B1":"MX52VA2W0026","B2":"MX52VA2W0034"},
  "VALMX20": {"A":"MXP800541007","B0":"MX52VA2O0026","B1":"MX52VA2O0000"},
  "VALMX28": {"A":"MX52VA130008","B0CF":"MX52VA130040","B0CO":"MX52VA130032","B0FI":"MX52VA130065","B0NC":"MX52VA130099","B1CO":"MX52VA1300B0","B1FI":"MX52VA130016","B1NC":"MX52VA1300C8"},
  "VALMXVL": {"A":"MX52VA140007","B0":"MX52VA140015","B1":"MX52VA140023","B2":"MX52VA140031","B3":"MX52VA140049"},
  "VALMXES": {"A":"MX52VA190002","B0":"MX52VA190010","B1":"MX52VA190028","B2":"MX52VA190036","B3":"MX52VA190044"},
  "VLMXTEC": {"A":"MX52VL080002","B0CF":"MX52VL080010","B0CO":"MX52VL080028","B0FI":"MX52VL080036","B0NC":"MX52VL080044","B1CF":"MX52VL080051","B1CO":"MX52VL080077","B1FI":"MX52VL080069","B1NC":"MX52VL080085","B2FI":"MX52VL0800B7"},
  "VLMXESG": {"A":"MX52VL0B0004","B0CF":"MX52VL0B0038","B0CO":"MX52VL0B00D0","B0FI":"MX52VL0B0012","B1CF":"MX52VL0B0079","B1CO":"MX52VL0B0053","B1FI":"MX52VL0B0046","B1NC":"MX52VL0B0061","B2FI":"MX52VL0B0087"},
  "VALMXHC": {"A":"MX52VA1L0004","B0CF":"MX52VA1L0046","B0CO":"MX52VA1L0020","B0FI":"MX52VA1L0012","B0NC":"MX52VA1L0038","B1CF":"MX52VA1L0087","B1CO":"MX52VA1L0061","B1FI":"MX52VA1L00D0","B1NC":"MX52VA1L0079","B2FI":"MX52VA1L0095"},
  "VXINFRA": {"A":"MX52VL0E0001","B0CO":"MX52VL0E0019","B0FI":"MX52VL0E0027","B1FI":"MX52VL0E0050","B2FI":"MX52VL0E0084"},
  "VLMXJUB": {"A":"MX52VL070003","B0CF":"MX52VL070011","B0NC":"MX52VL070045","B1CF":"MX52VL070052","B1FI":"MX52VL070078","B1NC":"MX52VL070086","B2NC":"MX52VL0700C7","B3NC":"MX52VL0700D5"},
  "VLMXP24": {"A":"MX52VL010009","B0CF":"MX52VL0100A4","B0NC":"MX52VL010025","B1FI":"MX52VL010041","B1NC":"MX52VL010058","B2NC":"MX52VL010082","B3NC":"MX52VL0100D8"},
  "VLMXP31": {"A":"MX52VL030007","B0CF":"MX52VL0300A0","B0FI":"MX52VL030015","B0NC":"MX52VL030023","B1CF":"MX52VL030049","B1FI":"MX52VL030049","B1NC":"MX52VL030056","B2NC":"MX52VL030080","B3NC":"MX52VL0300D4"},
  "VLMXP38": {"A":"MX52VL000000","B0CF":"MX52VL0000A6","B0FI":"MX52VL000018","B0NC":"MX52VL000026","B1CF":"MX52VL0000B4","B1FI":"MX52VL000042","B1NC":"MX52VL000059","B2NC":"MX52VL000083","B3NC":"MX52VL0000D0"},
  "VLMXP45": {"A":"MX52VL040014","B0CF":"MX52VL040089","B0FI":"MX52VL040022","B0NC":"MX52VL040048","B1CF":"MX52VL040097","B1CO":"MX52VL0400C4","B1FI":"MX52VL040071","B1NC":"MX52VL0400B6","B2NC":"MX52VL040030","B3NC":"MX52VL0400D2"},
  "VLMXP52": {"A":"MX52VL050005","B0FI":"MX52VL050013","B0NC":"MX52VL050021","B1FI":"MX52VL050047","B1NC":"MX52VL050096","B2NC":"MX52VL050088","B3NC":"MX52VL0500B3"},
  "VLMXP59": {"A":"MX52VL0C0003","B0NC":"MX52VL0C0037","B1FI":"MX52VL0C0086","B1NC":"MX52VL0C0052","B2NC":"MX52VL0C0078","B3NC":"MX52VL0C0094"},
}

TIPO_KEY = {
    "Serie A":                             "A",
    "Persona Física - B1FI/B1":           "PF",
    "Persona Física con Fee - B0FI/B0":    "PF_fee",
    "Plan Personal de Retiro - B1NC/B1CF": "PPR",
    "Persona Moral - B1CO":                "PM",
    "Persona Moral con Fee - B0CO":        "PM_fee",
}

SERIE_MAP = {
    "VXREPO1": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":"B1CO","PM_fee":"B0CO"},
    "VXGUBCP": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":"B1CO","PM_fee":"B0CO"},
    "VXUDIMP": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1NC","PM":"B1CO","PM_fee":"B0CO"},
    "VXDEUDA": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":"B1CO","PM_fee":"B0CO"},
    "VXGUBLP": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":"B1CO","PM_fee":"B0CO"},
    "VXTBILL": {"A":"A","PF":"B0FI","PF_fee":"B0FI","PPR":"B0CF","PM":"B0CO","PM_fee":"B0CO"},
    "VXCOBER": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":"B1CO","PM_fee":"B0CO"},
    "VLMXETF": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":"B1CO","PM_fee":"B0CO"},
    "VLMXDME": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":"B1CO","PM_fee":"B0CO"},
    "VALMXA":  {"A":"A","PF":"B1",  "PF_fee":"B0",  "PPR":"B1",  "PM":"B1",  "PM_fee":"B0"},
    "VALMX20": {"A":"A","PF":"B1",  "PF_fee":"B0",  "PPR":"B1",  "PM":"B1",  "PM_fee":"B0"},
    "VALMX28": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1NC","PM":"B1CO","PM_fee":"B0CO"},
    "VALMXVL": {"A":"A","PF":"B1",  "PF_fee":"B0",  "PPR":"B1",  "PM":"B1",  "PM_fee":"B0"},
    "VALMXES": {"A":"A","PF":"B1",  "PF_fee":"B0",  "PPR":"B1",  "PM":"B1",  "PM_fee":"B0"},
    "VLMXTEC": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":"B1CO","PM_fee":"B0CO"},
    "VLMXESG": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":"B1CO","PM_fee":"B0CO"},
    "VALMXHC": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":"B1CO","PM_fee":"B0CO"},
    "VXINFRA": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1FI","PM":None,  "PM_fee":"B0CO"},
    "VLMXJUB": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":None,  "PM_fee":None},
    "VLMXP24": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1NC","PM":None,  "PM_fee":None},
    "VLMXP31": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":None,  "PM_fee":None},
    "VLMXP38": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":None,  "PM_fee":None},
    "VLMXP45": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":None,  "PM_fee":None},
    "VLMXP52": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1NC","PM":None,  "PM_fee":None},
    "VLMXP59": {"A":"A","PF":"B1FI","PF_fee":"B0FI","PPR":"B1NC","PM":None,  "PM_fee":None},
}

_ms_cache = {}
MS_URL    = "https://api.morningstar.com/v2/service/mf/hlk0d0zmiy1b898b/universeid/txcm88fa8x3vxapp"
MS_ACCESS = "hwg0cty5re7araij32k035091f43wxd0"


def load_ms_universe():
    if _ms_cache:
        return _ms_cache
    try:
        resp = requests.get(MS_URL, params={"accesscode": MS_ACCESS, "format": "JSON"}, timeout=25)
        resp.raise_for_status()
        for fund in resp.json().get("data", []):
            api    = fund.get("api", {})
            ticker = api.get("FSCBI-Ticker", "").strip()
            if ticker:
                _ms_cache[ticker] = api
        print(f"[MS] Universo cargado: {len(_ms_cache)} fondos")
    except Exception as e:
        print(f"[MS ERROR] {e}")
    return _ms_cache

# ─────────────────────────────────────────────────────────────────────────────
# ACCIONES & ETFs — Yahoo Finance con cookie/crumb
# ─────────────────────────────────────────────────────────────────────────────
_accion_cache: dict = {}
_accion_cache_ts: dict = {}
ACCION_CACHE_TTL = 3600

GEO_TRANSLATE_YF = {
    "united states": "Estados Unidos", "mexico": "México", "canada": "Canadá",
    "united kingdom": "Reino Unido", "germany": "Alemania", "france": "Francia",
    "japan": "Japón", "china": "China", "brazil": "Brasil", "india": "India",
    "south korea": "Corea del Sur", "taiwan": "Taiwán", "australia": "Australia",
    "netherlands": "Países Bajos", "switzerland": "Suiza", "spain": "España",
    "italy": "Italia", "hong kong": "Hong Kong", "singapore": "Singapur",
    "ireland": "Irlanda", "denmark": "Dinamarca", "sweden": "Suecia",
    "norway": "Noruega", "finland": "Finlandia", "belgium": "Bélgica",
    "austria": "Austria", "portugal": "Portugal", "new zealand": "Nueva Zelanda",
    "brazil": "Brasil", "argentina": "Argentina", "chile": "Chile",
    "colombia": "Colombia", "peru": "Perú",
}

SEC_TRANSLATE_YF = {
    "technology": "Tecnología", "financial services": "Financiero",
    "healthcare": "Salud", "consumer cyclical": "Consumo Discrecional",
    "industrials": "Industriales", "communication services": "Comunicaciones",
    "consumer defensive": "Consumo Básico", "energy": "Energía",
    "basic materials": "Materiales", "real estate": "Bienes Raíces",
    "utilities": "Utilidades",
}



# ── Cookie cache para Yahoo Finance ──
_yf_cookie_cache: dict = {}   # {"cookie": str, "crumb": str, "ts": float}
_YF_COOKIE_TTL = 3600  # renovar cookie cada hora

def _ensure_yf_cookie(session: requests.Session) -> bool:
    """
    Obtiene y cachea cookie + crumb de Yahoo Finance.
    Sin esto, Yahoo bloquea requests desde servidores cloud.
    Retorna True si tuvo éxito.
    """
    global _yf_cookie_cache
    now = time.time()

    # Si tenemos cookie válida, aplicarla a la sesión y listo
    if _yf_cookie_cache.get("cookie") and (now - _yf_cookie_cache.get("ts", 0)) < _YF_COOKIE_TTL:
        session.cookies.set("B", _yf_cookie_cache["cookie"], domain=".yahoo.com")
        return True

    ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

    # Intento A: fc.yahoo.com (URL de consentimiento)
    for consent_url in ["https://fc.yahoo.com", "https://finance.yahoo.com"]:
        try:
            r1 = requests.get(
                consent_url,
                headers={"User-Agent": ua, "Accept": "text/html,application/xhtml+xml"},
                timeout=10, allow_redirects=True
            )
            cookie_val = r1.cookies.get("B") or ""
            if not cookie_val:
                # Buscar cualquier cookie útil
                for c in r1.cookies:
                    if c.name in ("B", "A1", "A1S"):
                        cookie_val = c.value
                        break
            if not cookie_val:
                continue

            # Paso 2: obtener crumb
            r2 = requests.get(
                "https://query2.finance.yahoo.com/v1/test/getcrumb",
                headers={"User-Agent": ua, "Cookie": f"B={cookie_val}"},
                timeout=10
            )
            crumb = r2.text.strip()

            if crumb and crumb != "" and "<" not in crumb:
                _yf_cookie_cache = {"cookie": cookie_val, "crumb": crumb, "ts": now}
                session.cookies.set("B", cookie_val, domain=".yahoo.com")
                print(f"[YF COOKIE] OK ({consent_url}) — crumb={crumb[:8]}...")
                return True
        except Exception as e:
            print(f"[YF COOKIE] {consent_url} falló: {e}")

    print("[YF COOKIE] No se pudo obtener cookie/crumb — se usará yfinance sin autenticación")
    return False


def get_accion_yf(ticker: str) -> dict | None:
    now = time.time()
    if ticker in _accion_cache and (now - _accion_cache_ts.get(ticker, 0)) < ACCION_CACHE_TTL:
        return _accion_cache[ticker]

    hist = None
    info = {}
    t    = None

    ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

    # ── Intento 1: sesión autenticada con cookie/crumb ──
    try:
        sess = requests.Session()
        sess.headers.update({
            "User-Agent": ua, "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": "https://finance.yahoo.com",
            "Referer": "https://finance.yahoo.com/",
        })
        _ensure_yf_cookie(sess)
        t    = yf.Ticker(ticker, session=sess)
        hist = t.history(period="3y", auto_adjust=True)
        if hist.empty:
            hist = t.history(period="1y", auto_adjust=True)
    except Exception as e:
        print(f"[YF] {ticker} intento-sesión falló: {e}")

    # ── Intento 2: yfinance nativo sin sesión ──
    if hist is None or hist.empty:
        try:
            t    = yf.Ticker(ticker)
            hist = t.history(period="3y", auto_adjust=True)
            if hist.empty:
                hist = t.history(period="1y", auto_adjust=True)
        except Exception as e:
            print(f"[YF] {ticker} intento-nativo falló: {e}")

    # ── Intento 3: yf.download (más estable en servidores cloud) ──
    if hist is None or hist.empty:
        try:
            hist = yf.download(ticker, period="1y", auto_adjust=True,
                               progress=False, threads=False)
            # yf.download retorna MultiIndex si solo es un ticker en algunas versiones
            if isinstance(hist.columns, pd.MultiIndex):
                hist.columns = hist.columns.get_level_values(0)
        except Exception as e:
            print(f"[YF] {ticker} intento-download falló: {e}")

    if hist is None or hist.empty:
        print(f"[YF] {ticker}: sin datos después de 3 intentos")
        return None

    # ── Obtener info (nombre, sector, país) ──
    if t is not None:
        try:
            info = t.info or {}
        except Exception:
            info = {}

    try:
        today  = datetime.now().date()
        prices = hist["Close"].dropna()
        if prices.empty:
            return None
        idx = prices.index

        def precio_en(d: date):
            ts = [i for i in idx if i.date() <= d]
            return float(prices[ts[-1]]) if ts else None

        p_hoy = precio_en(today)
        if p_hoy is None:
            return None

        precio_cierre = round(float(prices.iloc[-1]), 2)

        p_mtd = precio_en(date(today.year, today.month, 1))
        p_3m  = precio_en(today - timedelta(days=91))
        p_ytd = precio_en(date(today.year, 1, 1))
        p_1y  = precio_en(today - timedelta(days=365))
        p_2y  = precio_en(today - timedelta(days=730))
        p_3y  = precio_en(today - timedelta(days=1095))

        def rend_efectivo(p_ini):
            if p_ini and p_ini > 0:
                return round((p_hoy / p_ini - 1) * 100, 2)
            return None

        def rend_anual(p_ini, years):
            if p_ini and p_ini > 0:
                return round(((p_hoy / p_ini) ** (1 / years) - 1) * 100, 2)
            return None

        quote_type = info.get("quoteType", "").upper()
        tipo       = "ETF" if quote_type == "ETF" else "Acción"
        sector_en  = (info.get("sector") or "").strip().lower()
        sector     = SEC_TRANSLATE_YF.get(sector_en, info.get("sector") or "")
        pais_en    = (info.get("country") or "").strip().lower()
        pais       = GEO_TRANSLATE_YF.get(pais_en, info.get("country") or "México")
        moneda     = "MXN" if ticker.endswith(".MX") else "USD"
        nombre     = info.get("shortName") or info.get("longName") or ticker

        sectores_etf = {}
        geo_etf      = {}

        if quote_type == "ETF":
            try:
                holdings = t.funds_data if t else None
                if holdings and hasattr(holdings, "sector_weightings"):
                    for s, v in (holdings.sector_weightings or {}).items():
                        lbl = SEC_TRANSLATE_YF.get(s.lower(), s)
                        if v > 0:
                            sectores_etf[lbl] = round(v * 100, 2)
            except Exception:
                pass
            if not sectores_etf and sector:
                sectores_etf[sector] = 100.0
            if not geo_etf and pais:
                geo_etf[pais] = 100.0
        else:
            if sector:
                sectores_etf[sector] = 100.0
            if pais:
                geo_etf[pais] = 100.0

        result = {
            "ticker":        ticker,
            "nombre":        nombre,
            "tipo":          tipo,
            "sector":        sector,
            "pais":          pais,
            "moneda":        moneda,
            "precio_cierre": precio_cierre,
            "moneda_precio": moneda,
            "r1m":           rend_efectivo(p_mtd),
            "r3m":           rend_efectivo(p_3m),
            "ytd":           rend_efectivo(p_ytd),
            "r1y":           rend_anual(p_1y, 1),
            "r2y":           rend_anual(p_2y, 2),
            "r3y":           rend_anual(p_3y, 3),
            "sectores":      sectores_etf,
            "geo":           geo_etf,
        }

        _accion_cache[ticker]    = result
        _accion_cache_ts[ticker] = now
        print(f"[YF OK] {ticker}: {nombre} | p={precio_cierre:.2f} | tipo={tipo} | pais={pais}")
        return result

    except Exception as e:
        print(f"[YF ERROR] {ticker}: {e}")
        return None


def safe_float(val, default=0.0):
    try:    return float(val)
    except: return default


def resolve_serie(fondo, tipo_cliente):
    tipo_key    = TIPO_KEY.get(tipo_cliente, "PF")
    fondo_map   = SERIE_MAP.get(fondo, {})
    deseada     = fondo_map.get(tipo_key)
    disponibles = ISIN_MAP.get(fondo, {})
    if deseada and deseada in disponibles:
        return deseada
    for fb in ["B1FI", "B0FI", "B1CF", "B1NC", "B1CO", "B0CO", "B1", "B0", "A"]:
        if fb in disponibles:
            return fb
    return list(disponibles.keys())[0] if disponibles else "A"


def calcular_portafolio(fondos_pct: dict, tipo_cliente: str,
                        repo_mxn: dict = None, repo_usd: dict = None,
                        acciones: list = None) -> dict:
    universe = load_ms_universe()

    r1m = r3m = r6m = ytd = r1y = r2y = r3y = 0.0
    stock_t = bond_t = cash_t = 0.0
    geo_acc = {}; sec_acc = {}; supersec_acc = {}
    lista = []

    dur_mxn_num = ytm_mxn_num = bond_mxn_denom = 0.0
    dur_usd_num = ytm_usd_num = bond_usd_denom = 0.0
    cred_mxn = {}; cred_usd = {}
    bt_repo: dict = {}

    for fondo, pct in fondos_pct.items():
        if pct <= 0:
            continue
        serie  = resolve_serie(fondo, tipo_cliente)
        ticker = f"{fondo} {serie}"
        d      = universe.get(ticker, {})

        if not d:
            for s in ["B1FI", "B0FI", "B1CF", "B1NC", "B1CO", "B0CO", "B1", "B0", "A"]:
                t2 = f"{fondo} {s}"
                if t2 in universe:
                    d = universe[t2]; serie = s; break

        w = pct / 100.0

        r1m += safe_float(d.get("TTR-Return1Mth")) * w
        r3m += safe_float(d.get("TTR-Return3Mth")) * w
        r6m += safe_float(d.get("TTR-Return6Mth")) * w
        ytd += safe_float(d.get("TTR-ReturnYTD"))  * w
        r1y += safe_float(d.get("TTR-Return1Yr"))  * w
        r2y += safe_float(d.get("TTR-Return2Yr"))  * w
        r3y += safe_float(d.get("TTR-Return3Yr"))  * w

        stock = safe_float(d.get("AAB-StockNet"))
        bond  = safe_float(d.get("AAB-BondNet"))
        cash  = safe_float(d.get("AAB-CashNet"))

        is_usd       = fondo in FONDOS_DEUDA_USD
        is_deuda_mxn = fondo in FONDOS_DEUDA_MXN
        is_deuda     = fondo in FONDOS_DEUDA
        is_rv        = fondo in FONDOS_RV
        is_ciclo     = fondo in FONDOS_CICLO

        if is_rv or is_ciclo:
            stock_t += stock * w
        bond_t  += bond  * w
        cash_t  += cash  * w

        if (is_deuda or is_ciclo) and bond > 0:
            bond_w = (bond / 100.0) * w
            if bond_w > 0:
                dur_val = safe_float(d.get("PS-EffectiveDuration"))
                ytm_val = safe_float(d.get("PS-YieldToMaturity"))
                if is_usd:
                    dur_usd_num    += dur_val * w
                    ytm_usd_num    += ytm_val * w
                    bond_usd_denom += w
                else:
                    dur_mxn_num    += dur_val * w
                    ytm_mxn_num    += ytm_val * w
                    bond_mxn_denom += w

                if fondo in FONDOS_CRED_GLOBAL:
                    cred_mxn[SP_RATING_USD] = cred_mxn.get(SP_RATING_USD, 0) + 100 * w
                elif is_usd:
                    cred_usd[SP_RATING_USD] = cred_usd.get(SP_RATING_USD, 0) + 100 * w
                else:
                    cred_mxn[SP_RATING_MXN] = cred_mxn.get(SP_RATING_MXN, 0) + 100 * w

                supersector_map = {
                    "GBSR-SuperSectorCashandEquivalentsNet": "Reporto",
                    "GBSR-SuperSectorCorporateNet":          "Corporativo",
                    "GBSR-SuperSectorGovernmentNet":         "Gubernamental",
                    "GBSR-SuperSectorMunicipalNet":          "Municipal",
                    "GBSR-SuperSectorSecuritizedNet":        "Bursatilizado",
                    "GBSR-SuperSectorDerivativeNet":         "Derivados",
                }
                for ss_key, ss_lbl in supersector_map.items():
                    v = safe_float(d.get(ss_key))
                    if v > 0:
                        supersec_acc[ss_lbl] = supersec_acc.get(ss_lbl, 0) + v * w

        if (is_rv or is_ciclo) and stock > 0:
            geo_raw = d.get("RE-RegionalExposure", [])
            GEO_EXCLUDE = {"emerging market", "developing country", "emerging markets", "developed countries", "developed country"}
            if isinstance(geo_raw, list):
                for item in geo_raw:
                    region = item.get("Region", "")
                    val    = safe_float(item.get("Value", 0))
                    if region and val > 0 and region.lower() not in GEO_EXCLUDE:
                        geo_acc[region] = geo_acc.get(region, 0) + val * (stock * w / 100)

            sector_map = {
                "GR-TechnologyNet":           "Tecnología",
                "GR-FinancialServicesNet":    "Financiero",
                "GR-HealthcareNet":           "Salud",
                "GR-CommunicationServicesNet":"Comunicaciones",
                "GR-IndustrialsNet":          "Industriales",
                "GR-ConsumerCyclicalNet":     "Consumo Discrecional",
                "GR-ConsumerDefensiveNet":    "Consumo Básico",
                "GR-BasicMaterialsNet":       "Materiales",
                "GR-EnergyNet":               "Energía",
                "GR-RealEstateNet":           "Bienes Raíces",
                "GR-UtilitiesNet":            "Utilidades",
            }
            for key, nombre in sector_map.items():
                v = safe_float(d.get(key))
                if v > 0:
                    sec_acc[nombre] = sec_acc.get(nombre, 0) + v * (stock * w / 100)

        lista.append({
            "fondo": fondo, "serie": serie, "pct": round(pct, 2),
            "r1m": round(safe_float(d.get("TTR-Return1Mth")), 2),
            "r3m": round(safe_float(d.get("TTR-Return3Mth")), 2),
            "r1y": round(safe_float(d.get("TTR-Return1Yr")),  2),
            "r3y": round(safe_float(d.get("TTR-Return3Yr")),  2),
        })

    # ── Reporto directo ──
    for repo_cfg, es_usd, label_corto in [
        (repo_mxn, False, "MD MXP"),
        (repo_usd, True,  "MD USD"),
    ]:
        if not repo_cfg:
            continue
        pct  = float(repo_cfg.get("pct", 0))
        tasa = float(repo_cfg.get("tasa", 0))
        if pct <= 0:
            continue
        w = pct / 100.0
        rend = get_repo_rendimientos(tasa, es_usd)
        r1m += rend["r1m"] * w; r3m += rend["r3m"] * w
        r6m += rend["r6m"] * w; ytd += rend["ytd"] * w
        r1y += rend["r1y"] * w; r2y += rend["r2y"] * w; r3y += rend["r3y"] * w
        for pt in rend.get("backtesting", []):
            f = pt["fecha"]
            bt_repo[f] = bt_repo.get(f, 0.0) + pt["valor"] * w
        cash_t += 100.0 * w
        if es_usd:
            dur_usd_num += 0.0 * w; ytm_usd_num += tasa * w; bond_usd_denom += w
            cred_usd[SP_RATING_USD] = cred_usd.get(SP_RATING_USD, 0) + 100 * w
        else:
            dur_mxn_num += 0.0 * w; ytm_mxn_num += tasa * w; bond_mxn_denom += w
            cred_mxn[SP_RATING_MXN] = cred_mxn.get(SP_RATING_MXN, 0) + 100 * w
        supersec_acc["Reporto"] = supersec_acc.get("Reporto", 0) + 100 * w
        lista.append({"fondo": label_corto, "serie": "—", "pct": round(pct, 2),
                      "r1m": round(rend["r1m"], 2), "r3m": round(rend["r3m"], 2),
                      "r1y": round(rend["r1y"], 2), "r3y": round(rend["r3y"], 2)})

    # ── Acciones & ETFs (Yahoo Finance) ──
    for acc in (acciones or []):
        ticker = acc.get("ticker", "").upper()
        pct    = float(acc.get("pct", 0))
        if pct <= 0 or not ticker:
            continue
        w   = pct / 100.0
        yfd = get_accion_yf(ticker)
        if not yfd:
            continue

        r1m += (yfd.get("r1m") or 0) * w
        r3m += (yfd.get("r3m") or 0) * w
        ytd += (yfd.get("ytd") or 0) * w
        r1y += (yfd.get("r1y") or 0) * w
        r2y += (yfd.get("r2y") or 0) * w
        r3y += (yfd.get("r3y") or 0) * w
        stock_t += 100 * w

        lista.append({
            "fondo": ticker, "serie": yfd.get("tipo", "Acción"), "pct": round(pct, 2),
            "r1m": round(yfd.get("r1m") or 0, 2), "r3m": round(yfd.get("r3m") or 0, 2),
            "r1y": round(yfd.get("r1y") or 0, 2), "r3y": round(yfd.get("r3y") or 0, 2),
        })

        # Sectores
        if yfd.get("sectores"):
            for s, v in yfd["sectores"].items():
                sec_acc[s] = sec_acc.get(s, 0) + v * w
        elif yfd.get("sector"):
            sec_acc[yfd["sector"]] = sec_acc.get(yfd["sector"], 0) + 100 * w

        # Geografía
        if yfd.get("geo"):
            for g, v in yfd["geo"].items():
                geo_acc[g] = geo_acc.get(g, 0) + v * w
        elif yfd.get("pais"):
            geo_acc[yfd["pais"]] = geo_acc.get(yfd["pais"], 0) + 100 * w

    def filter_pct(d, min_pct=1.0, translate=None):
        t = sum(d.values()) or 1
        main  = []
        otros = 0.0
        for k, v in sorted(d.items(), key=lambda x: -x[1]):
            pct = v / t * 100
            label = (translate or {}).get(k.lower(), k)
            if pct >= min_pct:
                main.append((label, pct))
            else:
                otros += pct
        if otros > 0:
            main.append(("Otros", round(otros, 2)))
        return {"labels":[i[0] for i in main],"values":[round(i[1],2) for i in main]}

    GEO_TRANSLATE = {
        "united states":"Estados Unidos","canada":"Canadá","latin america":"América Latina",
        "united kingdom":"Reino Unido","eurozone":"Eurozona","europe - ex euro":"Europa ex-Euro",
        "europe - emerging":"Europa Emergente","africa":"África","middle east":"Medio Oriente",
        "japan":"Japón","australasia":"Australasia","asia - developed":"Asia Desarrollada",
        "asia - emerging":"Asia Emergente","greater asia":"Gran Asia","greater europe":"Gran Europa",
        "americas":"Américas","north america":"Norteamérica",
    }

    has_mxn = bond_mxn_denom > 0
    has_usd = bond_usd_denom > 0

    return {
        "ok": True,
        "rendimientos": {
            "mtd":round(r1m,2),"r3m":round(r3m,2),
            "ytd":round(ytd,2),"r1y":round(r1y,2),
            "r2y":round(r2y,2),"r3y":round(r3y,2),
        },
        "clase_activos": {
            "labels":["Deuda","Renta Variable","Reporto"],
            "values":[round(bond_t,2), round(stock_t,2), round(cash_t,2)],
        },
        "composicion": sorted(lista, key=lambda x: -x["pct"]),
        "geo":           filter_pct(geo_acc, translate=GEO_TRANSLATE),
        "sectores":      filter_pct(sec_acc),
        "supersectores": filter_pct(supersec_acc),
        "has_rv":        stock_t > 0,
        "bt_repo":       sorted(
            [{"fecha": f, "valor": round(v, 4)} for f, v in bt_repo.items()],
            key=lambda x: x["fecha"]
        ) if bt_repo else [],
        "deuda": {
            "has_mxn":  has_mxn,
            "dur_mxn":  round(dur_mxn_num / bond_mxn_denom, 2) if has_mxn else 0,
            "ytm_mxn":  round(ytm_mxn_num / bond_mxn_denom, 2) if has_mxn else 0,
            "cred_mxn": weighted_credit_rating(cred_mxn) if cred_mxn else "—",
            "has_usd":  has_usd,
            "dur_usd":  round(dur_usd_num / bond_usd_denom, 2) if has_usd else 0,
            "ytm_usd":  round(ytm_usd_num / bond_usd_denom, 2) if has_usd else 0,
            "cred_usd": weighted_credit_rating(cred_usd) if cred_usd else "—",
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# RUTAS
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        data = request.get_json(force=True)
        u    = data.get("usuario", "").strip().lower()
        p    = data.get("password", "").strip()
        user = USERS.get(u)
        if user and user["password"] == p:
            session["usuario"] = u
            return jsonify({"ok":True,"nombre":user["nombre"],"iniciales":user["iniciales"],"rol":user["rol"]})
        return jsonify({"ok": False}), 401
    return send_file(os.path.join(BASE, "login.html"))

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.route("/me")
def me():
    u = session.get("usuario")
    if not u or u not in USERS:
        return jsonify({"ok": False}), 401
    user = USERS[u]
    return jsonify({"ok":True,"nombre":user["nombre"],"iniciales":user["iniciales"],"rol":user["rol"]})

@app.route("/PC.pdf")
def pc_pdf():
    return send_from_directory(BASE, "PC.pdf")

@app.route("/VALMEX.png")
def valmex_logo():
    return send_from_directory(BASE, "VALMEX.png")

@app.route("/VALMEX2.png")
def valmex_logo2():
    return send_from_directory(BASE, "VALMEX2.png")

@app.route("/")
def index():
    if "usuario" not in session:
        return redirect(url_for("login"))
    return send_file(os.path.join(BASE, "valmex_dashboard.html"))


@app.route("/api/accion/validate", methods=["POST"])
def api_accion_validate():
    if "usuario" not in session:
        return jsonify({"ok": False, "error": "No autenticado"}), 401
    body   = request.get_json(force=True)
    ticker = (body.get("ticker") or "").strip().upper()
    if not ticker:
        return jsonify({"ok": False, "error": "Ticker vacío"}), 400
    if not ticker.endswith(".MX"):
        ticker = ticker + ".MX"
    data = get_accion_yf(ticker)
    if data is None:
        base = ticker.replace(".MX", "")
        return jsonify({"ok": False, "error": f"'{base}' no encontrado en SIC/BMV/BIVA. Verifica el ticker."}), 404
    return jsonify({"ok": True, "data": data})


@app.route("/api/propuesta", methods=["POST"])
def api_propuesta():
    if "usuario" not in session:
        return jsonify({"ok": False, "error": "No autenticado"}), 401

    body         = request.get_json(force=True)
    tipo_cliente = body.get("tipo_cliente", "Serie A")
    modo         = body.get("modo", "propuesta")

    if modo == "perfil":
        pid = str(body.get("perfil_id", "3"))
        fondos_pct = PERFILES.get(pid)
        if not fondos_pct:
            return jsonify({"ok": False, "error": f"Perfil {pid} no existe"}), 400
    else:
        raw = body.get("fondos", {})
        fondos_pct = {k: float(v) for k, v in raw.items() if float(v) > 0}
        repo_mxn = body.get("repo_mxn")
        repo_usd = body.get("repo_usd")
        acciones_raw = body.get("acciones", [])
        if not fondos_pct and not repo_mxn and not repo_usd and not acciones_raw:
            return jsonify({"ok": False, "error": "Sin fondos con % > 0"}), 400

    return jsonify(calcular_portafolio(fondos_pct, tipo_cliente,
                                        repo_mxn=body.get("repo_mxn"),
                                        repo_usd=body.get("repo_usd"),
                                        acciones=body.get("acciones", [])))


# ─────────────────────────────────────────────────────────────────────────────
# MACRO
# ─────────────────────────────────────────────────────────────────────────────
BANXICO_TOKEN = os.environ.get("BANXICO_TOKEN", "")
BANXICO_BASE  = "https://www.banxico.org.mx/SieAPIRest/service/v1/series"
FRED_API_KEY  = os.environ.get("FRED_API_KEY", "")
FRED_BASE     = "https://api.stlouisfed.org/fred/series/observations"

SERIE_TIIE28  = "SF43783"
SERIE_USDMXN  = "SF43718"
SERIE_CETES28 = "SF60633"
SERIE_FONDEO  = "SF43936"
SERIE_USD_REPO = "SOFR"

_macro_cache = {}; _macro_ts = 0
_hist_cache = {}; _hist_cache_ts = 0


def _banxico_serie_rango(serie_id, fecha_ini, fecha_fin):
    try:
        url  = f"{BANXICO_BASE}/{serie_id}/datos/{fecha_ini}/{fecha_fin}"
        hdrs = {"Bmx-Token": BANXICO_TOKEN, "Accept": "application/json"}
        r    = requests.get(url, headers=hdrs, timeout=15)
        r.raise_for_status()
        datos = r.json()["bmx"]["series"][0].get("datos", [])
        result = []
        for d in datos:
            try:
                result.append({"fecha": d["fecha"], "valor": float(d["dato"].replace(",", "."))})
            except Exception:
                pass
        return result
    except Exception as e:
        print(f"[BANXICO HIST ERROR] {serie_id}: {e}")
        return []


def _parse_fecha(s):
    try:
        if "/" in s:
            d, m, y = s.split("/")
            return date(int(y), int(m), int(d))
        return date.fromisoformat(s[:10])
    except Exception:
        return None


def _get_datos_hist(es_usd):
    global _hist_cache, _hist_cache_ts
    cache_key = "usd" if es_usd else "mxn"
    now = time.time()
    if cache_key in _hist_cache and (now - _hist_cache_ts) < 14400:
        return _hist_cache[cache_key]
    hoy = date.today(); ini = "2000-01-01"; fin = hoy.isoformat()
    if es_usd:
        datos = []
        for serie in [SERIE_USD_REPO, "DFF"]:
            try:
                params = {"series_id": serie, "observation_start": ini, "observation_end": fin,
                          "api_key": FRED_API_KEY, "file_type": "json"}
                r = requests.get(FRED_BASE, params=params, timeout=15)
                r.raise_for_status()
                obs = [o for o in r.json().get("observations", []) if o["value"] != "."]
                datos = [{"fecha": _parse_fecha(o["date"]), "valor": float(o["value"])} for o in obs]
                if len(datos) > 100:
                    print(f"[FRED] {serie}: {len(datos)} registros OK")
                    break
            except Exception as e:
                print(f"[FRED {serie} ERROR] {e}")
    else:
        raw   = _banxico_serie_rango(SERIE_FONDEO, ini, fin)
        datos = [{"fecha": _parse_fecha(d["fecha"]), "valor": d["valor"]} for d in raw if _parse_fecha(d["fecha"])]
    datos = sorted([d for d in datos if d["fecha"] is not None], key=lambda x: x["fecha"])
    _hist_cache[cache_key] = datos; _hist_cache_ts = now
    print(f"[HIST {'USD' if es_usd else 'MXN'}] {len(datos)} registros desde {datos[0]['fecha'] if datos else 'N/A'}")
    return datos


def get_repo_rendimientos(tasa_neta, es_usd):
    datos = _get_datos_hist(es_usd)
    if not datos:
        anual = tasa_neta
        return {"r1m":round(anual/12,2),"r3m":round(anual/4,2),"r6m":round(anual/2,2),
                "ytd":round(anual/12,2),"r1y":round(anual,2),"r2y":round(anual,2),"r3y":round(anual,2),"backtesting":[]}
    hoy = date.today(); tasa_ref_hoy = datos[-1]["valor"]; spread = tasa_ref_hoy - tasa_neta
    def componer_acum(desde):
        acum = 1.0; ultimo = None; rango = [d for d in datos if d["fecha"] >= desde]
        if not rango: return 0.0
        d_actual = desde; idx = 0
        while d_actual <= hoy:
            while idx < len(rango) and rango[idx]["fecha"] <= d_actual:
                ultimo = rango[idx]["valor"]; idx += 1
            if ultimo is not None:
                tasa_dia = max(0.0, ultimo - spread)
                acum *= (1 + tasa_dia / 360 / 100)
            d_actual += timedelta(days=1)
        return acum - 1
    def anualizar(acum_dec, años):
        if acum_dec <= -1: return -100.0
        return round(((1 + acum_dec) ** (1 / años) - 1) * 100, 2)
    def efectivo(acum_dec): return round(acum_dec * 100, 2)
    inicio_ytd = date(hoy.year, 1, 1)
    ini_back = date(2000, 1, 1)
    if datos and datos[0]["fecha"] > ini_back: ini_back = datos[0]["fecha"]
    bt_puntos = []; cur = date(ini_back.year, ini_back.month, 1)
    acum_bt = 1.0; ultimo = None; idx_bt = 0
    datos_bt = [d for d in datos if d["fecha"] >= ini_back]; d_cur = ini_back
    while d_cur <= hoy:
        while idx_bt < len(datos_bt) and datos_bt[idx_bt]["fecha"] <= d_cur:
            ultimo = datos_bt[idx_bt]["valor"]; idx_bt += 1
        if ultimo is not None:
            tasa_dia = max(0.0, ultimo - spread)
            acum_bt *= (1 + tasa_dia / 360 / 100)
        if d_cur.day == 1 or d_cur == ini_back:
            bt_puntos.append({"fecha": d_cur.isoformat(), "valor": round(acum_bt * 100, 4)})
        d_cur += timedelta(days=1)
    return {"r1m":efectivo(componer_acum(hoy-timedelta(days=30))),"r3m":efectivo(componer_acum(hoy-timedelta(days=91))),
            "r6m":efectivo(componer_acum(hoy-timedelta(days=182))),"ytd":efectivo(componer_acum(inicio_ytd)),
            "r1y":anualizar(componer_acum(hoy-timedelta(days=365)),1.0),"r2y":anualizar(componer_acum(hoy-timedelta(days=730)),2.0),
            "r3y":anualizar(componer_acum(hoy-timedelta(days=1095)),3.0),"backtesting":bt_puntos}


def get_banxico_dato(serie_id):
    try:
        url  = f"{BANXICO_BASE}/{serie_id}/datos/oportuno"
        hdrs = {"Bmx-Token": BANXICO_TOKEN, "Accept": "application/json"}
        resp = requests.get(url, headers=hdrs, timeout=10)
        resp.raise_for_status()
        datos = resp.json()["bmx"]["series"][0]["datos"]
        return datos[0]["dato"] if datos else None
    except Exception as e:
        print(f"[BANXICO ERROR] {serie_id}: {e}")
        return None


@app.route("/api/diag-repo")
def diag_repo():
    if "usuario" not in session:
        return jsonify({"ok": False, "error": "No autenticado"}), 401
    resultado = {}
    try:
        hoy = date.today(); ini = (hoy - timedelta(days=10)).isoformat(); fin = hoy.isoformat()
        raw = _banxico_serie_rango(SERIE_TIIE28, ini, fin)
        resultado["banxico"] = {"ok": len(raw) > 0, "token_set": bool(BANXICO_TOKEN), "registros": len(raw), "ultimo": raw[-1] if raw else None}
    except Exception as e:
        resultado["banxico"] = {"ok": False, "error": str(e)}
    try:
        hoy = date.today()
        params = {"series_id": "DFF", "observation_start": (hoy-timedelta(days=10)).isoformat(),
                  "observation_end": hoy.isoformat(), "api_key": FRED_API_KEY, "file_type": "json"}
        r = requests.get(FRED_BASE, params=params, timeout=10)
        obs = r.json().get("observations", [])
        resultado["fred"] = {"ok": len(obs) > 0, "key_set": bool(FRED_API_KEY), "status": r.status_code, "registros": len(obs), "ultimo": obs[-1] if obs else None}
    except Exception as e:
        resultado["fred"] = {"ok": False, "error": str(e)}
    try:
        rend_mxn = get_repo_rendimientos(7.0, False); rend_usd = get_repo_rendimientos(4.0, True)
        resultado["rendimientos_mxn"] = rend_mxn; resultado["rendimientos_usd"] = rend_usd
    except Exception as e:
        resultado["rendimientos"] = {"error": str(e)}
    return jsonify(resultado)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
