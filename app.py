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

# ── ETF: nombres simplificados → índice representado ──
ETF_INDEX_MAP = {
    "SPY": "S&P 500", "VOO": "S&P 500", "IVV": "S&P 500",
    "QQQ": "Nasdaq 100", "QQQM": "Nasdaq 100",
    "DIA": "Dow Jones 30", "DJIA": "Dow Jones 30",
    "IWM": "Russell 2000", "VTWO": "Russell 2000",
    "VTI": "US Total Market", "ITOT": "US Total Market",
    "EWW": "MSCI México", "EWZ": "MSCI Brasil", "EWJ": "MSCI Japón",
    "EFA": "MSCI EAFE", "EEM": "MSCI Emerging Markets", "VWO": "FTSE Emerging",
    "IEMG": "MSCI Core EM", "MCHI": "MSCI China",
    "VEA": "FTSE Developed ex-US", "VXUS": "FTSE All-World ex-US",
    "GLD": "Oro (Gold)", "SLV": "Plata (Silver)", "IAU": "Oro (Gold)",
    "USO": "WTI Crudo", "XLE": "Energy Select", "XLF": "Financial Select",
    "XLK": "Technology Select", "XLV": "Health Care Select",
    "XLI": "Industrial Select", "XLP": "Consumer Staples Select",
    "XLY": "Consumer Discretionary Select", "XLU": "Utilities Select",
    "ARKK": "ARK Innovation", "ARKW": "ARK Next Gen Internet",
    "TLT": "US Treasury 20+ Yr", "IEF": "US Treasury 7-10 Yr",
    "SHY": "US Treasury 1-3 Yr", "BND": "US Aggregate Bond",
    "AGG": "US Aggregate Bond", "LQD": "IG Corporate Bond",
    "HYG": "High Yield Bond", "JNK": "High Yield Bond",
    "VNQ": "US REITs", "VNQI": "Intl REITs",
    "NAFTRAC": "IPC México", "IVVPESO": "S&P 500 (MXN)",
}

_ETF_BRAND_PREFIXES = [
    "iShares ", "Vanguard ", "SPDR ", "Invesco ", "WisdomTree ",
    "ProShares ", "First Trust ", "Schwab ", "Global X ", "VanEck ",
    "ARK ", "JPMorgan ", "Fidelity ", "Franklin ", "PIMCO ",
    "BlackRock ", "State Street ", "Dimensional ",
]
_ETF_BRAND_SUFFIXES = [
    " ETF", " Trust", " Fund", " Index Fund", " Portfolio",
    " Shares", " UCITS", " Acc",
]

def simplificar_nombre_etf(ticker: str, nombre: str) -> str:
    clean = ticker.replace(".MX", "").upper()
    if clean in ETF_INDEX_MAP:
        return ETF_INDEX_MAP[clean]
    result = nombre
    for prefix in _ETF_BRAND_PREFIXES:
        if result.startswith(prefix):
            result = result[len(prefix):]
    for suffix in _ETF_BRAND_SUFFIXES:
        if result.endswith(suffix):
            result = result[:-len(suffix)]
    return result.strip() or nombre

# ── ETF: exposición geográfica — fuentes reales por proveedor ──
# Regiones canónicas en INGLÉS Morningstar (RE-RegionalExposure):
# United States, Canada, Latin America, Eurozone, Europe - ex Euro,
# United Kingdom, Japan, Australasia, Asia - Developed, Asia - Emerging,
# Europe - Emerging, Africa, Middle East

# iShares: product-id/slug para descargar CSV de holdings con Location
ISHARES_PRODUCTS = {
    "ACWI": "239600/ishares-msci-acwi-etf",
    "IVV":  "239726/ishares-core-sp-500-etf",
    "EEM":  "239637/ishares-msci-emerging-markets-etf",
    "EFA":  "239623/ishares-msci-eafe-etf",
    "IEFA": "244049/ishares-core-msci-eafe-etf",
    "IEMG": "244050/ishares-core-msci-emerging-markets-etf",
    "EWW":  "239676/ishares-msci-mexico-etf",
    "EWZ":  "239612/ishares-msci-brazil-etf",
    "EWJ":  "239665/ishares-msci-japan-etf",
    "EWG":  "239649/ishares-msci-germany-etf",
    "EWU":  "239690/ishares-msci-united-kingdom-etf",
    "EWA":  "239607/ishares-msci-australia-etf",
    "EWC":  "239615/ishares-msci-canada-etf",
    "EWT":  "239688/ishares-msci-taiwan-etf",
    "EWY":  "239681/ishares-msci-south-korea-etf",
    "FXI":  "239536/ishares-china-large-cap-etf",
    "MCHI": "239619/ishares-msci-china-etf",
    "INDA": "239659/ishares-msci-india-etf",
    "IWM":  "239710/ishares-russell-2000-etf",
    "URTH": "239750/ishares-msci-world-etf",
}

# Vanguard: tickers con endpoint /allocation que devuelve regiones
VANGUARD_TICKERS = {"VOO", "VTI", "VT", "VEA", "VXUS", "VWO", "VIG", "VUG", "VTV", "SCHD"}

# Mapeo Vanguard regiones → Morningstar regiones
VANGUARD_REGION_MAP = {
    "north america":     "United States",   # mayormente US
    "europe":            "Eurozone",
    "pacific":           "Japan",           # Japón + Asia Pac desarrollado
    "emerging markets":  "Asia - Emerging",
    "middle east":       "Middle East",
    "latin america":     "Latin America",
    "united kingdom":    "United Kingdom",
    "other":             "Otros",
}

# Mapeo iShares Location → Morningstar región (complementa COUNTRY_TO_REGION)
ISHARES_LOCATION_MAP = {
    "korea (south)": "Asia - Developed",
    "korea": "Asia - Developed",
    "cayman islands": "Asia - Emerging",
    "bermuda": "United States",
    "jersey": "Europe - ex Euro",
    "guernsey": "Europe - ex Euro",
    "isle of man": "Europe - ex Euro",
    "macau": "Asia - Emerging",
    "curacao": "Latin America",
    "puerto rico": "United States",
    "virgin islands": "United States",
    "panama": "Latin America",
    "cyprus": "Eurozone",
    "estonia": "Eurozone",
    "latvia": "Eurozone",
    "lithuania": "Eurozone",
    "slovakia": "Eurozone",
    "slovenia": "Eurozone",
    "malta": "Eurozone",
    "croatia": "Eurozone",
    "romania": "Europe - Emerging",
    "kenya": "Africa",
    "morocco": "Africa",
    "mauritius": "Africa",
    "pakistan": "Asia - Emerging",
    "bangladesh": "Asia - Emerging",
    "sri lanka": "Asia - Emerging",
    "kuwait": "Middle East",
    "bahrain": "Middle East",
    "oman": "Middle East",
    "jordan": "Middle East",
    "iceland": "Europe - ex Euro",
}

# Mapeo iShares Sector → español (nombres del CSV de holdings)
ISHARES_SECTOR_MAP = {
    "information technology": "Tecnología",
    "financials":             "Financiero",
    "industrials":            "Industriales",
    "consumer discretionary": "Consumo Discrecional",
    "health care":            "Salud",
    "communication":          "Comunicaciones",
    "consumer staples":       "Consumo Básico",
    "materials":              "Materiales",
    "energy":                 "Energía",
    "utilities":              "Utilidades",
    "real estate":            "Bienes Raíces",
    "cash and/or derivatives": None,  # excluir del drilldown
}

# Fallback estático para commodities y ETFs sin holdings
ETF_GEO_STATIC = {
    "GLD":     {"Global": 100.0},
    "SLV":     {"Global": 100.0},
    "IAU":     {"Global": 100.0},
    "NAFTRAC": {"Latin America": 100.0},
    "IVVPESO": {"United States": 100.0},
}

# Cache de geo+sec por ticker de ETF (evita re-fetches dentro de la sesión)
_ETF_DATA_CACHE = {}  # ticker → {"geo": dict, "sec": dict}

def _fetch_ishares_data(ticker: str) -> tuple:
    """Descarga CSV de iShares y extrae geo (Location) y sectores (Sector).
    Retorna (geo_dict, sec_dict) con regiones Morningstar y sectores en español."""
    import csv
    slug = ISHARES_PRODUCTS.get(ticker)
    if not slug:
        return {}, {}
    url = f"https://www.ishares.com/us/products/{slug}/1467271812596.ajax?tab=holdings&fileType=csv"
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        if r.status_code != 200:
            return {}, {}
    except Exception:
        return {}, {}
    lines = r.text.strip().split("\n")
    header_idx = None
    for i, line in enumerate(lines[:15]):
        if "Weight" in line and ("Location" in line or "Sector" in line):
            header_idx = i
            break
    if header_idx is None:
        return {}, {}
    reader = csv.DictReader(lines[header_idx:])
    geo = {}
    sec = {}
    for row in reader:
        try:
            raw_w = row.get("Weight (%)")
            if raw_w is None:
                continue
            w = float(str(raw_w).replace(",", ""))
        except (ValueError, TypeError):
            continue
        if w <= 0:
            continue
        # Geografía
        loc = (row.get("Location") or "").strip()
        if loc:
            loc_lower = loc.lower()
            region = (COUNTRY_TO_REGION.get(loc_lower)
                      or ISHARES_LOCATION_MAP.get(loc_lower)
                      or "Otros")
            geo[region] = geo.get(region, 0) + w
        # Sectores
        raw_sec = (row.get("Sector") or "").strip()
        if raw_sec:
            sec_label = ISHARES_SECTOR_MAP.get(raw_sec.lower())
            if sec_label is None:  # None = excluir (cash/derivatives)
                continue
            if not sec_label:
                sec_label = raw_sec
            sec[sec_label] = sec.get(sec_label, 0) + w
    return geo, sec

def _fetch_vanguard_geo(ticker: str) -> dict:
    """Usa API de Vanguard /allocation para obtener regiones."""
    url = f"https://investor.vanguard.com/investment-products/etfs/profile/api/{ticker}/allocation"
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        if r.status_code != 200:
            return {}
        data = r.json()
        regions = data.get("region", {}).get("region", [])
        if not regions:
            return {}
    except Exception:
        return {}
    geo = {}
    for reg in regions:
        name = reg.get("name", "").strip().lower()
        try:
            pct = float(reg.get("percent", "0"))
        except (ValueError, TypeError):
            continue
        if pct <= 0:
            continue
        region = VANGUARD_REGION_MAP.get(name, "Otros")
        geo[region] = geo.get(region, 0) + pct
    return geo

def _fetch_holdings_geo(ticker: str) -> dict:
    """Fallback: usa top holdings de Yahoo Finance y busca país de cada uno."""
    import yfinance as yf
    try:
        t = yf.Ticker(ticker)
        fd = t.funds_data
        th = fd.top_holdings
        if th is None or th.empty:
            return {}
    except Exception:
        return {}

    geo = {}
    total_w = 0.0
    for symbol, row in th.iterrows():
        w = float(row.get("Holding Percent", 0))
        if w <= 0:
            continue
        try:
            info_h = yf.Ticker(symbol).info
            country_h = (info_h.get("country") or "").strip().lower()
        except Exception:
            country_h = ""
        region = (COUNTRY_TO_REGION.get(country_h)
                  or ISHARES_LOCATION_MAP.get(country_h)
                  or "Otros")
        geo[region] = geo.get(region, 0) + w * 100
        total_w += w

    # Escalar proporcionalmente a 100%
    if total_w > 0 and geo:
        scale = 100.0 / (total_w * 100)
        geo = {k: round(v * scale, 2) for k, v in geo.items()}
    return geo

def get_etf_data(ticker: str) -> dict:
    """Obtiene exposición geográfica y sectorial real de un ETF.
    Retorna {"geo": dict, "sec": dict}.
    Cascada:
      1. iShares CSV (geo + sec exactos)
      2. Vanguard API (geo por región, sec de Yahoo)
      3. Fallback estático (commodities)
      4. Yahoo Finance top holdings (geo escalada)
    """
    clean_tk = ticker.replace(".MX", "").upper()

    # Cache
    if clean_tk in _ETF_DATA_CACHE:
        c = _ETF_DATA_CACHE[clean_tk]
        return {"geo": dict(c["geo"]), "sec": dict(c["sec"])}

    geo = {}
    sec = {}

    # 1. iShares CSV (datos exactos de geo + sectores)
    if clean_tk in ISHARES_PRODUCTS:
        geo, sec = _fetch_ishares_data(clean_tk)

    # 2. Vanguard API (solo regiones; sectores vendrán de Yahoo)
    if not geo and clean_tk in VANGUARD_TICKERS:
        geo = _fetch_vanguard_geo(clean_tk)

    # 3. Fallback estático (commodities, NAFTRAC, etc.)
    if not geo and clean_tk in ETF_GEO_STATIC:
        geo = dict(ETF_GEO_STATIC[clean_tk])

    # 4. Yahoo Finance top holdings (geo escalada proporcional)
    if not geo:
        geo = _fetch_holdings_geo(ticker)

    result = {"geo": geo, "sec": sec}
    if geo or sec:
        _ETF_DATA_CACHE[clean_tk] = result
    return result

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
# ACCIONES MX — DataBursatil (fuente principal para BMV/BIVA)
# ─────────────────────────────────────────────────────────────────────────────
DB_TOKEN = os.environ.get("DATABURSATIL_TOKEN", "")
DB_BASE  = "https://api.databursatil.com/v2"

# Casos especiales DataBursatil → Yahoo Finance
# (la Ñ se omite en DataBursatil pero se escribe como & en Yahoo Finance)
_YF_OVERRIDES = {"PENOLES": "PE&OLES"}

def _db_to_yf(ticker_db: str) -> str:
    """Convierte ticker de DataBursatil al formato Yahoo Finance (.MX)."""
    t = ticker_db.rstrip("*")                  # WALMEX* → WALMEX
    t = _YF_OVERRIDES.get(t, t)               # PENOLES → PE&OLES
    return t + ".MX"

_db_cache: dict    = {}
_db_cache_ts: dict = {}
DB_CACHE_TTL = 3600

# Catálogo completo de emisoras (BMV local + SIC/global + BIVA)
# Cargado una vez al inicio; estructura: {ticker: {nombre, bolsa, tipo, mercado}}
_catalogo_emisoras: dict = {}
_catalogo_ts: float      = 0
_CATALOGO_TTL = 86400    # refrescar cada 24 horas


def cargar_catalogo_emisoras(forzar: bool = False) -> dict:
    """
    Descarga el catálogo completo de emisoras de DataBursatil.
    Incluye BMV local, SIC (global) y BIVA.
    Retorna dict {ticker_db: {nombre, bolsa, tipo, mercado, yf_ticker}}
    """
    global _catalogo_emisoras, _catalogo_ts
    now = time.time()
    if not forzar and _catalogo_emisoras and (now - _catalogo_ts) < _CATALOGO_TTL:
        return _catalogo_emisoras
    if not DB_TOKEN:
        return {}

    catalogo = {}
    # Respuesta de DataBursatil: {emisora: {serie: {campos}}}
    for mercado in ["local", "global"]:
        try:
            r = requests.get(
                f"{DB_BASE}/emisoras",
                params={"token": DB_TOKEN, "mercado": mercado},
                timeout=30,
            )
            r.raise_for_status()
            data  = r.json()  # {emisora: {serie: {campos}}}
            count = 0
            for emisora, series in data.items():
                if not isinstance(series, dict):
                    continue
                for serie, campos in series.items():
                    if not isinstance(campos, dict):
                        continue
                    ticker_db = emisora.strip().upper() + serie.strip().upper()
                    yf_ticker = _db_to_yf(ticker_db)
                    tv = (campos.get("tipo_valor_descripcion") or "").upper()
                    if "FIBRA" in tv or "FIDEICOMISO" in tv:
                        tipo = "FIBRA"
                    elif "ETF" in tv or "TRAC" in tv or "FONDO" in tv:
                        tipo = "ETF"
                    elif "SIC" in tv or "SISTEMA INTERNACIONAL" in tv:
                        tipo = "SIC"
                    else:
                        tipo = "Acción"
                    catalogo[ticker_db] = {
                        "ticker_db": ticker_db,
                        "yf_ticker": yf_ticker,
                        "nombre":    campos.get("razon_social") or ticker_db,
                        "bolsa":     (campos.get("bolsa") or "").upper(),
                        "tipo":      tipo,
                        "mercado":   mercado,
                        "isin":      campos.get("isin", ""),
                        "estatus":   campos.get("estatus", ""),
                    }
                    count += 1
            print(f"[CATALOGO] {mercado}: {count} emisoras cargadas")
        except Exception as e:
            print(f"[CATALOGO ERROR] mercado={mercado}: {e}")

    if catalogo:
        _catalogo_emisoras = catalogo
        _catalogo_ts       = now
        print(f"[CATALOGO] Total: {len(catalogo)} emisoras (BMV + SIC + BIVA)")
    return _catalogo_emisoras


def get_accion_db(emisora_serie: str) -> dict | None:
    """
    Obtiene datos de una emisora mexicana desde DataBursatil.
    emisora_serie: p.ej. "AMXL", "GMEXICOB", "VOLARA"
    """
    if not DB_TOKEN:
        return None

    now = time.time()
    key = emisora_serie.upper().strip()
    if key in _db_cache and (now - _db_cache_ts.get(key, 0)) < DB_CACHE_TTL:
        return _db_cache[key]

    hoy = date.today()
    ini = "2000-01-01"
    fin = hoy.isoformat()

    # 1. Historial de precios
    try:
        r = requests.get(
            f"{DB_BASE}/historicos",
            params={"token": DB_TOKEN, "emisora_serie": key, "inicio": ini, "final": fin},
            timeout=20,
        )
        r.raise_for_status()
        hist_raw = r.json()
    except Exception as e:
        print(f"[DB HIST ERROR] {key}: {e}")
        return None

    # El response puede venir como dict directo {fecha: {precio, importe}}
    # o envuelto en {"data": {...}}
    if isinstance(hist_raw, dict) and "data" in hist_raw and isinstance(hist_raw["data"], dict):
        hist_raw = hist_raw["data"]

    if not hist_raw or not isinstance(hist_raw, dict):
        print(f"[DB] {key}: sin datos históricos")
        return None

    precios: list[tuple[date, float]] = []
    for fecha_str, vals in hist_raw.items():
        try:
            d = date.fromisoformat(fecha_str[:10])
            p = float(vals.get("precio", 0) if isinstance(vals, dict) else vals)
            if p > 0:
                precios.append((d, p))
        except Exception:
            pass

    if not precios:
        return None

    precios.sort(key=lambda x: x[0])

    def precio_en(target: date):
        candidates = [p for d, p in precios if d <= target]
        return candidates[-1] if candidates else None

    p_hoy = precio_en(hoy)
    if p_hoy is None:
        return None

    precio_cierre = round(precios[-1][1], 2)
    p_mtd = precio_en(date(hoy.year, hoy.month, 1))
    p_3m  = precio_en(hoy - timedelta(days=91))
    p_ytd = precio_en(date(hoy.year, 1, 1))
    p_1y  = precio_en(hoy - timedelta(days=365))
    p_2y  = precio_en(hoy - timedelta(days=730))
    p_3y  = precio_en(hoy - timedelta(days=1095))

    def rend_efectivo(p_ini):
        if p_ini and p_ini > 0:
            return round((p_hoy / p_ini - 1) * 100, 2)
        return None

    def rend_anual(p_ini, years):
        if p_ini and p_ini > 0:
            return round(((p_hoy / p_ini) ** (1 / years) - 1) * 100, 2)
        return None

    # 2. Info de la emisora (nombre, tipo) — desde catálogo en memoria
    catalogo = cargar_catalogo_emisoras()
    em_info  = catalogo.get(key, {})
    nombre   = em_info.get("nombre", key)
    tipo     = em_info.get("tipo",   "Acción")
    if tipo == "ETF":
        nombre = simplificar_nombre_etf(key, nombre)

    # Geo + Sectores: cascada real (iShares → Vanguard → estático → Yahoo)
    geo_db = {}
    sec_db = {}
    if tipo == "ETF":
        etf_d = get_etf_data(key)
        geo_db = etf_d.get("geo", {})
        sec_db = etf_d.get("sec", {})
    if not geo_db:
        geo_db = {"Latin America": 100.0}

    # ── Backtesting: serie mensual base 100 ──
    historico_bt = []
    try:
        df = pd.DataFrame(precios, columns=["fecha", "precio"])
        df["fecha"] = pd.to_datetime(df["fecha"])
        df = df.set_index("fecha").sort_index()
        monthly = df["precio"].resample("MS").first().dropna()
        if len(monthly) > 1:
            base = float(monthly.iloc[0])
            for dt, px in monthly.items():
                historico_bt.append({
                    "fecha": dt.strftime("%Y-%m-%d"),
                    "valor": round(float(px) / base * 100, 4)
                })
    except Exception:
        pass

    result = {
        "ticker":        key,
        "nombre":        nombre,
        "tipo":          tipo,
        "sector":        "",
        "pais":          "México",
        "moneda":        "MXN",
        "precio_cierre": precio_cierre,
        "moneda_precio": "MXN",
        "r1m":           rend_efectivo(p_mtd),
        "r3m":           rend_efectivo(p_3m),
        "ytd":           rend_efectivo(p_ytd),
        "r1y":           rend_anual(p_1y, 1),
        "r2y":           rend_anual(p_2y, 2),
        "r3y":           rend_anual(p_3y, 3),
        "sectores":      sec_db,
        "geo":           geo_db,
        "historico":     historico_bt,
    }

    _db_cache[key]    = result
    _db_cache_ts[key] = now
    print(f"[DB OK] {key}: {nombre} | p={precio_cierre:.2f} | tipo={tipo}")
    return result


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
    # Keys con espacios (info.sector de YF)
    "technology": "Tecnología", "financial services": "Financiero",
    "healthcare": "Salud", "consumer cyclical": "Consumo Discrecional",
    "industrials": "Industriales", "communication services": "Comunicaciones",
    "consumer defensive": "Consumo Básico", "energy": "Energía",
    "basic materials": "Materiales", "real estate": "Bienes Raíces",
    "utilities": "Utilidades",
    # Keys con underscores (funds_data.sector_weightings de YF)
    "financial_services": "Financiero", "consumer_cyclical": "Consumo Discrecional",
    "communication_services": "Comunicaciones", "consumer_defensive": "Consumo Básico",
    "basic_materials": "Materiales", "realestate": "Bienes Raíces",
}

# País → Región Morningstar EN INGLÉS (mismas keys que RE-RegionalExposure)
COUNTRY_TO_REGION = {
    "united states": "United States", "canada": "Canada",
    "mexico": "Latin America", "brazil": "Latin America", "chile": "Latin America",
    "colombia": "Latin America", "peru": "Latin America", "argentina": "Latin America",
    "united kingdom": "United Kingdom",
    "germany": "Eurozone", "france": "Eurozone", "netherlands": "Eurozone",
    "spain": "Eurozone", "italy": "Eurozone", "belgium": "Eurozone",
    "austria": "Eurozone", "finland": "Eurozone", "ireland": "Eurozone",
    "portugal": "Eurozone", "greece": "Eurozone", "luxembourg": "Eurozone",
    "switzerland": "Europe - ex Euro", "sweden": "Europe - ex Euro",
    "norway": "Europe - ex Euro", "denmark": "Europe - ex Euro",
    "poland": "Europe - Emerging", "czech republic": "Europe - Emerging",
    "hungary": "Europe - Emerging", "turkey": "Europe - Emerging", "russia": "Europe - Emerging",
    "japan": "Japan",
    "australia": "Australasia", "new zealand": "Australasia",
    "hong kong": "Asia - Developed", "singapore": "Asia - Developed",
    "south korea": "Asia - Developed", "taiwan": "Asia - Developed",
    "china": "Asia - Emerging", "india": "Asia - Emerging",
    "indonesia": "Asia - Emerging", "thailand": "Asia - Emerging",
    "malaysia": "Asia - Emerging", "philippines": "Asia - Emerging", "vietnam": "Asia - Emerging",
    "saudi arabia": "Middle East", "israel": "Middle East",
    "united arab emirates": "Middle East", "qatar": "Middle East",
    "south africa": "Africa", "nigeria": "Africa", "egypt": "Africa",
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
        hist = t.history(start="2000-01-01", auto_adjust=False)
    except Exception as e:
        print(f"[YF] {ticker} intento-sesión falló: {e}")

    # ── Intento 2: yfinance nativo sin sesión ──
    if hist is None or hist.empty:
        try:
            t    = yf.Ticker(ticker)
            hist = t.history(start="2000-01-01", auto_adjust=False)
        except Exception as e:
            print(f"[YF] {ticker} intento-nativo falló: {e}")

    # ── Intento 3: yf.download (más estable en servidores cloud) ──
    if hist is None or hist.empty:
        try:
            hist = yf.download(ticker, start="2000-01-01", auto_adjust=False,
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
        # fast_info como respaldo para precio de mercado
        try:
            fi = t.fast_info
            if fi:
                if not info.get("regularMarketPrice") and hasattr(fi, "last_price"):
                    info["regularMarketPrice"] = fi.last_price
                if not info.get("previousClose") and hasattr(fi, "previous_close"):
                    info["previousClose"] = fi.previous_close
        except Exception:
            pass

    try:
        today  = datetime.now().date()
        prices = hist["Close"].dropna()
        if prices.empty:
            return None

        # ── Limpiar serie bimodal (SIC .MX mezcla precios USD y MXN) ──
        # Detectar por daily returns >+100% (imposibles en mercado real)
        if len(prices) > 20:
            daily_ret = prices.pct_change().dropna()
            extreme_jumps = (daily_ret.abs() > 1.0).sum()  # >±100% diario
            if extreme_jumps > 3:
                median_price = prices.median()
                last_price = float(prices.iloc[-1])
                if last_price > median_price:
                    prices = prices[prices > median_price * 0.3]
                else:
                    prices = prices[prices < median_price * 3]

        idx = prices.index

        def precio_en(d: date):
            ts = [i for i in idx if i.date() <= d]
            return float(prices[ts[-1]]) if ts else None

        p_hoy = precio_en(today)
        if p_hoy is None:
            return None

        # Usar regularMarketPrice para todo (más preciso que history para SIC)
        raw_price = info.get("regularMarketPrice") or info.get("currentPrice")
        if raw_price and float(raw_price) > 0:
            p_hoy = float(raw_price)
        precio_cierre = round(p_hoy, 2)

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
        pais       = COUNTRY_TO_REGION.get(pais_en, info.get("country") or "Latin America")
        moneda     = "MXN" if ticker.endswith(".MX") else "USD"
        nombre     = info.get("shortName") or info.get("longName") or ticker

        if quote_type == "ETF":
            nombre = simplificar_nombre_etf(ticker, nombre)

        sectores_etf = {}
        geo_etf      = {}

        if quote_type == "ETF":
            # Geo + Sectores: cascada proveedor (iShares/Vanguard) → estático → Yahoo
            etf_data = get_etf_data(ticker)
            geo_etf = etf_data.get("geo", {})
            sectores_etf = etf_data.get("sec", {})
            # Sectores fallback: Yahoo Finance sector_weightings
            if not sectores_etf:
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
                region = COUNTRY_TO_REGION.get(pais_en, pais)
                geo_etf[region] = 100.0

        # ── Backtesting: serie mensual base 100 ──
        historico_bt = []
        try:
            monthly = prices.resample('MS').first().dropna()
            if len(monthly) > 1:
                base = float(monthly.iloc[0])
                for dt, px in monthly.items():
                    historico_bt.append({
                        "fecha": dt.strftime("%Y-%m-%d"),
                        "valor": round(float(px) / base * 100, 4)
                    })
        except Exception:
            pass

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
            "historico":     historico_bt,
        }

        _accion_cache[ticker]    = result
        _accion_cache_ts[ticker] = now
        print(f"[YF OK] {ticker}: {nombre} | p={precio_cierre:.2f} | tipo={tipo} | pais={pais}")
        return result

    except Exception as e:
        print(f"[YF ERROR] {ticker}: {e}")
        return None


def get_accion(ticker: str) -> dict | None:
    """
    Fuente unificada para acciones/ETFs.
    Prioridad: Yahoo Finance SIC (.MX) → DataBursatil (BMV local + SIC en MXN).
    YF primero para que precio actual (regularMarketPrice) e histórico sean consistentes.
    """
    db_key = ticker.upper().replace(".MX", "")
    # Normalizar caracteres especiales BMV (ñ/Ñ → & para Yahoo Finance)
    db_key = db_key.replace("Ñ", "&").replace("ñ", "&")

    # 1. Yahoo Finance SIC — ticker con .MX (MXN), precio más preciso
    mx_ticker = db_key + ".MX"
    data = get_accion_yf(mx_ticker)
    if data:
        return data

    # 2. DataBursatil — fallback para emisoras que YF no tenga
    if DB_TOKEN:
        data = get_accion_db(db_key)
        if data:
            return data

    # 3. Último recurso: Yahoo Finance global (solo si los anteriores fallaron)
    if ticker.upper() != mx_ticker:
        return get_accion_yf(ticker)
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
                        acciones: list = None,
                        bt_fecha_ini: str = None, bt_fecha_fin: str = None) -> dict:
    universe = load_ms_universe()

    r1m = r3m = r6m = ytd = r1y = r2y = r3y = 0.0
    stock_t = bond_t = cash_t = 0.0
    accion_t = etf_t = 0.0
    geo_acc = {}; sec_acc = {}; supersec_acc = {}
    lista = []

    dur_mxn_num = ytm_mxn_num = bond_mxn_denom = 0.0
    dur_usd_num = ytm_usd_num = bond_usd_denom = 0.0
    cred_mxn = {}; cred_usd = {}
    bt_components = []  # {"weight": float, "series": {fecha: valor_base100}, "is_repo": bool}

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
            "ytd": round(safe_float(d.get("TTR-ReturnYTD")),  2),
            "r1y": round(safe_float(d.get("TTR-Return1Yr")),  2),
            "r2y": round(safe_float(d.get("TTR-Return2Yr")),  2),
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
        repo_bt_series = {pt["fecha"]: pt["valor"] for pt in rend.get("backtesting", [])}
        if repo_bt_series:
            bt_components.append({"weight": w, "series": repo_bt_series, "is_repo": True})
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
                      "ytd": round(rend["ytd"], 2),
                      "r1y": round(rend["r1y"], 2), "r2y": round(rend["r2y"], 2),
                      "r3y": round(rend["r3y"], 2)})

    # ── Acciones & ETFs (Yahoo Finance) ──
    for acc in (acciones or []):
        ticker = acc.get("ticker", "").upper()
        pct    = float(acc.get("pct", 0))
        if pct <= 0 or not ticker:
            continue
        w   = pct / 100.0
        yfd = get_accion(ticker)
        if not yfd:
            continue

        r1m += (yfd.get("r1m") or 0) * w
        r3m += (yfd.get("r3m") or 0) * w
        ytd += (yfd.get("ytd") or 0) * w
        r1y += (yfd.get("r1y") or 0) * w
        r2y += (yfd.get("r2y") or 0) * w
        r3y += (yfd.get("r3y") or 0) * w
        if yfd.get("tipo") == "ETF":
            etf_t += 100 * w
        else:
            accion_t += 100 * w

        display_tk = yfd.get("ticker", ticker).replace(".MX", "")
        lista.append({
            "fondo": display_tk, "serie": yfd.get("tipo", "Acción"), "pct": round(pct, 2),
            "r1m": round(yfd.get("r1m") or 0, 2), "r3m": round(yfd.get("r3m") or 0, 2),
            "ytd": round(yfd.get("ytd") or 0, 2),
            "r1y": round(yfd.get("r1y") or 0, 2), "r2y": round(yfd.get("r2y") or 0, 2),
            "r3y": round(yfd.get("r3y") or 0, 2),
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

        # Backtesting: serie individual del componente
        acc_bt_series = {pt["fecha"]: pt["valor"] for pt in yfd.get("historico", [])}
        if acc_bt_series:
            bt_components.append({"weight": w, "series": acc_bt_series, "is_repo": False})

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

    # ── Combinar backtesting dinámico ──
    # Cada componente entra cuando alcanza su inception. Pesos se re-normalizan
    # entre componentes activos. Todo arranca base 100.
    bt_portafolio = {}
    bt_repo_filtered = {}

    if bt_components:
        # Todas las fechas únicas de todos los componentes
        all_dates = sorted(set(d for c in bt_components for d in c["series"]))

        # Aplicar rango: inicio = max(fecha_config, primer dato disponible)
        f_ini = bt_fecha_ini or all_dates[0]
        f_fin = bt_fecha_fin or all_dates[-1]
        all_dates = [d for d in all_dates if d <= f_fin]
        # Si fecha_ini es anterior al primer dato, ajustar al inception
        if f_ini < all_dates[0]:
            f_ini = all_dates[0]
        all_dates = [d for d in all_dates if d >= f_ini]

        if all_dates:
            port_value = 100.0
            repo_value = 100.0
            comp_prev = {}   # j → valor base-100 en el período anterior
            has_any_repo = any(c["is_repo"] for c in bt_components)

            for i, fecha in enumerate(all_dates):
                # Actualizar valores conocidos (forward-fill implícito: comp_prev retiene último)
                comp_now = {}
                for j, comp in enumerate(bt_components):
                    if fecha in comp["series"]:
                        comp_now[j] = comp["series"][fecha]
                    elif j in comp_prev:
                        comp_now[j] = comp_prev[j]  # forward-fill

                if i == 0:
                    bt_portafolio[fecha] = 100.0
                    if has_any_repo:
                        bt_repo_filtered[fecha] = 100.0
                    comp_prev = comp_now
                    continue

                # Retornos: solo de componentes activos en AMBOS períodos
                returns_all  = []
                returns_repo = []
                for j in comp_now:
                    if j in comp_prev and comp_prev[j] > 0:
                        ret = (comp_now[j] / comp_prev[j]) - 1
                        returns_all.append((j, ret))
                        if bt_components[j]["is_repo"]:
                            returns_repo.append((j, ret))

                # Portafolio holístico: pesos re-normalizados entre activos
                if returns_all:
                    total_w = sum(bt_components[j]["weight"] for j, _ in returns_all)
                    if total_w > 0:
                        w_ret = sum((bt_components[j]["weight"] / total_w) * r for j, r in returns_all)
                        port_value *= (1 + w_ret)
                bt_portafolio[fecha] = round(port_value, 4)

                # Línea de solo repos (referencia)
                if returns_repo and has_any_repo:
                    total_rw = sum(bt_components[j]["weight"] for j, _ in returns_repo)
                    if total_rw > 0:
                        rw_ret = sum((bt_components[j]["weight"] / total_rw) * r for j, r in returns_repo)
                        repo_value *= (1 + rw_ret)
                    bt_repo_filtered[fecha] = round(repo_value, 4)

                comp_prev = comp_now

    return {
        "ok": True,
        "rendimientos": {
            "mtd":round(r1m,2),"r3m":round(r3m,2),
            "ytd":round(ytd,2),"r1y":round(r1y,2),
            "r2y":round(r2y,2),"r3y":round(r3y,2),
        },
        "clase_activos": (lambda: {
            "labels": [l for l, v in [
                ("Deuda", round(bond_t, 2)),
                ("Renta Variable", round(stock_t, 2)),
                ("Acciones", round(accion_t, 2)),
                ("ETF", round(etf_t, 2)),
                ("Reporto", round(cash_t, 2)),
            ] if v > 0],
            "values": [v for _, v in [
                ("Deuda", round(bond_t, 2)),
                ("Renta Variable", round(stock_t, 2)),
                ("Acciones", round(accion_t, 2)),
                ("ETF", round(etf_t, 2)),
                ("Reporto", round(cash_t, 2)),
            ] if v > 0],
        })(),
        "composicion": sorted(lista, key=lambda x: -x["pct"]),
        "geo":           filter_pct(geo_acc, translate=GEO_TRANSLATE),
        "sectores":      filter_pct(sec_acc),
        "supersectores": filter_pct(supersec_acc),
        "has_rv":        stock_t + accion_t + etf_t > 0,
        "has_deuda":     has_mxn or has_usd,
        "bt_repo":       sorted(
            [{"fecha": f, "valor": round(v, 4)} for f, v in bt_repo_filtered.items()],
            key=lambda x: x["fecha"]
        ) if bt_repo_filtered else [],
        "bt_portafolio": sorted(
            [{"fecha": f, "valor": round(v, 4)} for f, v in bt_portafolio.items()],
            key=lambda x: x["fecha"]
        ) if bt_portafolio else [],
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

    db_key    = ticker.replace(".MX", "")
    # Normalizar caracteres especiales BMV (ñ/Ñ → & para Yahoo Finance)
    db_key    = db_key.replace("Ñ", "&").replace("ñ", "&")
    mx_ticker = db_key + ".MX"

    # 1. DataBursatil — BMV local y SIC (precios en MXN)
    if DB_TOKEN:
        data = get_accion_db(db_key)
        if data:
            return jsonify({"ok": True, "data": data, "fuente": "databursatil"})

    # 2. Yahoo Finance SIC — siempre con .MX para obtener precio en MXN
    data = get_accion_yf(mx_ticker)
    if data:
        return jsonify({"ok": True, "data": data, "fuente": "yahoo_sic"})

    # 3. Último recurso: Yahoo Finance global (solo si los anteriores fallaron)
    if ticker != mx_ticker:
        data = get_accion_yf(ticker)
        if data:
            return jsonify({"ok": True, "data": data, "fuente": "yahoo_global"})

    return jsonify({"ok": False, "error": f"'{db_key}' no encontrado en BMV/SIC. Verifica el ticker."}), 404


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
                                        acciones=body.get("acciones", []),
                                        bt_fecha_ini=body.get("bt_fecha_ini"),
                                        bt_fecha_fin=body.get("bt_fecha_fin")))


# ─────────────────────────────────────────────────────────────────────────────
# MACRO
# ─────────────────────────────────────────────────────────────────────────────
BANXICO_TOKEN = os.environ.get("BANXICO_TOKEN", "592b06934a31710cba9e9a6efebec12c1fe432f5459fc87e7f473380fa0a1d3a")
BANXICO_BASE  = "https://www.banxico.org.mx/SieAPIRest/service/v1/series"
FRED_API_KEY  = os.environ.get("FRED_API_KEY", "1a6dadbec2267dd21b3ad5d6447ed711")
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
        # Combinar DFF (Fed Funds, desde 2000) + SOFR (desde 2018-04) para historia completa
        datos_dff  = []
        datos_sofr = []
        for serie in ["DFF", SERIE_USD_REPO]:
            try:
                params = {"series_id": serie, "observation_start": ini, "observation_end": fin,
                          "api_key": FRED_API_KEY, "file_type": "json"}
                r = requests.get(FRED_BASE, params=params, timeout=15)
                r.raise_for_status()
                obs = [o for o in r.json().get("observations", []) if o["value"] != "."]
                parsed = [{"fecha": _parse_fecha(o["date"]), "valor": float(o["value"])} for o in obs]
                parsed = [d for d in parsed if d["fecha"] is not None]
                if serie == "DFF":
                    datos_dff = parsed
                    print(f"[FRED] DFF: {len(parsed)} registros OK")
                else:
                    datos_sofr = parsed
                    print(f"[FRED] SOFR: {len(parsed)} registros OK")
            except Exception as e:
                print(f"[FRED {serie} ERROR] {e}")
        # SOFR desde su inicio (2018-04), DFF para antes
        if datos_sofr:
            sofr_start = datos_sofr[0]["fecha"]
            datos = [d for d in datos_dff if d["fecha"] < sofr_start] + datos_sofr
            print(f"[HIST USD] Combinado: DFF hasta {sofr_start} + SOFR desde {sofr_start}")
        elif datos_dff:
            datos = datos_dff
        else:
            datos = []
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


@app.route("/api/emisoras/buscar")
def api_buscar_emisora():
    """Búsqueda en el catálogo en memoria — sin costo de créditos."""
    if "usuario" not in session:
        return jsonify({"ok": False, "error": "No autenticado"}), 401
    q = (request.args.get("q") or "").strip().upper()
    if not q or len(q) < 2:
        return jsonify({"ok": True, "results": []})
    if not DB_TOKEN:
        return jsonify({"ok": False, "error": "DataBursatil no configurado"}), 503

    catalogo = cargar_catalogo_emisoras()
    results  = []
    for ticker_db, info in catalogo.items():
        if q in ticker_db or q in info["nombre"].upper():
            results.append({
                "ticker": ticker_db,
                "yf_ticker": info["yf_ticker"],
                "nombre": info["nombre"],
                "bolsa":  info["bolsa"],
                "tipo":   info["tipo"],
                "mercado": info["mercado"],
            })
            if len(results) >= 30:
                break
    return jsonify({"ok": True, "results": results, "total_catalogo": len(catalogo)})


@app.route("/api/emisoras/catalogo")
def api_catalogo_emisoras():
    """Devuelve el catálogo completo (para cargar en el frontend de una vez)."""
    if "usuario" not in session:
        return jsonify({"ok": False, "error": "No autenticado"}), 401
    if not DB_TOKEN:
        return jsonify({"ok": False, "error": "DataBursatil no configurado"}), 503
    catalogo = cargar_catalogo_emisoras()
    return jsonify({"ok": True, "total": len(catalogo), "emisoras": list(catalogo.values())})


@app.route("/api/creditos/db")
def api_creditos_db():
    """Consulta créditos disponibles en DataBursatil."""
    if "usuario" not in session:
        return jsonify({"ok": False, "error": "No autenticado"}), 401
    if not DB_TOKEN:
        return jsonify({"ok": False, "error": "Token no configurado"}), 503
    try:
        r = requests.get(f"{DB_BASE}/creditos", params={"token": DB_TOKEN}, timeout=10)
        r.raise_for_status()
        return jsonify({"ok": True, "data": r.json()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    # Pre-cargar catálogo de emisoras al iniciar
    if DB_TOKEN:
        import threading
        threading.Thread(target=cargar_catalogo_emisoras, daemon=True).start()
    app.run(host="0.0.0.0", port=port, debug=False)
