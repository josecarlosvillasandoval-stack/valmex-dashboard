import os
import time
import requests
import yfinance as yf
from datetime import date, timedelta, datetime
from flask import Flask, send_file, request, jsonify, redirect, url_for, session, send_from_directory

BASE = os.path.dirname(os.path.abspath(__file__))

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "valmex-secret-2024")

# ─────────────────────────────────────────────────────────────────────────────
# USUARIOS
# ─────────────────────────────────────────────────────────────────────────────
USERS = {
    "jvilla": {"password": "valmex",   "nombre": "José Carlos Villa", "iniciales": "JV", "rol": "admin"},
    "admin":  {"password": "admin123", "nombre": "Administrador",      "iniciales": "AD", "rol": "admin"},
}

# ─────────────────────────────────────────────────────────────────────────────
# PERFILES MODELO — composiciones oficiales VALMEX
# ─────────────────────────────────────────────────────────────────────────────
PERFILES = {
    "0": {"VXGUBCP": 5.00,  "VXDEUDA": 90.00, "VXUDIMP": 5.00},
    "1": {"VXGUBCP": 25.00, "VXDEUDA": 12.00, "VXUDIMP": 7.00,  "VXGUBLP": 52.00, "VXTBILL": 4.00},
    "2": {"VXGUBCP": 26.93, "VXDEUDA": 18.00, "VXUDIMP": 5.83,  "VXGUBLP": 26.89, "VXTBILL": 2.35, "VALMX28": 17.00, "VALMX20": 3.00},
    "3": {"VXGUBCP": 20.40, "VXDEUDA": 5.76,  "VXUDIMP": 6.65,  "VXGUBLP": 25.11, "VXTBILL": 2.08, "VALMX28": 34.00, "VALMX20": 6.00},
    "4": {"VXGUBCP": 20.70, "VXDEUDA": 4.08,  "VXUDIMP": 5.37,  "VXGUBLP": 7.61,  "VXTBILL": 2.24, "VALMX28": 51.00, "VALMX20": 9.00},
}

# ─────────────────────────────────────────────────────────────────────────────
# CLASIFICACIÓN DE FONDOS
# ─────────────────────────────────────────────────────────────────────────────
FONDOS_DEUDA_MXN = {"VXREPO1", "VXGUBCP", "VXUDIMP", "VXDEUDA", "VXGUBLP", "VLMXETF"}
FONDOS_DEUDA_USD = {"VXTBILL", "VXCOBER", "VLMXDME"}
FONDOS_CRED_GLOBAL = {"VLMXETF"}  # Deuda MXN con calificación S&P USA (sin ajuste BBB): dur/ytm van a columna MX
SP_RATING_MXN = "BBB"  # S&P Global Ratings: México
SP_RATING_USD = "AA+"   # S&P Global Ratings: USA

# Calificaciones soberanas Fitch — actualizar cuando cambien
FONDOS_DEUDA     = FONDOS_DEUDA_MXN | FONDOS_DEUDA_USD
FONDOS_RV        = {"VALMXA", "VALMX20", "VALMX28", "VALMXVL", "VALMXES", "VLMXTEC", "VLMXESG", "VALMXHC", "VXINFRA"}
FONDOS_CICLO     = {"VLMXJUB", "VLMXP24", "VLMXP31", "VLMXP38", "VLMXP45", "VLMXP52", "VLMXP59"}

# ─────────────────────────────────────────────────────────────────────────────
# ESCALA CREDITICIA S&P — ponderación numérica global
# ─────────────────────────────────────────────────────────────────────────────
CREDIT_SCALE = ["AAA", "AA+", "AA", "AA-", "A+", "A", "A-", "BBB+", "BBB", "BBB-", "BB+", "BB", "BB-", "B+", "B", "B-", "<B", "NR"]
CREDIT_SCORE = {r: i for i, r in enumerate(CREDIT_SCALE)}

# Mapeo escala local México → escala global S&P
# Fuente: equivalencias estándar Moody's/S&P para emisores soberanos MX
MX_LOCAL_TO_GLOBAL = {
    "AAA": "BBB",
    "AA":  "BBB-",
    "A":   "BB+",
    "BBB": "BB",
    "BB":  "BB-",
    "B":   "B+",
    "<B":  "B",
    "NR":  "NR",
}

def weighted_credit_rating(cred_acc: dict, local_to_global: bool = False) -> str:
    """Calcula la calificación crediticia ponderada estilo S&P.
    Si local_to_global=True, convierte primero escala local MX a global.
    """
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

# ─────────────────────────────────────────────────────────────────────────────
# MAPA ISIN
# ─────────────────────────────────────────────────────────────────────────────
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

# ─────────────────────────────────────────────────────────────────────────────
# SERIES POR TIPO DE CLIENTE
# ─────────────────────────────────────────────────────────────────────────────
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

# Caché Morningstar
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
# ACCIONES & ETFs — Yahoo Finance
# ─────────────────────────────────────────────────────────────────────────────
_accion_cache: dict = {}
_accion_cache_ts: dict = {}
ACCION_CACHE_TTL = 3600  # 1 hora
ALPHA_VANTAGE_KEY = os.environ.get("ALPHA_VANTAGE_KEY", "YFUB8EA3CMFYEWKH")

# Mapeo país individual → región (para alinear con regiones Morningstar)
PAIS_A_REGION = {
    "Estados Unidos":  "Norteamérica",
    "Canadá":          "Norteamérica",
    "México":          "América Latina",
    "Brasil":          "América Latina",
    "Argentina":       "América Latina",
    "Chile":           "América Latina",
    "Colombia":        "América Latina",
    "Perú":            "América Latina",
    "Reino Unido":     "Europa ex-Euro",
    "Suiza":           "Europa ex-Euro",
    "Suecia":          "Europa ex-Euro",
    "Dinamarca":       "Europa ex-Euro",
    "Noruega":         "Europa ex-Euro",
    "Alemania":        "Eurozona",
    "Francia":         "Eurozona",
    "Países Bajos":    "Eurozona",
    "España":          "Eurozona",
    "Italia":          "Eurozona",
    "Finlandia":       "Eurozona",
    "Bélgica":         "Eurozona",
    "Portugal":        "Eurozona",
    "Irlanda":         "Eurozona",
    "Austria":         "Eurozona",
    "Japón":           "Japón",
    "Australia":       "Australasia",
    "Nueva Zelanda":   "Australasia",
    "Hong Kong":       "Asia Desarrollada",
    "Singapur":        "Asia Desarrollada",
    "Corea del Sur":   "Asia Desarrollada",
    "Taiwán":          "Asia Desarrollada",
    "China":           "Asia Emergente",
    "India":           "Asia Emergente",
    "Indonesia":       "Asia Emergente",
    "Tailandia":       "Asia Emergente",
    "Malasia":         "Asia Emergente",
    "Filipinas":       "Asia Emergente",
    "Vietnam":         "Asia Emergente",
    "Arabia Saudita":  "Medio Oriente",
    "Emiratos":        "Medio Oriente",
    "Israel":          "Medio Oriente",
    "Qatar":           "Medio Oriente",
    "Sudáfrica":       "África",
    "Egipto":          "África",
    "Nigeria":         "África",
    "Otros":           "Otros",
}

GEO_TRANSLATE_YF = {
    "united states": "Estados Unidos", "mexico": "México", "canada": "Canadá",
    "united kingdom": "Reino Unido", "germany": "Alemania", "france": "Francia",
    "japan": "Japón", "china": "China", "brazil": "Brasil", "india": "India",
    "south korea": "Corea del Sur", "taiwan": "Taiwán", "australia": "Australia",
    "netherlands": "Países Bajos", "switzerland": "Suiza", "spain": "España",
    "italy": "Italia", "hong kong": "Hong Kong", "singapore": "Singapur",
}

SEC_TRANSLATE_YF = {
    # Con espacios (formato info/longName)
    "technology":             "Tecnología",
    "financial services":     "Financiero",
    "healthcare":             "Salud",
    "consumer cyclical":      "Consumo Discrecional",
    "industrials":            "Industriales",
    "communication services": "Comunicaciones",
    "consumer defensive":     "Consumo Básico",
    "energy":                 "Energía",
    "basic materials":        "Materiales",
    "real estate":            "Bienes Raíces",
    "utilities":              "Utilidades",
    # Con guiones bajos (formato funds_data/sector_weightings)
    "technology":             "Tecnología",
    "financial_services":     "Financiero",
    "healthcare":             "Salud",
    "consumer_cyclical":      "Consumo Discrecional",
    "industrials":            "Industriales",
    "communication_services": "Comunicaciones",
    "consumer_defensive":     "Consumo Básico",
    "energy":                 "Energía",
    "basic_materials":        "Materiales",
    "real_estate":            "Bienes Raíces",
    "utilities":              "Utilidades",
    # Otros formatos posibles
    "realestate":             "Bienes Raíces",
    "consumercyclical":       "Consumo Discrecional",
    "consumerdefensive":      "Consumo Básico",
    "communicationservices":  "Comunicaciones",
    "basicmaterials":         "Materiales",
    "financialservices":      "Financiero",
}

# ── Yahoo Finance cookie/crumb cache ──
_yf_session   = None
_yf_crumb     = None
_yf_cookie_ts = 0

def _get_yf_session():
    """
    Obtiene sesión con cookies válidas de Yahoo Finance.
    Yahoo requiere: primero visitar finance.yahoo.com para obtener cookie,
    luego obtener crumb token, y usarlo en todas las peticiones.
    """
    global _yf_session, _yf_crumb, _yf_cookie_ts
    now = time.time()
    # Renovar cada 2 horas
    if _yf_session and _yf_crumb and (now - _yf_cookie_ts) < 7200:
        return _yf_session, _yf_crumb

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://finance.yahoo.com/",
    })

    try:
        # Paso 1: obtener cookies visitando Yahoo Finance
        r = session.get("https://finance.yahoo.com/", timeout=10)
        r.raise_for_status()

        # Paso 2: obtener crumb
        r2 = session.get(
            "https://query1.finance.yahoo.com/v1/test/csrfToken",
            headers={"Accept": "application/json"},
            timeout=10
        )
        crumb = None
        if r2.status_code == 200:
            try:
                crumb = r2.json().get("crumb")
            except Exception:
                pass

        # Fallback crumb endpoint
        if not crumb:
            r3 = session.get(
                "https://query2.finance.yahoo.com/v1/test/csrfToken",
                timeout=10
            )
            if r3.status_code == 200:
                try:
                    crumb = r3.json().get("crumb")
                except Exception:
                    pass

        _yf_session   = session
        _yf_crumb     = crumb or ""
        _yf_cookie_ts = now
        print(f"[YF SESSION] Cookie OK, crumb={'OK' if crumb else 'vacío'}")
        return _yf_session, _yf_crumb

    except Exception as e:
        print(f"[YF SESSION ERROR] {e}")
        _yf_session   = session  # usar igual aunque falle el crumb
        _yf_crumb     = ""
        _yf_cookie_ts = now
        return session, ""




# ── ETF holdings fallback — para cuando yfinance no retorna funds_data ──
# Datos aproximados de los ETFs más comunes (en % por región/sector)
_ETF_GEO_FALLBACK = {
    # ETFs de mercado global
    "ACWI.MX": {"Estados Unidos": 64.0, "Japón": 5.5, "Reino Unido": 3.8, "Francia": 3.2, "Canadá": 2.9, "Suiza": 2.5, "Alemania": 2.2, "Australia": 2.0, "Taiwán": 1.8, "India": 1.7, "Corea del Sur": 1.5, "Otros": 6.9},
    "ACWI":    {"Estados Unidos": 64.0, "Japón": 5.5, "Reino Unido": 3.8, "Francia": 3.2, "Canadá": 2.9, "Suiza": 2.5, "Alemania": 2.2, "Australia": 2.0, "Taiwán": 1.8, "India": 1.7, "Corea del Sur": 1.5, "Otros": 6.9},
    # ETFs USA
    "SPY.MX":  {"Estados Unidos": 100.0},
    "SPY":     {"Estados Unidos": 100.0},
    "IVV.MX":  {"Estados Unidos": 100.0},
    "VOO.MX":  {"Estados Unidos": 100.0},
    "QQQ.MX":  {"Estados Unidos": 100.0},
    "QQQ":     {"Estados Unidos": 100.0},
    "VTI.MX":  {"Estados Unidos": 100.0},
    # ETFs emergentes
    "EEM.MX":  {"China": 26.0, "India": 16.0, "Taiwán": 15.0, "Corea del Sur": 12.0, "Brasil": 5.5, "Arabia Saudita": 4.0, "Sudáfrica": 3.5, "Otros": 18.0},
    "VWO.MX":  {"China": 29.0, "India": 16.0, "Taiwán": 14.0, "Corea del Sur": 11.0, "Brasil": 5.0, "Arabia Saudita": 4.0, "Sudáfrica": 3.0, "Otros": 18.0},
    # ETFs Europa
    "EFA.MX":  {"Japón": 22.0, "Reino Unido": 14.5, "Francia": 11.5, "Suiza": 10.0, "Alemania": 9.0, "Australia": 7.5, "Países Bajos": 4.5, "Suecia": 3.5, "Hong Kong": 3.5, "Otros": 14.0},
    "IEFA.MX": {"Japón": 22.0, "Reino Unido": 14.0, "Francia": 11.0, "Suiza": 10.0, "Alemania": 9.0, "Australia": 7.5, "Otros": 26.5},
}

_ETF_SEC_FALLBACK = {
    "ACWI.MX": {"Tecnología": 24.0, "Financiero": 15.0, "Salud": 11.0, "Industriales": 10.0, "Consumo Discrecional": 10.0, "Comunicaciones": 8.0, "Consumo Básico": 6.5, "Energía": 4.5, "Materiales": 4.0, "Bienes Raíces": 2.5, "Utilidades": 2.5, "Otros": 1.5},
    "ACWI":    {"Tecnología": 24.0, "Financiero": 15.0, "Salud": 11.0, "Industriales": 10.0, "Consumo Discrecional": 10.0, "Comunicaciones": 8.0, "Consumo Básico": 6.5, "Energía": 4.5, "Materiales": 4.0, "Bienes Raíces": 2.5, "Utilidades": 2.5, "Otros": 1.5},
    "SPY.MX":  {"Tecnología": 32.5, "Financiero": 13.0, "Salud": 12.5, "Consumo Discrecional": 10.5, "Comunicaciones": 8.5, "Industriales": 8.0, "Consumo Básico": 5.5, "Energía": 3.5, "Materiales": 2.5, "Bienes Raíces": 2.0, "Utilidades": 2.5, "Otros": 1.0},
    "SPY":     {"Tecnología": 32.5, "Financiero": 13.0, "Salud": 12.5, "Consumo Discrecional": 10.5, "Comunicaciones": 8.5, "Industriales": 8.0, "Consumo Básico": 5.5, "Energía": 3.5, "Materiales": 2.5, "Bienes Raíces": 2.0, "Utilidades": 2.5, "Otros": 1.0},
    "IVV.MX":  {"Tecnología": 32.5, "Financiero": 13.0, "Salud": 12.5, "Consumo Discrecional": 10.5, "Comunicaciones": 8.5, "Industriales": 8.0, "Consumo Básico": 5.5, "Energía": 3.5, "Materiales": 2.5, "Bienes Raíces": 2.0, "Utilidades": 2.5},
    "VOO.MX":  {"Tecnología": 32.5, "Financiero": 13.0, "Salud": 12.5, "Consumo Discrecional": 10.5, "Comunicaciones": 8.5, "Industriales": 8.0, "Consumo Básico": 5.5, "Energía": 3.5, "Materiales": 2.5, "Bienes Raíces": 2.0, "Utilidades": 2.5},
    "QQQ.MX":  {"Tecnología": 51.0, "Comunicaciones": 16.0, "Consumo Discrecional": 14.0, "Salud": 6.0, "Industriales": 5.0, "Financiero": 4.0, "Consumo Básico": 2.5, "Otros": 1.5},
    "QQQ":     {"Tecnología": 51.0, "Comunicaciones": 16.0, "Consumo Discrecional": 14.0, "Salud": 6.0, "Industriales": 5.0, "Financiero": 4.0, "Consumo Básico": 2.5, "Otros": 1.5},
    "VTI.MX":  {"Tecnología": 30.0, "Financiero": 13.5, "Salud": 12.5, "Industriales": 13.0, "Consumo Discrecional": 9.5, "Comunicaciones": 8.5, "Consumo Básico": 5.0, "Energía": 3.5, "Materiales": 2.5, "Bienes Raíces": 3.5, "Utilidades": 2.5},
    "EEM.MX":  {"Tecnología": 22.0, "Financiero": 21.0, "Consumo Discrecional": 14.0, "Comunicaciones": 10.0, "Materiales": 8.0, "Industriales": 7.0, "Energía": 5.0, "Salud": 4.0, "Consumo Básico": 4.0, "Otros": 5.0},
    "VWO.MX":  {"Financiero": 22.0, "Tecnología": 21.0, "Consumo Discrecional": 13.0, "Comunicaciones": 10.0, "Materiales": 8.0, "Industriales": 7.0, "Energía": 5.0, "Salud": 4.0, "Otros": 10.0},
    "EFA.MX":  {"Financiero": 19.5, "Industriales": 16.0, "Salud": 13.0, "Consumo Discrecional": 11.0, "Tecnología": 9.5, "Consumo Básico": 9.0, "Materiales": 7.0, "Comunicaciones": 5.0, "Energía": 4.5, "Bienes Raíces": 2.5, "Utilidades": 3.0},
    "IEFA.MX": {"Financiero": 19.0, "Industriales": 16.0, "Salud": 13.0, "Consumo Discrecional": 11.5, "Tecnología": 10.0, "Consumo Básico": 9.0, "Materiales": 7.0, "Comunicaciones": 5.0, "Energía": 4.0, "Otros": 5.5},
}
# ── Limpieza de nombres de ETFs e índices ──
# Yahoo Finance devuelve nombres como "iShares MSCI ACWI ETF" o "Invesco QQQ Trust"
# Queremos solo el índice/estrategia que representa, sin la gestora
def limpiar_nombre_instrumento(nombre: str, ticker: str, quote_type: str) -> str:
    """
    Limpia el nombre de un instrumento eliminando prefijos de gestora
    y simplificando a lo que realmente representa.
    """
    if not nombre:
        return ticker

    # Mapa de tickers conocidos → nombre descriptivo en español
    NOMBRES_CONOCIDOS = {
        # ETFs globales
        "ACWI":   "MSCI ACWI Global",
        "VT":     "Total World Market",
        "VTI":    "Total Stock Market EE.UU.",
        "VOO":    "S&P 500",
        "IVV":    "S&P 500",
        "SPY":    "S&P 500",
        "QQQ":    "Nasdaq 100",
        "QQEW":   "Nasdaq 100 Eq. Ponderado",
        "EFA":    "MSCI EAFE Desarrollados",
        "IEFA":   "MSCI EAFE Core",
        "EEM":    "MSCI Emergentes",
        "VWO":    "MSCI Emergentes Vanguard",
        "IEMG":   "MSCI Emergentes Core",
        # ETFs sectoriales
        "XLK":    "S&P 500 Tecnología",
        "XLF":    "S&P 500 Financiero",
        "XLV":    "S&P 500 Salud",
        "XLE":    "S&P 500 Energía",
        "XLI":    "S&P 500 Industriales",
        "XLY":    "S&P 500 Consumo Discrecional",
        "XLP":    "S&P 500 Consumo Básico",
        "XLB":    "S&P 500 Materiales",
        "XLU":    "S&P 500 Utilidades",
        "XLRE":   "S&P 500 Bienes Raíces",
        "XLC":    "S&P 500 Comunicaciones",
        # ETFs renta fija
        "AGG":    "Bloomberg Aggregate Bonds - EE.UU.",
        "BND":    "Vanguard Total Bond Market - EE.UU.",
        "TLT":    "Bonos del Tesoro EE.UU. 20+ años",
        "IEF":    "Bonos del Tesoro EE.UU. 7-10 años",
        "SHY":    "Bonos del Tesoro EE.UU. 1-3 años",
        "LQD":    "Bonos Corporativos Grado Inversión EE.UU.",
        "HYG":    "Bonos High Yield EE.UU.",
        "EMB":    "Bonos de Mercados Emergentes",
        # Acciones conocidas (se pueden agregar más)
        "AAPL":   "Apple",
        "MSFT":   "Microsoft",
        "GOOGL":  "Alphabet (Google)",
        "AMZN":   "Amazon",
        "NVDA":   "Nvidia",
        "META":   "Meta (Facebook)",
        "TSLA":   "Tesla",
        "WALMEX": "Walmart México",
        "GFNORTE":"GFNorte - Grupo Financiero Banorte",
        "CEMEX":  "CEMEX",
        "AMXL":   "América Móvil",
        "FEMSAUBD":"FEMSA",
        "BIMBOA": "Grupo Bimbo",
    }

    ticker_base = ticker.replace(".MX", "").upper()
    if ticker_base in NOMBRES_CONOCIDOS:
        return NOMBRES_CONOCIDOS[ticker_base]

    # Si no está en el mapa, limpiar prefijos de gestoras
    PREFIJOS_GESTORAS = [
        "iShares MSCI ", "iShares Core MSCI ", "iShares Core ", "iShares ",
        "Invesco ", "Vanguard ", "SPDR ", "SPDR S&P 500 ",
        "Fidelity ", "Schwab ", "WisdomTree ", "ProShares ",
        "First Trust ", "VanEck ", "ARK ", "Global X ",
        "Direxion ", "Xtrackers ", "Franklin ",
    ]
    nombre_limpio = nombre
    for prefijo in PREFIJOS_GESTORAS:
        if nombre_limpio.startswith(prefijo):
            nombre_limpio = nombre_limpio[len(prefijo):]
            break

    # Quitar sufijos redundantes
    SUFIJOS = [" ETF", " Index Fund", " Fund", " Trust", " Portfolio"]
    for suf in SUFIJOS:
        if nombre_limpio.endswith(suf):
            nombre_limpio = nombre_limpio[:-len(suf)]

    return nombre_limpio or nombre



def get_accion_yf(ticker: str) -> dict | None:
    """
    Obtiene datos de una acción/ETF via Yahoo Finance (yfinance).
    yfinance maneja cookies y crumb automáticamente.
    Tickers .MX = SIC/BMV/BIVA en pesos mexicanos.
    """
    now = time.time()
    if ticker in _accion_cache and (now - _accion_cache_ts.get(ticker, 0)) < ACCION_CACHE_TTL:
        return _accion_cache[ticker]

    try:
        t = yf.Ticker(ticker)

        # Historial de precios — 3 años diarios ajustados
        hist = t.history(period="3y", auto_adjust=True)
        if hist.empty:
            hist = t.history(period="1y", auto_adjust=True)
        if hist.empty:
            print(f"[YF] {ticker}: historial vacío")
            return None

        # Info del instrumento
        try:
            info = t.info or {}
        except Exception:
            info = {}

        today  = datetime.now().date()
        prices = hist["Close"]
        idx    = prices.index

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
        pais       = GEO_TRANSLATE_YF.get(pais_en, info.get("country") or "Estados Unidos")
        nombre_raw = info.get("shortName") or info.get("longName") or ticker
        nombre     = limpiar_nombre_instrumento(nombre_raw, ticker, quote_type)
        moneda     = "MXN" if ticker.endswith(".MX") else "USD"

        # Traducciones de países para ETFs
        # Traducir países de Yahoo Finance a español (luego se mapean a región)
        GEO_PAISES_ETF = {
            "united states": "Estados Unidos", "japan": "Japón",
            "united kingdom": "Reino Unido", "canada": "Canadá",
            "france": "Francia", "germany": "Alemania", "china": "China",
            "switzerland": "Suiza", "australia": "Australia", "india": "India",
            "taiwan": "Taiwán", "south korea": "Corea del Sur",
            "netherlands": "Países Bajos", "sweden": "Suecia",
            "denmark": "Dinamarca", "hong kong": "Hong Kong",
            "singapore": "Singapur", "brazil": "Brasil", "mexico": "México",
            "spain": "España", "italy": "Italia", "south africa": "Sudáfrica",
            "saudi arabia": "Arabia Saudita", "others": "Otros",
            "new zealand": "Nueva Zelanda", "norway": "Noruega",
            "finland": "Finlandia", "belgium": "Bélgica", "ireland": "Irlanda",
            "austria": "Austria", "portugal": "Portugal",
            "indonesia": "Indonesia", "thailand": "Tailandia",
            "malaysia": "Malasia", "philippines": "Filipinas",
            "vietnam": "Vietnam", "israel": "Israel",
            "argentina": "Argentina", "chile": "Chile",
            "colombia": "Colombia", "peru": "Perú",
            "egypt": "Egipto", "nigeria": "Nigeria",
            "qatar": "Qatar", "uae": "Emiratos",
            "united arab emirates": "Emiratos",
            "north america": "Norteamérica",
            "eurozone": "Eurozona",
            "europe": "Europa ex-Euro",
            "latin america": "América Latina",
            "asia emerging": "Asia Emergente",
            "asia developed": "Asia Desarrollada",
            "asia": "Asia Emergente",
            "emerging markets": "Asia Emergente",
        }

        sectores_etf = {}
        geo_etf      = {}

        if quote_type == "ETF":
            try:
                holdings = t.funds_data
                if holdings and hasattr(holdings, "sector_weightings"):
                    sw = holdings.sector_weightings or {}
                    if hasattr(sw, 'items'):
                        for s, v in sw.items():
                            # Normalizar clave: guiones bajos → espacio, lowercase
                            s_norm = s.lower().replace('_', ' ').strip()
                            s_raw  = s.lower().replace(' ', '_').strip()
                            lbl = SEC_TRANSLATE_YF.get(s_norm) or SEC_TRANSLATE_YF.get(s_raw) or SEC_TRANSLATE_YF.get(s.lower(), s_norm)
                            try:
                                val = float(v)
                                if val > 0:
                                    # Consolidar en el mismo label (no duplicar)
                                    sectores_etf[lbl] = sectores_etf.get(lbl, 0) + round(val * 100 if val <= 1 else val, 2)
                            except Exception:
                                pass

                if holdings and hasattr(holdings, "country_weightings"):
                    cw = holdings.country_weightings
                    def _add_geo(pais_key, v):
                        p_norm = str(pais_key).lower().replace('_', ' ').strip()
                        # Traducir a español
                        pais_es = GEO_PAISES_ETF.get(p_norm, str(pais_key))
                        # Agrupar en región del sistema
                        region = PAIS_A_REGION.get(pais_es, pais_es)
                        try:
                            val = float(v)
                            val = val * 100 if val <= 1 else val
                            if val > 0.1:
                                geo_etf[region] = geo_etf.get(region, 0) + round(val, 2)
                        except Exception:
                            pass

                    if hasattr(cw, 'items'):
                        for p_en, v in cw.items():
                            _add_geo(p_en, v)
                    elif hasattr(cw, 'to_dict'):
                        for p_en, v in cw.to_dict().items():
                            _add_geo(p_en, v)
            except Exception as ex:
                print(f"[YF ETF holdings] {ticker}: {ex}")

        # Fallback geo para ETFs conocidos
        ticker_base = ticker.replace(".MX", "").upper()
        if quote_type == "ETF" and not geo_etf:
            ETF_GEO_FALLBACK = {
                "ACWI":  {"Norteamérica": 66.9, "Eurozona": 9.8, "Europa ex-Euro": 7.2, "Japón": 5.5, "Asia Desarrollada": 4.8, "Asia Emergente": 2.9, "América Latina": 0.8, "Otros": 2.1},
                "VT":    {"Norteamérica": 66.0, "Eurozona": 9.5, "Europa ex-Euro": 7.0, "Japón": 5.5, "Asia Desarrollada": 4.5, "Asia Emergente": 3.5, "América Latina": 1.5, "Otros": 2.5},
                "SPY":   {"Norteamérica": 100.0},
                "IVV":   {"Norteamérica": 100.0},
                "VOO":   {"Norteamérica": 100.0},
                "QQQ":   {"Norteamérica": 100.0},
                "VTI":   {"Norteamérica": 100.0},
                "EEM":   {"Asia Emergente": 57.0, "Asia Desarrollada": 12.0, "América Latina": 5.5, "Medio Oriente": 4.0, "África": 3.5, "Otros": 18.0},
                "VWO":   {"Asia Emergente": 60.0, "Asia Desarrollada": 11.0, "América Latina": 5.0, "Medio Oriente": 4.0, "África": 3.0, "Otros": 17.0},
                "IEMG":  {"Asia Emergente": 58.0, "Asia Desarrollada": 12.0, "América Latina": 5.5, "Medio Oriente": 4.0, "África": 3.5, "Otros": 17.0},
                "EFA":   {"Japón": 22.0, "Europa ex-Euro": 18.0, "Eurozona": 18.5, "Australasia": 7.5, "Asia Desarrollada": 4.0, "Norteamérica": 0.0, "Otros": 30.0},
                "IEFA":  {"Japón": 22.0, "Europa ex-Euro": 18.0, "Eurozona": 18.0, "Australasia": 7.5, "Asia Desarrollada": 4.0, "Otros": 30.5},
                "VEA":   {"Japón": 22.0, "Europa ex-Euro": 17.0, "Eurozona": 17.5, "Australasia": 7.5, "Asia Desarrollada": 4.0, "Otros": 32.0},
            }
            geo_etf = ETF_GEO_FALLBACK.get(ticker_base, ETF_GEO_FALLBACK.get(ticker, {}))

        # Fallback sectores para ETFs conocidos
        if quote_type == "ETF" and not sectores_etf:
            ETF_SEC_FALLBACK = {
                "ACWI": {"Tecnología": 24.0, "Financiero": 15.0, "Salud": 11.0, "Industriales": 10.0, "Consumo Discrecional": 10.0, "Comunicaciones": 8.0, "Consumo Básico": 6.5, "Energía": 4.5, "Materiales": 4.0, "Bienes Raíces": 2.5, "Utilidades": 2.5},
                "SPY":  {"Tecnología": 32.5, "Financiero": 13.0, "Salud": 12.5, "Consumo Discrecional": 10.5, "Comunicaciones": 8.5, "Industriales": 8.0, "Consumo Básico": 5.5, "Energía": 3.5, "Materiales": 2.5, "Bienes Raíces": 2.0, "Utilidades": 2.5},
                "IVV":  {"Tecnología": 32.5, "Financiero": 13.0, "Salud": 12.5, "Consumo Discrecional": 10.5, "Comunicaciones": 8.5, "Industriales": 8.0, "Consumo Básico": 5.5, "Energía": 3.5, "Materiales": 2.5, "Bienes Raíces": 2.0, "Utilidades": 2.5},
                "VOO":  {"Tecnología": 32.5, "Financiero": 13.0, "Salud": 12.5, "Consumo Discrecional": 10.5, "Comunicaciones": 8.5, "Industriales": 8.0, "Consumo Básico": 5.5, "Energía": 3.5, "Materiales": 2.5, "Bienes Raíces": 2.0, "Utilidades": 2.5},
                "QQQ":  {"Tecnología": 51.0, "Comunicaciones": 16.0, "Consumo Discrecional": 14.0, "Salud": 6.0, "Industriales": 5.0, "Financiero": 4.0, "Consumo Básico": 2.5},
                "VTI":  {"Tecnología": 30.0, "Financiero": 13.5, "Salud": 12.5, "Industriales": 13.0, "Consumo Discrecional": 9.5, "Comunicaciones": 8.5, "Consumo Básico": 5.0, "Energía": 3.5, "Materiales": 2.5, "Bienes Raíces": 3.5, "Utilidades": 2.5},
                "EEM":  {"Tecnología": 22.0, "Financiero": 21.0, "Consumo Discrecional": 14.0, "Comunicaciones": 10.0, "Materiales": 8.0, "Industriales": 7.0, "Energía": 5.0, "Salud": 4.0, "Consumo Básico": 4.0},
                "VWO":  {"Financiero": 22.0, "Tecnología": 21.0, "Consumo Discrecional": 13.0, "Comunicaciones": 10.0, "Materiales": 8.0, "Industriales": 7.0, "Energía": 5.0, "Salud": 4.0, "Consumo Básico": 4.0},
                "EFA":  {"Financiero": 20.0, "Industriales": 16.0, "Salud": 13.0, "Consumo Básico": 12.0, "Tecnología": 10.0, "Consumo Discrecional": 9.0, "Materiales": 7.5, "Energía": 5.0, "Comunicaciones": 4.5, "Utilidades": 3.0},
            }
            sectores_etf = ETF_SEC_FALLBACK.get(ticker_base, ETF_SEC_FALLBACK.get(ticker, {}))

        # Geo para acciones individuales
        if not geo_etf and pais:
            geo_etf = {pais: 100.0}

        # Agrupar geo de acciones individuales en región
        if not (quote_type == "ETF"):
            geo_por_region = {}
            for pais_label, v in geo_etf.items():
                region = PAIS_A_REGION.get(pais_label, pais_label)
                geo_por_region[region] = geo_por_region.get(region, 0) + v
            geo_etf = geo_por_region

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
        print(f"[YF OK] {ticker}: {nombre} | p={p_hoy:.2f}")
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

    # Acumuladores separados MXN / USD
    dur_mxn_num = ytm_mxn_num = bond_mxn_denom = 0.0
    dur_usd_num = ytm_usd_num = bond_usd_denom = 0.0
    cred_mxn = {}; cred_usd = {}

    # Backtesting reporto: {fecha: valor_acum_ponderado}
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
        # Clasificación del fondo
        is_usd       = fondo in FONDOS_DEUDA_USD
        is_deuda_mxn = fondo in FONDOS_DEUDA_MXN
        is_deuda     = fondo in FONDOS_DEUDA
        is_rv        = fondo in FONDOS_RV
        is_ciclo     = fondo in FONDOS_CICLO

        # Clase de activos: stock_t solo acumula fondos RV/ciclo
        # Fondos de deuda como VLMXDME pueden traer AAB-StockNet>0 de Morningstar — ignorar
        if is_rv or is_ciclo:
            stock_t += stock * w
        bond_t  += bond  * w
        cash_t  += cash  * w

        # ── Drilldown deuda: solo fondos de deuda MXN/USD (no RV puro) ──
        # Ciclo de vida participa en drilldown MXN
        if (is_deuda or is_ciclo) and bond > 0:
            bond_w = (bond / 100.0) * w   # para calificación crediticia (por tramo de deuda)
            if bond_w > 0:
                dur_val = safe_float(d.get("PS-EffectiveDuration"))
                ytm_val = safe_float(d.get("PS-YieldToMaturity"))
                # Duración y YTM se ponderan por peso en portafolio (w), no por tramo de deuda
                if is_usd:
                    dur_usd_num    += dur_val * w
                    ytm_usd_num    += ytm_val * w
                    bond_usd_denom += w
                else:
                    dur_mxn_num    += dur_val * w
                    ytm_mxn_num    += ytm_val * w
                    bond_mxn_denom += w

                # Calificación crediticia: CQB-* ya es % del fondo completo → ponderar por w
                # Fondos MXN: Morningstar usa escala local MX → ajuste Valmex: todo entra como BBB
                # Fondos USD: ya en escala global → se usa tal cual
                # Calificación crediticia
                # Fondos/Reporto USD (incl. VLMXETF, VLMXDME): AA+ (Fitch USA)
                # Fondos MXN: ajuste Valmex → BBB
                if fondo in FONDOS_CRED_GLOBAL:
                    # MXN pero calificación S&P USA → acumula en cred_mxn (columna MX)
                    cred_mxn[SP_RATING_USD] = cred_mxn.get(SP_RATING_USD, 0) + 100 * w
                elif is_usd:
                    cred_usd[SP_RATING_USD] = cred_usd.get(SP_RATING_USD, 0) + 100 * w
                else:
                    cred_mxn[SP_RATING_MXN] = cred_mxn.get(SP_RATING_MXN, 0) + 100 * w

                # Super-sectores: son % del fondo completo (misma escala que AAB-BondNet/CashNet)
                # Se ponderan por w (peso en portafolio), NO por bond_w
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

        # ── Geo y sectores: fondos RV puros + Ciclo de Vida ──
        if (is_rv or is_ciclo) and stock > 0:
            geo_raw = d.get("RE-RegionalExposure", [])
            GEO_EXCLUDE = {"emerging market", "developed country", "emerging markets", "developed countries"}
            if isinstance(geo_raw, list):
                for item in geo_raw:
                    region = item.get("Region", "")
                    val    = safe_float(item.get("Value", 0))
                    if region and val > 0 and region.lower() not in GEO_EXCLUDE:
                        geo_acc[region] = geo_acc.get(region, 0) + val * (stock * w / 100)

            sector_map = {
                "GR-TechnologyNet":           "Technology",
                "GR-FinancialServicesNet":    "Financial Services",
                "GR-HealthcareNet":           "Healthcare",
                "GR-CommunicationServicesNet":"Communication Services",
                "GR-IndustrialsNet":          "Industrials",
                "GR-ConsumerCyclicalNet":     "Consumer Cyclical",
                "GR-ConsumerDefensiveNet":    "Consumer Defensive",
                "GR-BasicMaterialsNet":       "Basic Materials",
                "GR-EnergyNet":               "Energy",
                "GR-RealEstateNet":           "Real Estate",
                "GR-UtilitiesNet":            "Utilities",
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

    # ── Reporto directo (pseudo-fondo sintético) ──
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

        # Rendimientos históricos reales basados en tasa de referencia
        rend = get_repo_rendimientos(tasa, es_usd)

        r1m += rend["r1m"] * w
        r3m += rend["r3m"] * w
        r6m += rend["r6m"] * w
        ytd += rend["ytd"] * w
        r1y += rend["r1y"] * w
        r2y += rend["r2y"] * w
        r3y += rend["r3y"] * w

        # Acumular backtesting ponderado
        for pt in rend.get("backtesting", []):
            f = pt["fecha"]
            bt_repo[f] = bt_repo.get(f, 0.0) + pt["valor"] * w

        # Clase activos: suma a Efectivo (equivalente a AAB-CashNet=100 de los fondos)
        cash_t += 100.0 * w
        # Drilldown deuda: dur=0 (overnight), ytm=tasa neta
        bond_w = w
        if es_usd:
            dur_usd_num    += 0.0  * w
            ytm_usd_num    += tasa * w
            bond_usd_denom += w
            cred_usd[SP_RATING_USD] = cred_usd.get(SP_RATING_USD, 0) + 100 * w   # S&P Global: USA
        else:
            dur_mxn_num    += 0.0  * w
            ytm_mxn_num    += tasa * w
            bond_mxn_denom += w
            cred_mxn[SP_RATING_MXN] = cred_mxn.get(SP_RATING_MXN, 0) + 100 * w   # S&P Global: México
        supersec_acc["Reporto"] = supersec_acc.get("Reporto", 0) + 100 * bond_w
        lista.append({
            "fondo": label_corto, "serie": "—", "pct": round(pct, 2),
            "r1m": round(rend["r1m"], 2),
            "r3m": round(rend["r3m"], 2),
            "r1y": round(rend["r1y"], 2),
            "r3y": round(rend["r3y"], 2),
        })

    def top_n(d, n=8):
        t = sum(d.values()) or 1
        items = sorted(d.items(), key=lambda x: -x[1])[:n]
        return {"labels":[i[0] for i in items],"values":[round(i[1]/t*100,2) for i in items]}

    GEO_TRANSLATE = {
        "united states":     "Estados Unidos",
        "canada":            "Canadá",
        "latin america":     "América Latina",
        "united kingdom":    "Reino Unido",
        "eurozone":          "Eurozona",
        "europe - ex euro":  "Europa ex-Euro",
        "europe - emerging": "Europa Emergente",
        "africa":            "África",
        "middle east":       "Medio Oriente",
        "japan":             "Japón",
        "australasia":       "Australasia",
        "asia - developed":  "Asia Desarrollada",
        "asia - emerging":   "Asia Emergente",
        "greater asia":      "Gran Asia",
        "greater europe":    "Gran Europa",
        "americas":          "Américas",
        "north america":     "Norteamérica",
    }

    SEC_TRANSLATE = {
        "technology":             "Tecnología",
        "financial services":     "Financiero",
        "healthcare":             "Salud",
        "communication services": "Comunicaciones",
        "industrials":            "Industriales",
        "consumer cyclical":      "Consumo Discrecional",
        "consumer defensive":     "Consumo Básico",
        "basic materials":        "Materiales",
        "energy":                 "Energía",
        "real estate":            "Bienes Raíces",
        "utilities":              "Utilidades",
    }

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

        # Rendimientos ponderados
        r1m += (yfd.get("r1m") or 0) * w
        r3m += (yfd.get("r3m") or 0) * w
        ytd += (yfd.get("ytd") or 0) * w
        r1y += (yfd.get("r1y") or 0) * w
        r2y += (yfd.get("r2y") or 0) * w
        r3y += (yfd.get("r3y") or 0) * w

        # Clase de activos: 100% RV
        stock_t += 100 * w

        # Composición
        lista.append({
            "fondo": ticker,
            "serie": yfd.get("tipo", "Acción"),
            "pct":   round(pct, 2),
            "r1m":   round(yfd.get("r1m") or 0, 2),
            "r3m":   round(yfd.get("r3m") or 0, 2),
            "r1y":   round(yfd.get("r1y") or 0, 2),
            "r3y":   round(yfd.get("r3y") or 0, 2),
        })

        # Sectores: ETF usa su propio desglose, Acción usa su sector
        if yfd.get("sectores"):
            for s, v in yfd["sectores"].items():
                sec_acc[s] = sec_acc.get(s, 0) + v * w
        elif yfd.get("sector"):
            sec_acc[yfd["sector"]] = sec_acc.get(yfd["sector"], 0) + 100 * w

        # Geo: acción/ETF → agrupar en regiones del sistema (igual que Morningstar)
        if yfd.get("geo"):
            for g, v in yfd["geo"].items():
                region = PAIS_A_REGION.get(g, g)  # mapear país → región
                geo_acc[region] = geo_acc.get(region, 0) + v * w
        elif yfd.get("pais"):
            region = PAIS_A_REGION.get(yfd["pais"], yfd["pais"])
            geo_acc[region] = geo_acc.get(region, 0) + 100 * w

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
        "sectores":      filter_pct(sec_acc, translate=SEC_TRANSLATE),
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
    """Valida un ticker de Yahoo Finance y retorna sus datos básicos."""
    if "usuario" not in session:
        return jsonify({"ok": False, "error": "No autenticado"}), 401
    body   = request.get_json(force=True)
    ticker = (body.get("ticker") or "").strip().upper()
    if not ticker:
        return jsonify({"ok": False, "error": "Ticker vacío"}), 400
    # Siempre buscar en MXN — agregar .MX si no lo tiene
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
        # Permitir portafolio solo con reporto o acciones (sin fondos Valmex)
        acciones_raw = body.get("acciones", [])
        if not fondos_pct and not repo_mxn and not repo_usd and not acciones_raw:
            return jsonify({"ok": False, "error": "Sin fondos con % > 0"}), 400

    return jsonify(calcular_portafolio(fondos_pct, tipo_cliente,
                                        repo_mxn=body.get("repo_mxn"),
                                        repo_usd=body.get("repo_usd"),
                                        acciones=body.get("acciones", [])))


# ─────────────────────────────────────────────────────────────────────────────
# MACRO — Banxico SIE API
# ─────────────────────────────────────────────────────────────────────────────
BANXICO_TOKEN = os.environ.get("BANXICO_TOKEN", "")  # Configura tu token en variable de entorno
BANXICO_BASE  = "https://www.banxico.org.mx/SieAPIRest/service/v1/series"
FRED_API_KEY  = os.environ.get("FRED_API_KEY", "")
FRED_BASE     = "https://api.stlouisfed.org/fred/series/observations"

# IDs de series
SERIE_TIIE28  = "SF43783"   # TIIE 28 días
SERIE_USDMXN  = "SF43718"   # USD/MXN FIX
SERIE_CETES28 = "SF60633"   # Cetes 28 días (proxy T-Bill MX)
SERIE_FONDEO  = "SF43936"   # Fondeo bancario overnight MXN (diario) ← backtesting MXN
SERIE_USD_REPO = "SOFR"     # SOFR overnight USD (FRED) ← backtesting USD

_macro_cache = {}
_macro_ts    = 0

# ── Caché de tasas históricas ──
_hist_cache    = {}
_hist_cache_ts = 0


def _banxico_serie_rango(serie_id: str, fecha_ini: str, fecha_fin: str) -> list[dict]:
    """Descarga serie Banxico en rango yyyy-mm-dd. Retorna lista de {fecha, valor}."""
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


def _fred_serie_rango(series_id: str, fecha_ini: str, fecha_fin: str) -> list[dict]:
    """Descarga serie FRED en rango. Retorna lista de {fecha, valor}."""
    try:
        params = {
            "series_id":       series_id,
            "observation_start": fecha_ini,
            "observation_end":   fecha_fin,
            "api_key":         FRED_API_KEY,
            "file_type":       "json",
            "frequency":       "m",         # mensual
            "aggregation_method": "avg",
        }
        r = requests.get(FRED_BASE, params=params, timeout=15)
        r.raise_for_status()
        obs = r.json().get("observations", [])
        result = []
        for o in obs:
            try:
                if o["value"] != ".":
                    result.append({"fecha": o["date"], "valor": float(o["value"])})
            except Exception:
                pass
        return result
    except Exception as e:
        print(f"[FRED ERROR] {series_id}: {e}")
        return []


def _promedio_serie(datos: list[dict], fecha_desde: date) -> float | None:
    """Promedio de valores desde fecha_desde hasta hoy."""
    vals = [d["valor"] for d in datos
            if _parse_fecha(d["fecha"]) and _parse_fecha(d["fecha"]) >= fecha_desde]
    return sum(vals) / len(vals) if vals else None


def _parse_fecha(s: str) -> date | None:
    """Parsea fecha en formato dd/mm/yyyy o yyyy-mm-dd."""
    try:
        if "/" in s:
            d, m, y = s.split("/")
            return date(int(y), int(m), int(d))
        return date.fromisoformat(s[:10])
    except Exception:
        return None


def _get_datos_hist(es_usd: bool) -> list:
    """
    Descarga y cachea serie histórica de tasa overnight desde 2000.
    MXN: SF43936 (Fondeo bancario overnight, Banxico)
    USD: SOFR → fallback DFF (Fed Funds, FRED)
    Cache de 4 horas.
    """
    global _hist_cache, _hist_cache_ts
    cache_key = "usd" if es_usd else "mxn"
    now = time.time()

    if cache_key in _hist_cache and (now - _hist_cache_ts) < 14400:
        return _hist_cache[cache_key]

    hoy  = date.today()
    ini  = "2000-01-01"          # desde año 2000 para backtesting completo
    fin  = hoy.isoformat()

    if es_usd:
        # Intentar SOFR primero (disponible desde 2018), luego DFF (desde 1954)
        datos = []
        for serie in [SERIE_USD_REPO, "DFF"]:
            try:
                params = {
                    "series_id":        serie,
                    "observation_start": ini,
                    "observation_end":   fin,
                    "api_key":           FRED_API_KEY,
                    "file_type":         "json",
                }
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
        # Fondeo bancario overnight diario Banxico (SF43936)
        raw   = _banxico_serie_rango(SERIE_FONDEO, ini, fin)
        datos = [{"fecha": _parse_fecha(d["fecha"]), "valor": d["valor"]}
                 for d in raw if _parse_fecha(d["fecha"])]

    datos = sorted([d for d in datos if d["fecha"] is not None], key=lambda x: x["fecha"])
    _hist_cache[cache_key]  = datos
    _hist_cache_ts          = now
    print(f"[HIST {'USD' if es_usd else 'MXN'}] {len(datos)} registros desde {datos[0]['fecha'] if datos else 'N/A'}")
    return datos


def get_repo_rendimientos(tasa_neta: float, es_usd: bool) -> dict:
    """
    Rendimientos históricos por composición diaria overnight.
    - MTD, 3M, 6M, YTD  → efectivos (< 1 año)
    - 12M, 24M, 36M      → anualizados  (1 + r)^(1/n) - 1
    spread = tasa_ref_hoy - tasa_neta_cliente  (constante en el tiempo)
    """
    datos = _get_datos_hist(es_usd)

    if not datos:
        # Fallback si no hay conectividad
        anual = tasa_neta
        return {
            "r1m":  round(anual / 12, 2),
            "r3m":  round(anual / 4,  2),
            "r6m":  round(anual / 2,  2),
            "ytd":  round(anual / 12, 2),
            "r1y":  round(anual,      2),
            "r2y":  round(anual,      2),
            "r3y":  round(anual,      2),
            "backtesting": [],
        }

    hoy          = date.today()
    tasa_ref_hoy = datos[-1]["valor"]
    spread       = tasa_ref_hoy - tasa_neta

    def componer_acum(desde: date) -> float:
        """Retorna rendimiento acumulado decimal (no %)."""
        acum    = 1.0
        ultimo  = None
        rango   = [d for d in datos if d["fecha"] >= desde]
        if not rango:
            return 0.0
        d_actual = desde
        idx      = 0
        while d_actual <= hoy:
            while idx < len(rango) and rango[idx]["fecha"] <= d_actual:
                ultimo = rango[idx]["valor"]
                idx   += 1
            if ultimo is not None:
                tasa_dia = max(0.0, ultimo - spread)
                acum    *= (1 + tasa_dia / 360 / 100)
            d_actual += timedelta(days=1)
        return acum - 1  # decimal

    def anualizar(acum_dec: float, años: float) -> float:
        """(1 + r)^(1/n) - 1, en %."""
        if acum_dec <= -1:
            return -100.0
        return round(((1 + acum_dec) ** (1 / años) - 1) * 100, 2)

    def efectivo(acum_dec: float) -> float:
        return round(acum_dec * 100, 2)

    inicio_ytd = date(hoy.year, 1, 1)
    dias_ytd   = max((hoy - inicio_ytd).days, 1)

    # ── Backtesting: valor acumulado mensual desde ini_back ──
    ini_back  = date(2000, 1, 1)
    # Usar el primer dato disponible si es posterior a 2000
    if datos and datos[0]["fecha"] > ini_back:
        ini_back = datos[0]["fecha"]

    bt_puntos = []
    # Generar un punto por mes (primer día de cada mes)
    cur = date(ini_back.year, ini_back.month, 1)
    acum_bt = 1.0
    ultimo   = None
    idx_bt   = 0
    datos_bt = [d for d in datos if d["fecha"] >= ini_back]
    d_cur    = ini_back

    while d_cur <= hoy:
        # Avanzar tasa
        while idx_bt < len(datos_bt) and datos_bt[idx_bt]["fecha"] <= d_cur:
            ultimo = datos_bt[idx_bt]["valor"]
            idx_bt += 1
        if ultimo is not None:
            tasa_dia = max(0.0, ultimo - spread)
            acum_bt *= (1 + tasa_dia / 360 / 100)
        # Registrar punto si es primer día del mes o primer día
        if d_cur.day == 1 or d_cur == ini_back:
            bt_puntos.append({
                "fecha": d_cur.isoformat(),
                "valor": round(acum_bt * 100, 4)   # base 100
            })
        d_cur += timedelta(days=1)

    return {
        "r1m":         efectivo(componer_acum(hoy - timedelta(days=30))),
        "r3m":         efectivo(componer_acum(hoy - timedelta(days=91))),
        "r6m":         efectivo(componer_acum(hoy - timedelta(days=182))),
        "ytd":         efectivo(componer_acum(inicio_ytd)),
        "r1y":         anualizar(componer_acum(hoy - timedelta(days=365)),   1.0),
        "r2y":         anualizar(componer_acum(hoy - timedelta(days=730)),   2.0),
        "r3y":         anualizar(componer_acum(hoy - timedelta(days=1095)),  3.0),
        "backtesting": bt_puntos,
    }

def get_banxico_dato(serie_id: str) -> str | None:
    """Obtiene el dato oportuno de una serie Banxico."""
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

def get_macro() -> dict:
    """Devuelve datos macro con caché de 1 hora."""
    global _macro_cache, _macro_ts
    import time
    if _macro_cache and (time.time() - _macro_ts) < 3600:
        return _macro_cache

    tiie  = get_banxico_dato(SERIE_TIIE28)
    usdmx = get_banxico_dato(SERIE_USDMXN)
    cetes = get_banxico_dato(SERIE_CETES28)

    _macro_cache = {
        "tiie28":  round(float(tiie),  4) if tiie  else None,
        "usdmxn":  round(float(usdmx), 4) if usdmx else None,
        "cetes28": round(float(cetes), 4) if cetes else None,
    }
    _macro_ts = time.time()
    return _macro_cache





@app.route("/api/diag-repo")
def diag_repo():
    """Diagnóstico: verifica conectividad con Banxico y FRED para reporto."""
    if "usuario" not in session:
        return jsonify({"ok": False, "error": "No autenticado"}), 401

    resultado = {}

    # Test Banxico TIIE
    try:
        hoy = date.today()
        ini = (hoy - timedelta(days=10)).isoformat()
        fin = hoy.isoformat()
        raw = _banxico_serie_rango(SERIE_TIIE28, ini, fin)
        resultado["banxico"] = {
            "ok": len(raw) > 0,
            "token_set": bool(BANXICO_TOKEN),
            "registros": len(raw),
            "ultimo": raw[-1] if raw else None,
        }
    except Exception as e:
        resultado["banxico"] = {"ok": False, "error": str(e)}

    # Test FRED DFF
    try:
        hoy = date.today()
        params = {
            "series_id": "DFF",
            "observation_start": (hoy - timedelta(days=10)).isoformat(),
            "observation_end":   hoy.isoformat(),
            "api_key":    FRED_API_KEY,
            "file_type":  "json",
        }
        r = requests.get(FRED_BASE, params=params, timeout=10)
        obs = r.json().get("observations", [])
        resultado["fred"] = {
            "ok": len(obs) > 0,
            "key_set": bool(FRED_API_KEY),
            "status": r.status_code,
            "registros": len(obs),
            "ultimo": obs[-1] if obs else None,
        }
    except Exception as e:
        resultado["fred"] = {"ok": False, "error": str(e)}

    # Test rendimientos con tasa 7%
    try:
        rend_mxn = get_repo_rendimientos(7.0, False)
        rend_usd = get_repo_rendimientos(4.0, True)
        resultado["rendimientos_mxn"] = rend_mxn
        resultado["rendimientos_usd"] = rend_usd
    except Exception as e:
        resultado["rendimientos"] = {"error": str(e)}

    return jsonify(resultado)


    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
