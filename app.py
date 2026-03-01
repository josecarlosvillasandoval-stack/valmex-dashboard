import os
import time
import threading
import requests
import yfinance as yf
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
# ──────────────── ESTO POR NADA DEL MUNDO SE MUEVE ──────────────────────────
PERFILES = {
    "0": {"VXGUBCP": 5.00,  "VXDEUDA": 90.00, "VXUDIMP": 5.00},
    "1": {"VXGUBCP": 25.00, "VXDEUDA": 12.00, "VXUDIMP": 7.00,  "VXGUBLP": 52.00, "VXTBILL": 4.00},
    "2": {"VXGUBCP": 26.93, "VXDEUDA": 18.00, "VXUDIMP": 5.83,  "VXGUBLP": 26.89, "VXTBILL": 2.35, "VALMX28": 17.00, "VALMX20": 3.00},
    "3": {"VXGUBCP": 20.40, "VXDEUDA": 5.76,  "VXUDIMP": 6.65,  "VXGUBLP": 25.11, "VXTBILL": 2.08, "VALMX28": 34.00, "VALMX20": 6.00},
    "4": {"VXGUBCP": 20.70, "VXDEUDA": 4.08,  "VXUDIMP": 5.37,  "VXGUBLP": 7.61,  "VXTBILL": 2.24, "VALMX28": 51.00, "VALMX20": 9.00},
}

# ─────────────────────────────────────────────────────────────────────────────
# FONDOS USD para drilldown separado
# ─────────────────────────────────────────────────────────────────────────────
FONDOS_USD = {"VXTBILL", "VXCOBER", "VLMXDME"}

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
# MAPA ISIN / SERIES / TIPO CLIENTE
# ──────────────── ESTO POR NADA DEL MUNDO SE MUEVE ──────────────────────────
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
    "Persona Física - B1FI/B1":           "PF",
    "Persona Física con Fee - B0FI/B0":    "PF_fee",
    "Plan Personal de Retiro - B1NC/B1CF": "PPR",
    "Persona Moral - B1CO":                "PM",
    "Persona Moral con Fee - B0CO":        "PM_fee",
}

SERIE_MAP = {
    "VXREPO1": {"PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":"B1CO","PM_fee":"B0CO"},
    "VXGUBCP": {"PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":"B1CO","PM_fee":"B0CO"},
    "VXUDIMP": {"PF":"B1FI","PF_fee":"B0FI","PPR":"B1NC","PM":"B1CO","PM_fee":"B0CO"},
    "VXDEUDA": {"PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":"B1CO","PM_fee":"B0CO"},
    "VXGUBLP": {"PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":"B1CO","PM_fee":"B0CO"},
    "VXTBILL": {"PF":"B0FI","PF_fee":"B0FI","PPR":"B0CF","PM":"B0CO","PM_fee":"B0CO"},
    "VXCOBER": {"PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":"B1CO","PM_fee":"B0CO"},
    "VLMXETF": {"PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":"B1CO","PM_fee":"B0CO"},
    "VLMXDME": {"PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":"B1CO","PM_fee":"B0CO"},
    "VALMXA":  {"PF":"B1",  "PF_fee":"B0",  "PPR":"B1",  "PM":"B1",  "PM_fee":"B0"},
    "VALMX20": {"PF":"B1",  "PF_fee":"B0",  "PPR":"B1",  "PM":"B1",  "PM_fee":"B0"},
    "VALMX28": {"PF":"B1FI","PF_fee":"B0FI","PPR":"B1NC","PM":"B1CO","PM_fee":"B0CO"},
    "VALMXVL": {"PF":"B1",  "PF_fee":"B0",  "PPR":"B1",  "PM":"B1",  "PM_fee":"B0"},
    "VALMXES": {"PF":"B1",  "PF_fee":"B0",  "PPR":"B1",  "PM":"B1",  "PM_fee":"B0"},
    "VLMXTEC": {"PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":"B1CO","PM_fee":"B0CO"},
    "VLMXESG": {"PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":"B1CO","PM_fee":"B0CO"},
    "VALMXHC": {"PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":"B1CO","PM_fee":"B0CO"},
    "VXINFRA": {"PF":"B1FI","PF_fee":"B0FI","PPR":"B1FI","PM":None,  "PM_fee":"B0CO"},
    "VLMXJUB": {"PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":None,  "PM_fee":None},
    "VLMXP24": {"PF":"B1FI","PF_fee":"B0FI","PPR":"B1NC","PM":None,  "PM_fee":None},
    "VLMXP31": {"PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":None,  "PM_fee":None},
    "VLMXP38": {"PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":None,  "PM_fee":None},
    "VLMXP45": {"PF":"B1FI","PF_fee":"B0FI","PPR":"B1CF","PM":None,  "PM_fee":None},
    "VLMXP52": {"PF":"B1FI","PF_fee":"B0FI","PPR":"B1NC","PM":None,  "PM_fee":None},
    "VLMXP59": {"PF":"B1FI","PF_fee":"B0FI","PPR":"B1NC","PM":None,  "PM_fee":None},
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


def calcular_portafolio(fondos_pct: dict, tipo_cliente: str) -> dict:
    universe = load_ms_universe()

    r1m = r3m = r6m = ytd = r1y = r2y = r3y = 0.0
    stock_t = bond_t = cash_t = 0.0
    geo_acc = {}; sec_acc = {}
    lista = []

    # Acumuladores separados MXN / USD
    # Ponderamos dur y ytm por (bond_fraction * w) para normalizar correctamente
    dur_mxn_num = ytm_mxn_num = bond_mxn_denom = 0.0
    dur_usd_num = ytm_usd_num = bond_usd_denom = 0.0
    cred_mxn = {}; cred_usd = {}

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
        stock_t += stock * w
        bond_t  += bond  * w
        cash_t  += cash  * w

        # ── Drilldown deuda separado MXN / USD ──
        # Peso real de la parte de deuda de este fondo en el portafolio
        bond_w = (bond / 100.0) * w  # fracción del portafolio que es deuda de este fondo
        is_usd = fondo in FONDOS_USD

        if bond > 0 and bond_w > 0:
            dur_val = safe_float(d.get("PS-EffectiveDuration"))
            ytm_val = safe_float(d.get("PS-YieldToMaturity"))  # ya viene en % ej: 7.96
            if is_usd:
                dur_usd_num   += dur_val * bond_w
                ytm_usd_num   += ytm_val * bond_w
                bond_usd_denom += bond_w
            else:
                dur_mxn_num   += dur_val * bond_w
                ytm_mxn_num   += ytm_val * bond_w
                bond_mxn_denom += bond_w

        # Calificación crediticia ponderada por deuda
        for cq_key, cq_lbl in [
            ("CQB-AAA","AAA"),("CQB-AA","AA"),("CQB-A","A"),
            ("CQB-BBB","BBB"),("CQB-BB","BB"),("CQB-B","B"),
            ("CQB-BelowB","<B"),("CQB-NotRated","NR"),
        ]:
            v = safe_float(d.get(cq_key))
            if v > 0 and bond_w > 0:
                contribution = v * bond_w
                if is_usd:
                    cred_usd[cq_lbl] = cred_usd.get(cq_lbl, 0) + contribution
                else:
                    cred_mxn[cq_lbl] = cred_mxn.get(cq_lbl, 0) + contribution

        if stock > 0:
            geo_raw = d.get("RE-RegionalExposure", [])
            if isinstance(geo_raw, list):
                for item in geo_raw:
                    region = item.get("Region", "")
                    val    = safe_float(item.get("Value", 0))
                    if region and val > 0:
                        geo_acc[region] = geo_acc.get(region, 0) + val * (stock * w / 100)

        if stock > 0:
            sector_map = {
                "GR-TechnologyNet":"Tecnología","GR-FinancialServicesNet":"Financiero",
                "GR-HealthcareNet":"Salud","GR-CommunicationServicesNet":"Comunicaciones",
                "GR-IndustrialsNet":"Industriales","GR-ConsumerCyclicalNet":"Consumo discrecional",
                "GR-ConsumerDefensiveNet":"Consumo básico","GR-BasicMaterialsNet":"Materiales",
                "GR-EnergyNet":"Energía","GR-RealEstateNet":"Bienes raíces","GR-UtilitiesNet":"Utilidades",
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

    def top_n(d, n=8):
        t = sum(d.values()) or 1
        items = sorted(d.items(), key=lambda x: -x[1])[:n]
        return {"labels":[i[0] for i in items],"values":[round(i[1]/t*100,2) for i in items]}

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
            "values":[round(bond_t,2),round(stock_t,2),round(cash_t,2)],
        },
        "composicion": sorted(lista, key=lambda x: -x["pct"]),
        "geo":      top_n(geo_acc),
        "sectores": top_n(sec_acc),
        "deuda": {
            "has_mxn":  has_mxn,
            "dur_mxn":  round(dur_mxn_num / bond_mxn_denom, 2) if has_mxn else 0,
            "ytm_mxn":  round(ytm_mxn_num / bond_mxn_denom, 2) if has_mxn else 0,
            "cred_mxn": weighted_credit_rating(cred_mxn, local_to_global=True) if cred_mxn else "—",
            "has_usd":  has_usd,
            "dur_usd":  round(dur_usd_num / bond_usd_denom, 2) if has_usd else 0,
            "ytm_usd":  round(ytm_usd_num / bond_usd_denom, 2) if has_usd else 0,
            "cred_usd": weighted_credit_rating(cred_usd) if cred_usd else "—",
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# HISTÓRICOS MERCADO — carga y caché en memoria
# ─────────────────────────────────────────────────────────────────────────────
_repo_cache = {}
_repo_lock  = threading.Lock()
_repo_ts    = 0.0
REPO_TTL    = 3600  # 1 hora

def _fetch_hist_mxn():
    """USD/MXN semanal desde 2000 vía Yahoo Finance (MXN=X)."""
    try:
        df = yf.Ticker("MXN=X").history(start="2000-01-01", interval="1wk")[["Close"]].dropna()
        fechas  = [d.strftime("%Y-%m-%d") for d in df.index]
        precios = [round(float(v), 4) for v in df["Close"]]
        print(f"[HIST MXN] {len(fechas)} registros desde {fechas[0]}")
        return {"fechas": fechas, "precios": precios}
    except Exception as e:
        print(f"[HIST MXN ERROR] {e}")
        return {"fechas": [], "precios": []}

def _fetch_sofr():
    """SOFR diario desde FRED (CSV público, sin API key)."""
    try:
        r = requests.get(
            "https://fred.stlouisfed.org/graph/fredgraph.csv?id=SOFR",
            timeout=20,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        r.raise_for_status()
        lines = [l for l in r.text.strip().split("\n")[1:] if l.split(",")[1].strip() not in ("", ".")]
        fechas = [l.split(",")[0] for l in lines]
        tasas  = [round(float(l.split(",")[1]), 4) for l in lines]
        print(f"[FRED] SOFR: {len(fechas)} registros OK")
        return {"fechas": fechas, "tasas": tasas}
    except Exception as e:
        print(f"[FRED ERROR] {e}")
        return {"fechas": [], "tasas": []}

def _fetch_hist_usd():
    """T-Bill 3M diario desde FRED filtrado desde inicio de SOFR (2018-04-03)."""
    try:
        r = requests.get(
            "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DTB3",
            timeout=20,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        r.raise_for_status()
        lines  = [l for l in r.text.strip().split("\n")[1:] if l.split(",")[1].strip() not in ("", ".")]
        lines  = [l for l in lines if l.split(",")[0] >= "2018-04-03"]
        fechas = [l.split(",")[0] for l in lines]
        tasas  = [round(float(l.split(",")[1]), 4) for l in lines]
        if fechas:
            print(f"[HIST USD] {len(fechas)} registros desde {fechas[0]}")
        return {"fechas": fechas, "tasas": tasas}
    except Exception as e:
        print(f"[HIST USD ERROR] {e}")
        return {"fechas": [], "tasas": []}

def load_repo():
    """Carga o devuelve cacheado el repositorio de datos de mercado."""
    global _repo_ts
    now = time.time()
    with _repo_lock:
        if _repo_cache and now - _repo_ts < REPO_TTL:
            return dict(_repo_cache)
    mxn  = _fetch_hist_mxn()
    sofr = _fetch_sofr()
    usd  = _fetch_hist_usd()
    data = {"ok": True, "mxn": mxn, "sofr": sofr, "usd": usd}
    with _repo_lock:
        _repo_cache.clear()
        _repo_cache.update(data)
        _repo_ts = time.time()
    return data

# ─────────────────────────────────────────────────────────────────────────────
# CACHÉ TICKERS YAHOO FINANCE
# ─────────────────────────────────────────────────────────────────────────────
_yf_cache     = {}
_yf_lock      = threading.Lock()
YF_TICKER_TTL = 900  # 15 min

def yf_quote(symbol: str) -> dict:
    """Obtiene cotización de Yahoo Finance con caché y manejo de rate-limit."""
    now = time.time()
    with _yf_lock:
        cached = _yf_cache.get(symbol)
        if cached and now - cached["ts"] < YF_TICKER_TTL:
            return cached["data"]
    try:
        tk   = yf.Ticker(symbol)
        info = tk.info
        qt   = info.get("quoteType", "")
        if not info or qt in ("", "NONE") or info.get("regularMarketPrice") is None and info.get("currentPrice") is None:
            result = {"ok": False, "error": "Ticker no encontrado o sin datos", "ticker": symbol}
        else:
            result = {
                "ok":         True,
                "ticker":     symbol,
                "nombre":     info.get("longName") or info.get("shortName") or symbol,
                "precio":     info.get("regularMarketPrice") or info.get("currentPrice"),
                "moneda":     info.get("currency", "MXN"),
                "cambio_pct": round(info.get("regularMarketChangePercent") or 0.0, 4),
                "volumen":    info.get("regularMarketVolume"),
                "exchange":   info.get("exchange"),
                "tipo":       qt,
            }
        print(f"[YF OK] {symbol}: {result.get('precio')}")
    except Exception as e:
        print(f"[YF ERROR] {symbol}: {e}")
        result = {"ok": False, "error": str(e), "ticker": symbol}
    with _yf_lock:
        _yf_cache[symbol] = {"ts": time.time(), "data": result}
    return result

# ─────────────────────────────────────────────────────────────────────────────
# TRADUCCIÓN Y UTILIDADES PARA ANÁLISIS DE INSTRUMENTOS (Comparativa)
# ─────────────────────────────────────────────────────────────────────────────

# Sectores: soporta tanto las claves internas de yfinance (camelCase/snake) como
# los strings de Morningstar (Title Case).
SECTORES_YF_ES = {
    "technology":"Tecnología","healthcare":"Salud","financialServices":"Servicios Financieros",
    "financialservices":"Servicios Financieros","consumerCyclical":"Consumo Cíclico",
    "consumercyclical":"Consumo Cíclico","consumerDefensive":"Consumo Básico",
    "consumerdefensive":"Consumo Básico","industrials":"Industrial","energy":"Energía",
    "basicMaterials":"Materiales Básicos","basicmaterials":"Materiales Básicos",
    "realestate":"Bienes Raíces","real_estate":"Bienes Raíces",
    "communicationServices":"Comunicaciones","communicationservices":"Comunicaciones",
    "utilities":"Servicios Públicos","Technology":"Tecnología","Healthcare":"Salud",
    "Financial Services":"Servicios Financieros","Consumer Cyclical":"Consumo Cíclico",
    "Consumer Defensive":"Consumo Básico","Industrials":"Industrial","Energy":"Energía",
    "Basic Materials":"Materiales Básicos","Real Estate":"Bienes Raíces",
    "Communication Services":"Comunicaciones","Utilities":"Servicios Públicos",
    "Consumer Staples":"Consumo Básico","Consumer Discretionary":"Consumo Cíclico",
    "Information Technology":"Tecnología","Health Care":"Salud",
    "Materials":"Materiales Básicos","Financials":"Servicios Financieros",
}

PAISES_ES = {
    "United States":"Estados Unidos","Mexico":"México","Canada":"Canadá",
    "United Kingdom":"Reino Unido","Germany":"Alemania","France":"Francia",
    "Japan":"Japón","China":"China","Brazil":"Brasil","India":"India",
    "South Korea":"Corea del Sur","Australia":"Australia","Switzerland":"Suiza",
    "Netherlands":"Países Bajos","Taiwan":"Taiwán","Hong Kong":"Hong Kong",
    "Italy":"Italia","Spain":"España","Sweden":"Suecia","Denmark":"Dinamarca",
    "Singapore":"Singapur","Saudi Arabia":"Arabia Saudita","South Africa":"Sudáfrica",
    "Argentina":"Argentina","Chile":"Chile","Colombia":"Colombia","Belgium":"Bélgica",
    "Norway":"Noruega","Finland":"Finlandia","Ireland":"Irlanda","Austria":"Austria",
    "Israel":"Israel","Indonesia":"Indonesia","Thailand":"Tailandia",
    "Malaysia":"Malasia","Philippines":"Filipinas","Poland":"Polonia",
    "Turkey":"Turquía","Russia":"Rusia","Greece":"Grecia","Portugal":"Portugal",
    "New Zealand":"Nueva Zelanda","United Arab Emirates":"Emiratos Árabes",
}

TIPOS_ES = {
    "EQUITY":"Acción","ETF":"ETF","INDEX":"Índice","MUTUALFUND":"Fondo Mutuo",
    "FUTURE":"Futuro","CURRENCY":"Divisa","CRYPTOCURRENCY":"Criptomoneda","BOND":"Bono",
}

# Nombres limpios para ETFs e índices conocidos (sin marca corporativa)
ETF_NOMBRES_LIMPIOS = {
    "SPY":"S&P 500","IVV":"S&P 500","VOO":"S&P 500","CSPX":"S&P 500",
    "QQQ":"Nasdaq 100","QQQM":"Nasdaq 100","ONEQ":"Nasdaq Composite",
    "DIA":"Dow Jones Industrial","IWM":"Russell 2000","IWB":"Russell 1000",
    "IWF":"Russell 1000 Crecimiento","IWD":"Russell 1000 Valor",
    "EEM":"Mercados Emergentes","VWO":"Mercados Emergentes","IEMG":"Mercados Emergentes",
    "EFA":"Mercados Desarrollados Ex-EUA","VXUS":"Acciones Internacionales",
    "VEA":"Mercados Desarrollados","VTI":"Mercado Total EUA",
    "ITOT":"Mercado Total EUA","ACWI":"Mercado Global","VT":"Mercado Global",
    "EWW":"Acciones México","NAFTRAC.MX":"IPC México",
    "XLK":"Tecnología S&P","XLF":"Financiero S&P","XLV":"Salud S&P",
    "XLE":"Energía S&P","XLI":"Industrial S&P","XLB":"Materiales S&P",
    "XLY":"Consumo Discrecional S&P","XLP":"Consumo Básico S&P",
    "XLU":"Servicios Públicos S&P","XLRE":"Bienes Raíces S&P","XLC":"Comunicaciones S&P",
    "AGG":"Bonos EUA Amplio","BND":"Bonos EUA Amplio",
    "TLT":"Bonos Tesoro 20+ Años","IEF":"Bonos Tesoro 7-10 Años",
    "SHY":"Bonos Tesoro 1-3 Años","LQD":"Bonos Corporativos IG",
    "HYG":"Bonos Alto Rendimiento","EMB":"Bonos Mercados Emergentes",
    "GLD":"Oro","IAU":"Oro","SLV":"Plata","PDBC":"Materias Primas","DBC":"Materias Primas",
    "VNQ":"Bienes Raíces EUA","IYR":"Bienes Raíces EUA",
    "^GSPC":"S&P 500","^SPX":"S&P 500","^NDX":"Nasdaq 100","^IXIC":"Nasdaq Composite",
    "^DJI":"Dow Jones Industrial","^RUT":"Russell 2000","^FTSE":"FTSE 100",
    "^N225":"Nikkei 225","^HSI":"Hang Seng","^DAX":"DAX 40","^MXX":"IPC México",
}

# Geografía predefinida para ETFs conocidos (datos aproximados de referencia)
ETF_GEO = {
    "SPY":[{"nombre":"Estados Unidos","pct":100.0}],
    "IVV":[{"nombre":"Estados Unidos","pct":100.0}],
    "VOO":[{"nombre":"Estados Unidos","pct":100.0}],
    "QQQ":[{"nombre":"Estados Unidos","pct":100.0}],
    "QQQM":[{"nombre":"Estados Unidos","pct":100.0}],
    "DIA":[{"nombre":"Estados Unidos","pct":100.0}],
    "IWM":[{"nombre":"Estados Unidos","pct":100.0}],
    "VTI":[{"nombre":"Estados Unidos","pct":100.0}],
    "ITOT":[{"nombre":"Estados Unidos","pct":100.0}],
    "EWW":[{"nombre":"México","pct":100.0}],
    "NAFTRAC.MX":[{"nombre":"México","pct":100.0}],
    "^MXX":[{"nombre":"México","pct":100.0}],
    "^GSPC":[{"nombre":"Estados Unidos","pct":100.0}],
    "^NDX":[{"nombre":"Estados Unidos","pct":100.0}],
    "^DJI":[{"nombre":"Estados Unidos","pct":100.0}],
    "EEM":[{"nombre":"China","pct":27.5},{"nombre":"India","pct":17.0},
           {"nombre":"Taiwán","pct":15.2},{"nombre":"Corea del Sur","pct":12.7},
           {"nombre":"Brasil","pct":6.1},{"nombre":"Otros","pct":21.5}],
    "VWO":[{"nombre":"China","pct":26.8},{"nombre":"India","pct":18.5},
           {"nombre":"Taiwán","pct":14.2},{"nombre":"Brasil","pct":8.5},
           {"nombre":"Otros","pct":32.0}],
    "IEMG":[{"nombre":"China","pct":25.3},{"nombre":"India","pct":17.8},
            {"nombre":"Taiwán","pct":14.0},{"nombre":"Corea del Sur","pct":11.9},
            {"nombre":"Brasil","pct":5.8},{"nombre":"Otros","pct":25.2}],
    "EFA":[{"nombre":"Japón","pct":21.1},{"nombre":"Reino Unido","pct":15.8},
           {"nombre":"Francia","pct":11.2},{"nombre":"Alemania","pct":9.0},
           {"nombre":"Suiza","pct":8.7},{"nombre":"Otros","pct":34.2}],
    "VEA":[{"nombre":"Japón","pct":22.5},{"nombre":"Reino Unido","pct":13.1},
           {"nombre":"Canadá","pct":10.2},{"nombre":"Francia","pct":9.8},
           {"nombre":"Alemania","pct":8.4},{"nombre":"Otros","pct":36.0}],
    "ACWI":[{"nombre":"Estados Unidos","pct":64.5},{"nombre":"Japón","pct":5.5},
            {"nombre":"Reino Unido","pct":4.0},{"nombre":"Francia","pct":3.2},
            {"nombre":"Alemania","pct":2.8},{"nombre":"Otros","pct":20.0}],
    "VT":[{"nombre":"Estados Unidos","pct":62.8},{"nombre":"Japón","pct":6.0},
          {"nombre":"Reino Unido","pct":4.2},{"nombre":"China","pct":3.8},
          {"nombre":"Otros","pct":23.2}],
    "VXUS":[{"nombre":"Japón","pct":14.5},{"nombre":"Reino Unido","pct":10.2},
            {"nombre":"China","pct":9.8},{"nombre":"Francia","pct":6.5},
            {"nombre":"Otros","pct":59.0}],
}

# Prefijos y sufijos de marca a eliminar de nombres de ETFs
_ETF_PREF = ["iShares MSCI ","iShares Core ","iShares ","Vanguard ","SPDR S&P ",
             "SPDR ","Invesco QQQ Trust ","Invesco ","Schwab ","Fidelity ",
             "JPMorgan ","ProShares ","WisdomTree ","VanEck ","First Trust ",
             "Direxion ","ARK ","Global X ","BlackRock "]
_ETF_SUF  = [" ETF"," Trust"," Index Fund"," Fund"," Portfolio"," Index"," Shares"]


def _limpiar_nombre(ticker: str, nombre: str, qt: str) -> str:
    """Devuelve nombre limpio: sin marca para ETFs/índices, shortName para acciones."""
    tb = ticker.upper().split(".")[0]
    if tb in ETF_NOMBRES_LIMPIOS:
        return ETF_NOMBRES_LIMPIOS[tb]
    if ticker in ETF_NOMBRES_LIMPIOS:
        return ETF_NOMBRES_LIMPIOS[ticker]
    if qt in ("ETF", "INDEX"):
        r = nombre
        for p in _ETF_PREF:
            if r.startswith(p):
                r = r[len(p):]; break
        for s in _ETF_SUF:
            if r.endswith(s):
                r = r[:-len(s)]; break
        return r.strip() or nombre
    return nombre


def _calcular_rendimientos(hist) -> dict:
    """Calcula rendimientos MTD/3M/YTD/12M/24M/36M desde DataFrame de precios."""
    import pandas as pd
    from datetime import date, timedelta
    if hist is None or hist.empty:
        return {}
    prices = hist["Close"].dropna()
    if len(prices) < 2:
        return {}
    tz     = prices.index.tz
    ultimo = float(prices.iloc[-1])

    def p_prev(d):
        try:
            ts   = pd.Timestamp(d).tz_localize(tz) if tz else pd.Timestamp(d)
            mask = prices.index <= ts
            return float(prices[mask].iloc[-1]) if mask.any() else None
        except:
            return None

    def ret(p0, anios=None):
        if not p0:
            return None
        r = (ultimo / p0 - 1) * 100
        if anios and anios > 1:
            r = ((ultimo / p0) ** (1.0 / anios) - 1) * 100
        return round(r, 2)

    hoy = date.today()
    return {
        "mtd": ret(p_prev(hoy.replace(day=1))),
        "3m":  ret(p_prev(hoy - timedelta(days=92))),
        "ytd": ret(p_prev(hoy.replace(month=1, day=1))),
        "12m": ret(p_prev(hoy.replace(year=hoy.year - 1))),
        "24m": ret(p_prev(hoy.replace(year=hoy.year - 2)), anios=2),
        "36m": ret(p_prev(hoy.replace(year=hoy.year - 3)), anios=3),
    }


def _get_sectores(tk, info: dict, qt: str) -> list:
    """Sectores en español para una acción o ETF."""
    if qt == "EQUITY":
        s = info.get("sector", "")
        if s:
            return [{"nombre": SECTORES_YF_ES.get(s, s), "pct": 100.0}]
        ind = info.get("industry", "")
        return [{"nombre": ind, "pct": 100.0}] if ind else []

    # ETF / INDEX / MUTUALFUND — intento 1: sectorWeightings en info
    try:
        sw = info.get("sectorWeightings")
        if sw and isinstance(sw, list):
            out = []
            for item in sw:
                for k, v in item.items():
                    nom = SECTORES_YF_ES.get(k, SECTORES_YF_ES.get(k.lower(), k))
                    pct = round(float(v) * 100, 1)
                    if pct >= 0.5:
                        out.append({"nombre": nom, "pct": pct})
            if out:
                return sorted(out, key=lambda x: -x["pct"])[:8]
    except Exception:
        pass

    # intento 2: funds_data (yfinance >=0.2.40)
    try:
        fd  = tk.funds_data
        sw2 = getattr(fd, "sector_weightings", None)
        if sw2 is not None and len(sw2) > 0:
            out = []
            for k, v in sw2.items():
                nom = SECTORES_YF_ES.get(k, SECTORES_YF_ES.get(k.lower(), k))
                pct = round(float(v) * 100, 1)
                if pct >= 0.5:
                    out.append({"nombre": nom, "pct": pct})
            if out:
                return sorted(out, key=lambda x: -x["pct"])[:8]
    except Exception:
        pass

    return []


def _get_geografia(ticker: str, info: dict, qt: str) -> list:
    """Exposición geográfica del instrumento."""
    tb = ticker.upper().split(".")[0]
    if tb  in ETF_GEO: return ETF_GEO[tb]
    if ticker in ETF_GEO: return ETF_GEO[ticker]
    # Acción individual → país del emisor
    pais_en = info.get("country", "")
    pais_es = PAISES_ES.get(pais_en, pais_en)
    if pais_es:
        return [{"nombre": pais_es, "pct": 100.0}]
    # Fallback por bolsa
    _exchange_pais = {
        "NMS":"Estados Unidos","NYQ":"Estados Unidos","NYSEArca":"Estados Unidos",
        "NGM":"Estados Unidos","BTS":"Estados Unidos","PCX":"Estados Unidos",
        "MEX":"México","BMV":"México","TSX":"Canadá","LSE":"Reino Unido",
        "PAR":"Francia","XETR":"Alemania","MIL":"Italia","MCE":"España",
        "TYO":"Japón","SHH":"China","SHZ":"China","HKG":"Hong Kong",
        "ASX":"Australia","BSE":"India","NSE":"India","KSC":"Corea del Sur","SAO":"Brasil",
    }
    p = _exchange_pais.get(info.get("exchange", ""), "")
    return [{"nombre": p, "pct": 100.0}] if p else []


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


@app.route("/api/propuesta", methods=["POST"])
def api_propuesta():
    if "usuario" not in session:
        return jsonify({"ok": False, "error": "No autenticado"}), 401

    body         = request.get_json(force=True)
    tipo_cliente = body.get("tipo_cliente", "Persona Física - B1FI/B1")
    modo         = body.get("modo", "propuesta")

    if modo == "perfil":
        pid = str(body.get("perfil_id", "3"))
        fondos_pct = PERFILES.get(pid)
        if not fondos_pct:
            return jsonify({"ok": False, "error": f"Perfil {pid} no existe"}), 400
    else:
        raw = body.get("fondos", {})
        fondos_pct = {k: float(v) for k, v in raw.items() if float(v) > 0}
        if not fondos_pct:
            return jsonify({"ok": False, "error": "Sin fondos con % > 0"}), 400

    return jsonify(calcular_portafolio(fondos_pct, tipo_cliente))


@app.route("/api/accion/data", methods=["POST"])
def api_accion_data():
    """Datos completos de un instrumento para la sección Comparativa."""
    if "usuario" not in session:
        return jsonify({"ok": False, "error": "No autenticado"}), 401
    body   = request.get_json(force=True)
    ticker = body.get("ticker", "").strip().upper()
    if not ticker:
        return jsonify({"ok": False, "error": "ticker requerido"}), 400

    now       = time.time()
    cache_key = f"data_{ticker}"
    with _yf_lock:
        cached = _yf_cache.get(cache_key)
        if cached and now - cached["ts"] < YF_TICKER_TTL:
            return jsonify(cached["data"])
    try:
        tk    = yf.Ticker(ticker)
        info  = tk.info
        qt    = info.get("quoteType", "EQUITY")
        precio = info.get("regularMarketPrice") or info.get("currentPrice")
        if not info or precio is None:
            result = {"ok": False, "error": "Ticker no encontrado o sin precio", "ticker": ticker}
        else:
            nombre_largo = info.get("longName") or info.get("shortName") or ticker
            nombre_corto = _limpiar_nombre(ticker, nombre_largo, qt)
            tipo_es      = TIPOS_ES.get(qt, qt)

            # Histórico 3 años diario
            hist = tk.history(period="3y", interval="1d")

            # Rendimientos
            rends = _calcular_rendimientos(hist)

            # Base-100 semanal (menos payload)
            hist_base = {}
            if not hist.empty:
                prices_w = hist["Close"].dropna().resample("W").last().dropna()
                if not prices_w.empty:
                    base_val = float(prices_w.iloc[0])
                    if base_val > 0:
                        fechas  = [d.strftime("%Y-%m-%d") for d in prices_w.index]
                        base100 = [round(float(v) / base_val * 100, 2) for v in prices_w]
                        hist_base = {"fechas": fechas, "base100": base100}

            sectores  = _get_sectores(tk, info, qt)
            geografia = _get_geografia(ticker, info, qt)

            result = {
                "ok":          True,
                "ticker":      ticker,
                "nombre":      nombre_corto,
                "nombre_full": nombre_largo,
                "precio":      precio,
                "moneda":      info.get("currency", "MXN"),
                "cambio_pct":  round(info.get("regularMarketChangePercent") or 0.0, 4),
                "tipo":        tipo_es,
                "exchange":    info.get("exchange", ""),
                "rendimientos": rends,
                "historico":   hist_base,
                "sectores":    sectores,
                "geografia":   geografia,
            }
            print(f"[YF DATA] {ticker}: {nombre_corto} @ {precio} {info.get('currency','')}")
    except Exception as e:
        print(f"[YF DATA ERROR] {ticker}: {e}")
        result = {"ok": False, "error": str(e), "ticker": ticker}
    with _yf_lock:
        _yf_cache[cache_key] = {"ts": time.time(), "data": result}
    return jsonify(result)


@app.route("/api/diag-repo")
def api_diag_repo():
    if "usuario" not in session:
        return jsonify({"ok": False, "error": "No autenticado"}), 401
    return jsonify(load_repo())


@app.route("/api/accion/validate", methods=["POST"])
def api_accion_validate():
    if "usuario" not in session:
        return jsonify({"ok": False, "error": "No autenticado"}), 401
    body   = request.get_json(force=True)
    ticker = body.get("ticker", "").strip().upper()
    if not ticker:
        return jsonify({"ok": False, "error": "ticker requerido"}), 400
    return jsonify(yf_quote(ticker))


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
