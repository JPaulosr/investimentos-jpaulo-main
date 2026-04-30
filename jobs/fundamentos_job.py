#!/usr/bin/env python3
# jobs/fundamentos_job.py
# -*- coding: utf-8 -*-
"""
FUNDAMENTOS JOB — coleta indicadores, cria cache e calcula valuation projetivo.

Função:
- cria/verifica abas no Google Sheets:
    fundamentos_cache
    fundamentos_historico
    valuation_config
    valuation_resultado
- lê tickers da carteira_snapshot e/ou ativos_master;
- busca indicadores no Investidor10 com requests + regex tolerante;
- calcula preço teto projetivo para ações usando LPA + payout + DY alvo;
- calcula teto histórico por DPA 12m para ações/FIIs usando aba proventos;
- grava resultado para o Radar/Momento do Aporte consumir depois.

Compatível com:
- GitHub Actions/single-user: usa SHEET_ID + GCP_SERVICE_ACCOUNT_JSON;
- VPS multiusuário: rodar_jobs.py injeta SHEET_ID de cada usuário.

Uso local:
    python jobs/fundamentos_job.py

Observação:
- O site pode mudar o HTML ou bloquear requisições. O job não quebra a carteira:
  ele salva o que conseguir e registra "erro"/"observacao" nas abas.
"""

from __future__ import annotations

import html
import json
import math
import os
import re
import sys
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
import gspread
from google.oauth2.service_account import Credentials

# =============================================================================
# ENV / CONSTANTES
# =============================================================================
SHEET_ID = (os.getenv("SHEET_ID_NOVO") or os.getenv("SHEET_ID") or "").strip()
GCP_JSON = (
    os.getenv("GCP_SERVICE_ACCOUNT_JSON")
    or os.getenv("GCP_SERVICE_ACCOUNT")
    or os.getenv("GOOGLE_SERVICE_ACCOUNT")
    or ""
).strip()

REQUEST_TIMEOUT = 25
SLEEP_BETWEEN_TICKERS = float(os.getenv("FUNDAMENTOS_SLEEP", "1.0") or 1.0)
USER_AGENT = os.getenv(
    "FUNDAMENTOS_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
)

ABA_CARTEIRA = "carteira_snapshot"
ABA_ATIVOS_MASTER = "ativos_master"
ABA_PROVENTOS = os.getenv("ABA_PROVENTOS_NOVO") or os.getenv("ABA_PROVENTOS") or "proventos"
ABA_CACHE = "fundamentos_cache"
ABA_HIST = "fundamentos_historico"
ABA_CONFIG = "valuation_config"
ABA_RESULT = "valuation_resultado"

FUNDAMENTOS_CACHE_HEADERS = [
    "ticker", "nome", "classe", "fonte_url", "status_coleta", "erro", "atualizado_em",
    "preco", "pl", "psr", "pvp", "dy_pct", "payout_pct",
    "margem_liquida_pct", "margem_bruta_pct", "margem_ebit_pct", "margem_ebitda_pct",
    "ev_ebitda", "ev_ebit", "p_ebitda", "p_ebit", "p_ativo", "p_cap_giro", "p_ativo_circ_liq",
    "vpa", "lpa", "giro_ativos", "roe_pct", "roic_pct", "roa_pct",
    "divida_liquida_patrimonio", "divida_liquida_ebitda", "divida_liquida_ebit",
    "divida_bruta_patrimonio", "patrimonio_ativos", "passivos_ativos", "liquidez_corrente",
    "cagr_receitas_5a_pct", "cagr_lucros_5a_pct",
    "receita_liquida_12m", "lucro_liquido_12m", "ebitda_12m", "ebit_12m",
]

FUNDAMENTOS_HIST_HEADERS = [
    "data_coleta", "ticker", "classe", "indicador", "valor", "fonte",
]

VALUATION_CONFIG_HEADERS = [
    "ticker", "ativo", "classe", "dy_alvo_pct",
    "payout_conservador_pct", "payout_base_pct",
    "crescimento_lpa_conservador_pct", "crescimento_lpa_base_pct",
    "margem_seguranca_pct", "pl_max", "pvp_max", "divida_ebitda_max",
    "roe_min_pct", "roic_min_pct", "payout_max_pct", "cagr_lucro_min_pct",
    "usar_teto_mais_conservador", "observacao", "atualizado_em",
]

VALUATION_RESULT_HEADERS = [
    "ticker", "nome", "classe", "preco_atual", "dpa_12m_historico",
    "lpa_atual", "payout_atual_pct", "payout_conservador_pct", "payout_base_pct",
    "dpa_proj_conservador", "dpa_proj_base",
    "dy_alvo_pct", "teto_historico", "teto_proj_conservador", "teto_proj_base",
    "teto_com_margem", "margem_seguranca_pct", "distancia_teto_pct",
    "pl", "pvp", "roe_pct", "roic_pct", "divida_liquida_ebitda", "cagr_lucros_5a_pct",
    "score_qualidade", "status_qualidade", "status_valuation", "motivos", "atualizado_em",
]

SHEETS = {
    ABA_CACHE: FUNDAMENTOS_CACHE_HEADERS,
    ABA_HIST: FUNDAMENTOS_HIST_HEADERS,
    ABA_CONFIG: VALUATION_CONFIG_HEADERS,
    ABA_RESULT: VALUATION_RESULT_HEADERS,
}

# =============================================================================
# HELPERS GERAIS
# =============================================================================
def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def norm_ticker(x: Any) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(x or "").upper().strip())


def normalize_name(s: Any) -> str:
    s = str(s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s


def strip_accents(s: str) -> str:
    # leve e sem dependência externa
    trans = str.maketrans(
        "áàãâäéèêëíìîïóòõôöúùûüçÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇ",
        "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC",
    )
    return str(s or "").translate(trans)


def to_float(v: Any) -> float:
    if v is None:
        return 0.0
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        try:
            if math.isnan(float(v)) or math.isinf(float(v)):
                return 0.0
            return float(v)
        except Exception:
            return 0.0

    s = str(v).strip()
    if not s or s.lower() in {"nan", "nat", "none", "null", "—", "-"}:
        return 0.0

    mult = 1.0
    s_low = strip_accents(s.lower())
    if "trilhao" in s_low or "trilhoes" in s_low:
        mult = 1_000_000_000_000.0
    elif "bilhao" in s_low or "bilhoes" in s_low:
        mult = 1_000_000_000.0
    elif "milhao" in s_low or "milhoes" in s_low:
        mult = 1_000_000.0
    elif re.search(r"\bmil\b", s_low):
        mult = 1_000.0

    s = s.replace("R$", "").replace("%", "").replace(" ", "")
    s = re.sub(r"[^0-9,\.\-]", "", s)
    if not s or s in {"-", ".", ","}:
        return 0.0

    # pt-BR: 1.234,56 | en-US: 1,234.56 | decimal simples: 9,34
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        s = s.replace(",", ".")

    try:
        return float(s) * mult
    except Exception:
        return 0.0


def brl(v: Any) -> str:
    n = to_float(v)
    return f"R$ {n:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def pct(v: Any) -> str:
    n = to_float(v)
    return f"{n:,.2f}%".replace(".", ",")


def parse_dates(values: Any) -> pd.Series:
    s = values.copy() if isinstance(values, pd.Series) else pd.Series(values)
    s_str = s.astype(str).str.strip()
    out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")

    num = pd.to_numeric(s_str.str.replace(",", ".", regex=False), errors="coerce")
    mask_num = num.notna() & (num > 20000) & (num < 80000)
    if mask_num.any():
        out.loc[mask_num] = pd.to_datetime(
            num.loc[mask_num], unit="D", origin="1899-12-30", errors="coerce"
        ).dt.normalize()

    mask_iso = s_str.str.match(r"^\d{4}-\d{1,2}-\d{1,2}", na=False) & out.isna()
    if mask_iso.any():
        out.loc[mask_iso] = pd.to_datetime(s_str.loc[mask_iso], errors="coerce", dayfirst=False)

    mask_rest = out.isna() & s_str.notna() & ~s_str.str.lower().isin(["", "nan", "nat", "none"])
    if mask_rest.any():
        out.loc[mask_rest] = pd.to_datetime(s_str.loc[mask_rest], errors="coerce", dayfirst=True)
    return out


def pick_col(df: pd.DataFrame, names: List[str]) -> Optional[str]:
    if df is None or df.empty:
        return None
    lower = {strip_accents(str(c).strip().lower()): c for c in df.columns}
    for n in names:
        key = strip_accents(n.strip().lower())
        if key in lower:
            return lower[key]
    return None


def is_yes(v: Any, default: bool = True) -> bool:
    if v is None or str(v).strip() == "":
        return default
    s = str(v).strip().lower()
    return s in {"sim", "s", "true", "1", "yes", "y", "ativo"}

# =============================================================================
# GOOGLE SHEETS
# =============================================================================
def get_client() -> gspread.Client:
    if not GCP_JSON:
        raise RuntimeError("GCP_SERVICE_ACCOUNT_JSON/GCP_SERVICE_ACCOUNT vazio.")
    info = json.loads(GCP_JSON)
    if "private_key" in info and isinstance(info["private_key"], str):
        info["private_key"] = info["private_key"].replace("\\n", "\n")
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def open_sheet() -> gspread.Spreadsheet:
    if not SHEET_ID:
        raise RuntimeError("SHEET_ID/SHEET_ID_NOVO vazio.")
    return get_client().open_by_key(SHEET_ID)


def ensure_ws(sh: gspread.Spreadsheet, title: str, headers: List[str], rows: int = 3000) -> gspread.Worksheet:
    try:
        ws = sh.worksheet(title)
    except Exception:
        ws = sh.add_worksheet(title=title, rows=rows, cols=max(len(headers), 20))
        ws.update([headers], "A1", value_input_option="USER_ENTERED")
        print(f"  📋 Aba criada: {title}")
        return ws

    vals = ws.get_all_values()
    if not vals:
        ws.update([headers], "A1", value_input_option="USER_ENTERED")
        return ws

    cur = [str(h).strip() for h in vals[0]]
    cur_norm = [strip_accents(c.lower()) for c in cur]
    updates = []
    for h in headers:
        if strip_accents(h.lower()) not in cur_norm:
            cur.append(h)
            cur_norm.append(strip_accents(h.lower()))
    if cur != [str(h).strip() for h in vals[0]]:
        ws.update([cur], "A1", value_input_option="USER_ENTERED")
        print(f"  ✅ Header atualizado: {title}")
    return ws


def ensure_all_sheets(sh: gspread.Spreadsheet) -> None:
    for title, headers in SHEETS.items():
        ensure_ws(sh, title, headers)


def read_df(sh: gspread.Spreadsheet, title: str) -> pd.DataFrame:
    try:
        ws = sh.worksheet(title)
        vals = ws.get_all_values(value_render_option="UNFORMATTED_VALUE")
        if not vals or len(vals) < 2:
            return pd.DataFrame()
        headers = [str(h).strip() for h in vals[0]]
        return pd.DataFrame(vals[1:], columns=headers)
    except Exception as e:
        print(f"  ⚠ Não consegui ler aba '{title}': {e}")
        return pd.DataFrame()


def write_replace(ws: gspread.Worksheet, headers: List[str], rows: List[List[Any]]) -> None:
    out = [headers] + rows
    ws.clear()
    ws.update(out, "A1", value_input_option="USER_ENTERED")


def append_rows(ws: gspread.Worksheet, rows: List[List[Any]], chunk: int = 200) -> None:
    if not rows:
        return
    for i in range(0, len(rows), chunk):
        ws.append_rows(rows[i:i + chunk], value_input_option="USER_ENTERED")

# =============================================================================
# LEITURA DE TICKERS / PROVENTOS
# =============================================================================
def load_universe(sh: gspread.Spreadsheet) -> pd.DataFrame:
    """Lê universo de ativos priorizando carteira_snapshot, com complemento do ativos_master."""
    frames = []

    df_cart = read_df(sh, ABA_CARTEIRA)
    if not df_cart.empty:
        df = df_cart.copy()
        df.columns = [str(c).strip().lower() for c in df.columns]
        tk_col = pick_col(df, ["ticker", "ativo", "codigo"])
        if tk_col:
            out = pd.DataFrame()
            out["ticker"] = df[tk_col].apply(norm_ticker)
            out["nome"] = df[pick_col(df, ["nome", "empresa", "nome_ativo"])].apply(normalize_name) if pick_col(df, ["nome", "empresa", "nome_ativo"]) else ""
            out["classe"] = df[pick_col(df, ["classe", "tipo_ativo", "tipo"])].astype(str) if pick_col(df, ["classe", "tipo_ativo", "tipo"]) else ""
            out["preco"] = df[pick_col(df, ["preco_atual", "cotacao", "preco", "valor_atual"])].apply(to_float) if pick_col(df, ["preco_atual", "cotacao", "preco", "valor_atual"]) else 0.0
            out["preco_medio"] = df[pick_col(df, ["preco_medio", "pm"])].apply(to_float) if pick_col(df, ["preco_medio", "pm"]) else 0.0
            out["quantidade"] = df[pick_col(df, ["quantidade", "qtd", "cotas"])].apply(to_float) if pick_col(df, ["quantidade", "qtd", "cotas"]) else 0.0
            frames.append(out)

    df_master = read_df(sh, ABA_ATIVOS_MASTER)
    if not df_master.empty:
        df = df_master.copy()
        df.columns = [str(c).strip().lower() for c in df.columns]
        tk_col = pick_col(df, ["ticker", "ativo", "codigo"])
        if tk_col:
            out = pd.DataFrame()
            out["ticker"] = df[tk_col].apply(norm_ticker)
            out["nome"] = df[pick_col(df, ["nome", "empresa", "nome_ativo"])].apply(normalize_name) if pick_col(df, ["nome", "empresa", "nome_ativo"]) else ""
            out["classe"] = df[pick_col(df, ["classe", "tipo_ativo", "tipo"])].astype(str) if pick_col(df, ["classe", "tipo_ativo", "tipo"]) else ""
            out["preco"] = 0.0
            out["preco_medio"] = 0.0
            out["quantidade"] = 0.0
            frames.append(out)

    if not frames:
        return pd.DataFrame(columns=["ticker", "nome", "classe", "preco", "preco_medio", "quantidade"])

    uni = pd.concat(frames, ignore_index=True)
    uni = uni[uni["ticker"].astype(str).str.len() > 0]
    if uni.empty:
        return uni

    # consolida: mantém preço/quantidade/preço médio quando existir
    rows = []
    for tk, grp in uni.groupby("ticker", sort=True):
        def first_non_empty(col: str) -> Any:
            for x in grp[col].tolist():
                if str(x).strip() not in {"", "0", "0.0", "nan", "None"}:
                    return x
            return ""
        rows.append({
            "ticker": tk,
            "nome": first_non_empty("nome"),
            "classe": first_non_empty("classe"),
            "preco": max([to_float(x) for x in grp["preco"].tolist()] or [0.0]),
            "preco_medio": max([to_float(x) for x in grp["preco_medio"].tolist()] or [0.0]),
            "quantidade": max([to_float(x) for x in grp["quantidade"].tolist()] or [0.0]),
        })
    return pd.DataFrame(rows)


def calc_dpa_12m_from_proventos(sh: gspread.Spreadsheet) -> Dict[str, float]:
    """Calcula DPA/VPC 12m por ticker a partir da aba proventos."""
    df = read_df(sh, ABA_PROVENTOS)
    if df.empty:
        return {}

    df.columns = [str(c).strip().lower() for c in df.columns]
    tk_col = pick_col(df, ["ticker", "ativo", "codigo"])
    dt_col = pick_col(df, ["data", "data_pagamento", "pagamento"])
    vpc_col = pick_col(df, ["valor_por_cota", "vpc", "valor_liq_por_cota"])
    val_col = pick_col(df, ["valor", "total", "valor_total"])
    qtd_col = pick_col(df, ["quantidade_na_data", "quantidade", "qtd", "cotas"])
    tipo_col = pick_col(df, ["tipo", "tipo_pagamento"])
    id_col = pick_col(df, ["id", "event_id"])

    if not tk_col or not dt_col:
        return {}

    df["_ticker"] = df[tk_col].apply(norm_ticker)
    df["_data"] = parse_dates(df[dt_col])
    df = df.dropna(subset=["_data"])
    limite = pd.Timestamp(datetime.now().date() - timedelta(days=365))
    df = df[df["_data"] >= limite]

    if vpc_col:
        df["_vpc"] = df[vpc_col].apply(to_float)
    elif val_col and qtd_col:
        vals = df[val_col].apply(to_float)
        qtds = df[qtd_col].apply(to_float).replace(0, pd.NA)
        df["_vpc"] = (vals / qtds).fillna(0.0)
    else:
        return {}

    df = df[(df["_ticker"] != "") & (df["_vpc"] > 0)]
    if df.empty:
        return {}

    # dedupe conservador: id se existir; senão ticker+data+tipo+vpc
    if id_col:
        df["_dedupe"] = df[id_col].astype(str).str.strip()
        mask_empty = df["_dedupe"].isin(["", "nan", "None"])
        if tipo_col:
            df.loc[mask_empty, "_dedupe"] = (
                df.loc[mask_empty, "_ticker"].astype(str) + "|" +
                df.loc[mask_empty, "_data"].dt.strftime("%Y-%m-%d") + "|" +
                df.loc[mask_empty, tipo_col].astype(str) + "|" +
                df.loc[mask_empty, "_vpc"].round(8).astype(str)
            )
    else:
        tipo_series = df[tipo_col].astype(str) if tipo_col else ""
        df["_dedupe"] = (
            df["_ticker"].astype(str) + "|" +
            df["_data"].dt.strftime("%Y-%m-%d") + "|" +
            tipo_series.astype(str) + "|" +
            df["_vpc"].round(8).astype(str)
        )
    df = df.drop_duplicates("_dedupe")

    return {str(k): round(float(v), 8) for k, v in df.groupby("_ticker")["_vpc"].sum().items()}

# =============================================================================
# INVESTIDOR10 SCRAPER
# =============================================================================
def clean_html_text(raw_html: str) -> str:
    raw_html = re.sub(r"<script.*?</script>", " ", raw_html, flags=re.I | re.S)
    raw_html = re.sub(r"<style.*?</style>", " ", raw_html, flags=re.I | re.S)
    text = re.sub(r"<[^>]+>", " ", raw_html)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def label_pattern(label: str) -> str:
    # cria regex tolerante a espaços, acentos e barras simples
    base = strip_accents(label.lower())
    base = re.escape(base)
    base = base.replace(r"\ ", r"\s+")
    base = base.replace(r"\/", r"\s*/\s*")
    return base


def _first_number_after(text: str, start_pos: int, max_chars: int = 120, *, require_unit: bool = False, skip_years: bool = True) -> float:
    """Busca o primeiro número útil depois de um label.

    Corrige erros comuns do Investidor10:
    - capturar o "5" de "5 anos" no CAGR;
    - capturar números de cabeçalho como 12M/2025 antes do valor real.
    """
    trecho = text[start_pos:start_pos + max_chars].replace("−", "-")
    num_re = re.compile(
        r"(-?\d{1,3}(?:\.\d{3})*(?:,\d+)?|-?\d+(?:,\d+)?|-?\d+\.\d+)\s*"
        r"(Bilhao|Bilhoes|Milhao|Milhoes|Mil|%)?",
        re.I,
    )
    for m in num_re.finditer(trecho):
        raw = m.group(1)
        unit = m.group(2) or ""
        ini = max(0, m.start() - 12)
        fim = min(len(trecho), m.end() + 12)
        around = trecho[ini:fim].lower()
        after = trecho[m.end():m.end() + 12].lower()

        n_plain = to_float(raw)
        if skip_years and (1900 <= n_plain <= 2100):
            continue
        if re.search(r"\b" + re.escape(raw) + r"\s*(anos|ano|meses|mes|m)\b", around, flags=re.I):
            continue
        if raw in {"12", "5", "10"} and re.search(r"^(\s*)(m|anos|ano)", after, flags=re.I):
            continue
        if require_unit and not unit:
            continue
        return to_float((raw + " " + unit).strip())
    return 0.0


def find_metric(text_ascii: str, labels: List[str], max_chars: int = 90) -> float:
    """Acha o número logo após um label, evitando capturar anos/cabeçalhos."""
    for label in labels:
        lp = label_pattern(label)
        m = re.search(lp, text_ascii, flags=re.I | re.S)
        if not m:
            continue
        v = _first_number_after(text_ascii, m.end(), max_chars=max_chars, require_unit=False)
        if v != 0:
            return v
    return 0.0


def find_percent_metric(text_ascii: str, labels: List[str], max_chars: int = 100) -> float:
    """Acha percentual logo após um label. Se houver %, prioriza esse número."""
    for label in labels:
        lp = label_pattern(label)
        m = re.search(lp, text_ascii, flags=re.I | re.S)
        if not m:
            continue
        trecho = text_ascii[m.end():m.end() + max_chars].replace("−", "-")
        for mm in re.finditer(r"(-?\d{1,3}(?:\.\d{3})*(?:,\d+)?|-?\d+(?:,\d+)?|-?\d+\.\d+)\s*%", trecho):
            raw = mm.group(1)
            n = to_float(raw)
            if 1900 <= n <= 2100:
                continue
            if raw in {"5", "10"} and re.search(r"anos|ano", trecho[mm.start():mm.end()+12], flags=re.I):
                continue
            return n
        v = _first_number_after(text_ascii, m.end(), max_chars=max_chars, require_unit=False)
        if v != 0:
            return v
    return 0.0


def find_money_metric(text_ascii: str, labels: List[str], max_chars: int = 220) -> float:
    """Acha valores monetários de tabelas de resultados, exigindo unidade quando possível."""
    for label in labels:
        lp = label_pattern(label)
        m = re.search(lp, text_ascii, flags=re.I | re.S)
        if not m:
            continue
        trecho = text_ascii[m.end():m.end() + max_chars].replace("−", "-")
        money_re = re.compile(
            r"(-?\d{1,3}(?:\.\d{3})*(?:,\d+)?|-?\d+(?:,\d+)?|-?\d+\.\d+)\s*"
            r"(Bilhao|Bilhoes|Milhao|Milhoes|Mil)",
            re.I,
        )
        for mm in money_re.finditer(trecho):
            raw, unit = mm.group(1), mm.group(2)
            return to_float(raw + " " + unit)
        v = _first_number_after(text_ascii, m.end(), max_chars=max_chars, require_unit=False)
        if v != 0:
            return v
    return 0.0


def _validar_metricas_basicas(metrics: Dict[str, Any]) -> Dict[str, Any]:
    """Remove capturas claramente falsas para não contaminar valuation."""
    if to_float(metrics.get("pvp")) > 8:
        metrics["pvp"] = 0.0
    for k in ["cagr_receitas_5a_pct", "cagr_lucros_5a_pct"]:
        v = to_float(metrics.get(k))
        if v == 5.0:
            metrics[k] = 0.0
    for k in ["receita_liquida_12m", "lucro_liquido_12m", "ebitda_12m", "ebit_12m"]:
        v = to_float(metrics.get(k))
        if 0 < abs(v) < 100_000:
            metrics[k] = 0.0
    return metrics


def fetch_investidor10(ticker: str, classe_hint: str = "") -> Dict[str, Any]:
    tk = norm_ticker(ticker)
    classe_low = strip_accents(str(classe_hint or "").lower())

    # tenta rota mais provável primeiro.
    # Não trate todo ticker terminado em 11 como FII: TAEE11, KLBN11, SANB11 são units de ações.
    if any(x in classe_low for x in ["fii", "fundo", "fiagro"]):
        urls = [f"https://investidor10.com.br/fiis/{tk.lower()}/", f"https://investidor10.com.br/acoes/{tk.lower()}/"]
    elif any(x in classe_low for x in ["acao", "ações", "acoes", "stock"]):
        urls = [f"https://investidor10.com.br/acoes/{tk.lower()}/", f"https://investidor10.com.br/fiis/{tk.lower()}/"]
    else:
        # fallback: começa por ações para evitar confundir units com FIIs
        urls = [f"https://investidor10.com.br/acoes/{tk.lower()}/", f"https://investidor10.com.br/fiis/{tk.lower()}/"]

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
    }

    last_err = ""
    for url in urls:
        try:
            r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            if r.status_code != 200 or not r.text:
                last_err = f"HTTP {r.status_code} em {url}"
                continue
            raw = r.text
            text = clean_html_text(raw)
            text_ascii = strip_accents(text)

            # se a página retornou outra coisa ou busca genérica, pula
            if tk.lower() not in raw.lower() and tk not in text_ascii.upper():
                last_err = f"ticker não encontrado no HTML em {url}"
                continue

            metrics = {
                "fonte_url": url,
                "preco": find_metric(text_ascii, [f"{tk} cotacao", "cotacao", "preco atual"], max_chars=60),
                "pl": find_metric(text_ascii, ["P/L"], max_chars=45),
                "psr": find_metric(text_ascii, ["P/Receita (PSR)", "P/Receita", "PSR"], max_chars=55),
                "pvp": find_metric(text_ascii, ["P/VP", "P/VPA"], max_chars=45),
                "dy_pct": find_percent_metric(text_ascii, ["Dividend Yield", "DY"], max_chars=70),
                "payout_pct": find_percent_metric(text_ascii, ["Payout"], max_chars=70),
                "margem_liquida_pct": find_percent_metric(text_ascii, ["Margem Liquida"], max_chars=70),
                "margem_bruta_pct": find_percent_metric(text_ascii, ["Margem Bruta"], max_chars=70),
                "margem_ebit_pct": find_percent_metric(text_ascii, ["Margem Ebit"], max_chars=70),
                "margem_ebitda_pct": find_percent_metric(text_ascii, ["Margem Ebtida", "Margem Ebitda"], max_chars=70),
                "ev_ebitda": find_metric(text_ascii, ["EV/Ebitda", "EV/Ebtida"], max_chars=55),
                "ev_ebit": find_metric(text_ascii, ["EV/Ebit"], max_chars=55),
                "p_ebitda": find_metric(text_ascii, ["P/Ebitda", "P/Ebtida"], max_chars=55),
                "p_ebit": find_metric(text_ascii, ["P/Ebit"], max_chars=55),
                "p_ativo": find_metric(text_ascii, ["P/Ativo"], max_chars=55),
                "p_cap_giro": find_metric(text_ascii, ["P/Cap.Giro", "P/Cap Giro"], max_chars=55),
                "p_ativo_circ_liq": find_metric(text_ascii, ["P/Ativo Circ. Liq.", "P/Ativo Circ Liq"], max_chars=55),
                "vpa": find_metric(text_ascii, ["VPA"], max_chars=55),
                "lpa": find_metric(text_ascii, ["LPA"], max_chars=55),
                "giro_ativos": find_metric(text_ascii, ["Giro Ativos"], max_chars=55),
                "roe_pct": find_percent_metric(text_ascii, ["ROE"], max_chars=70),
                "roic_pct": find_percent_metric(text_ascii, ["ROIC"], max_chars=70),
                "roa_pct": find_percent_metric(text_ascii, ["ROA"], max_chars=70),
                "divida_liquida_patrimonio": find_metric(text_ascii, ["Divida Liquida / Patrimonio"], max_chars=65),
                "divida_liquida_ebitda": find_metric(text_ascii, ["Divida Liquida / Ebitda", "Divida Liquida / Ebtida"], max_chars=65),
                "divida_liquida_ebit": find_metric(text_ascii, ["Divida Liquida / Ebit"], max_chars=65),
                "divida_bruta_patrimonio": find_metric(text_ascii, ["Divida Bruta / Patrimonio"], max_chars=65),
                "patrimonio_ativos": find_metric(text_ascii, ["Patrimonio / Ativos"], max_chars=65),
                "passivos_ativos": find_metric(text_ascii, ["Passivos / Ativos"], max_chars=65),
                "liquidez_corrente": find_metric(text_ascii, ["Liquidez Corrente"], max_chars=65),
                "cagr_receitas_5a_pct": find_percent_metric(text_ascii, ["CAGR Receitas 5 anos", "CAGR Receitas"], max_chars=100),
                "cagr_lucros_5a_pct": find_percent_metric(text_ascii, ["CAGR Lucros 5 anos", "CAGR Lucros"], max_chars=100),
                "receita_liquida_12m": find_money_metric(text_ascii, ["Receita Liquida - (R$)", "Receita Liquida"], max_chars=260),
                "lucro_liquido_12m": find_money_metric(text_ascii, ["Lucro Liquido - (R$)", "Lucro Liquido"], max_chars=260),
                "ebitda_12m": find_money_metric(text_ascii, ["EBITDA - (R$)", "EBTIDA - (R$)", "EBITDA"], max_chars=260),
                "ebit_12m": find_money_metric(text_ascii, ["EBIT - (R$)", "EBIT"], max_chars=260),
            }

            metrics = _validar_metricas_basicas(metrics)

            # remove falso preço absurdo quando regex pega variação/ano; preço do app tem prioridade depois
            return {
                "status_coleta": "OK",
                "erro": "",
                **metrics,
            }
        except Exception as e:
            last_err = str(e)
            continue

    return {"status_coleta": "ERRO", "erro": last_err or "não coletado", "fonte_url": urls[0] if urls else ""}

# =============================================================================
# CONFIG E VALUATION
# =============================================================================
def default_config_for(row: Dict[str, Any]) -> Dict[str, Any]:
    tk = norm_ticker(row.get("ticker"))
    classe = str(row.get("classe") or "").strip()
    classe_low = strip_accents(classe.lower())

    if "fii" in classe_low or "fiagro" in classe_low:
        dy_alvo = 9.0
        pvp_max = 1.00
        roe_min = 0.0
        roic_min = 0.0
        div_max = 999.0
        payout_max = 999.0
    else:
        dy_alvo = 8.0
        pvp_max = 2.00
        roe_min = 12.0
        roic_min = 8.0
        div_max = 3.5
        payout_max = 90.0

    return {
        "ticker": tk,
        "ativo": "Sim",
        "classe": classe,
        "dy_alvo_pct": dy_alvo,
        "payout_conservador_pct": 70.0,
        "payout_base_pct": "",  # vazio = usa payout atual com teto
        "crescimento_lpa_conservador_pct": 0.0,
        "crescimento_lpa_base_pct": 0.0,
        "margem_seguranca_pct": 10.0,
        "pl_max": 15.0,
        "pvp_max": pvp_max,
        "divida_ebitda_max": div_max,
        "roe_min_pct": roe_min,
        "roic_min_pct": roic_min,
        "payout_max_pct": payout_max,
        "cagr_lucro_min_pct": 0.0,
        "usar_teto_mais_conservador": "Sim",
        "observacao": "config padrão automática",
        "atualizado_em": now_str(),
    }


def ensure_valuation_config(sh: gspread.Spreadsheet, universe: pd.DataFrame) -> pd.DataFrame:
    ws = ensure_ws(sh, ABA_CONFIG, VALUATION_CONFIG_HEADERS, rows=1000)
    df_cfg = read_df(sh, ABA_CONFIG)
    if df_cfg.empty:
        existing = set()
    else:
        df_cfg.columns = [str(c).strip() for c in df_cfg.columns]
        tk_col = pick_col(df_cfg, ["ticker"])
        existing = set(df_cfg[tk_col].apply(norm_ticker).tolist()) if tk_col else set()

    new_rows = []
    for _, r in universe.iterrows():
        tk = norm_ticker(r.get("ticker"))
        if not tk or tk in existing:
            continue
        cfg = default_config_for(r.to_dict())
        new_rows.append([cfg.get(h, "") for h in VALUATION_CONFIG_HEADERS])
        existing.add(tk)

    if new_rows:
        append_rows(ws, new_rows)
        print(f"  ✅ valuation_config: {len(new_rows)} ativo(s) novo(s) adicionados")
        df_cfg = read_df(sh, ABA_CONFIG)
    return df_cfg


def cfg_map_from_df(df_cfg: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    if df_cfg.empty:
        return {}
    df = df_cfg.copy()
    df.columns = [str(c).strip() for c in df.columns]
    tk_col = pick_col(df, ["ticker"])
    if not tk_col:
        return {}
    out = {}
    for _, r in df.iterrows():
        tk = norm_ticker(r.get(tk_col))
        if tk:
            out[tk] = r.to_dict()
    return out


def calc_quality_and_valuation(f: Dict[str, Any], cfg: Dict[str, Any], dpa_hist: float, preco_fallback: float) -> Dict[str, Any]:
    preco = to_float(f.get("preco")) or to_float(preco_fallback)
    lpa = to_float(f.get("lpa"))
    payout_atual = to_float(f.get("payout_pct"))
    dy_alvo = to_float(cfg.get("dy_alvo_pct")) or 8.0
    margem_seg = to_float(cfg.get("margem_seguranca_pct")) or 0.0

    # payout projetivo
    payout_cons = to_float(cfg.get("payout_conservador_pct"))
    if payout_cons <= 0:
        payout_cons = min(payout_atual or 70.0, 70.0)

    payout_base = to_float(cfg.get("payout_base_pct"))
    if payout_base <= 0:
        payout_base = min(payout_atual or 75.0, 85.0)

    cresc_cons = to_float(cfg.get("crescimento_lpa_conservador_pct")) / 100.0
    cresc_base = to_float(cfg.get("crescimento_lpa_base_pct")) / 100.0

    # histórico por proventos reais
    teto_hist = (dpa_hist / (dy_alvo / 100.0)) if dpa_hist > 0 and dy_alvo > 0 else 0.0

    # projetivo por LPA x payout
    dpa_cons = 0.0
    dpa_base = 0.0
    teto_cons = 0.0
    teto_base = 0.0
    if lpa > 0 and dy_alvo > 0:
        dpa_cons = lpa * (1 + cresc_cons) * (payout_cons / 100.0)
        dpa_base = lpa * (1 + cresc_base) * (payout_base / 100.0)
        teto_cons = dpa_cons / (dy_alvo / 100.0)
        teto_base = dpa_base / (dy_alvo / 100.0)

    use_conserv = is_yes(cfg.get("usar_teto_mais_conservador"), True)
    tetos_validos = [x for x in [teto_hist, teto_cons, teto_base] if x and x > 0]
    if not tetos_validos:
        teto_ref = 0.0
    elif use_conserv:
        teto_ref = min(tetos_validos)
    else:
        teto_ref = teto_cons or teto_hist or teto_base

    teto_com_margem = teto_ref * (1 - margem_seg / 100.0) if teto_ref > 0 else 0.0
    distancia = ((preco / teto_com_margem) - 1) * 100.0 if preco > 0 and teto_com_margem > 0 else 0.0

    # Score simples e auditável
    score = 100
    motivos = []

    pl = to_float(f.get("pl"))
    pvp = to_float(f.get("pvp"))
    roe = to_float(f.get("roe_pct"))
    roic = to_float(f.get("roic_pct"))
    div_ebitda = to_float(f.get("divida_liquida_ebitda"))
    cagr_lucro = to_float(f.get("cagr_lucros_5a_pct"))

    pl_max = to_float(cfg.get("pl_max")) or 15.0
    pvp_max = to_float(cfg.get("pvp_max")) or 999.0
    div_max = to_float(cfg.get("divida_ebitda_max")) or 999.0
    roe_min = to_float(cfg.get("roe_min_pct"))
    roic_min = to_float(cfg.get("roic_min_pct"))
    payout_max = to_float(cfg.get("payout_max_pct")) or 999.0
    cagr_min = to_float(cfg.get("cagr_lucro_min_pct"))

    if pl > 0 and pl > pl_max:
        score -= 8
        motivos.append(f"P/L acima do limite ({pl:.2f} > {pl_max:.2f})")
    if pvp > 0 and pvp > pvp_max:
        score -= 12
        motivos.append(f"P/VP acima do limite ({pvp:.2f} > {pvp_max:.2f})")
    if roe_min > 0 and roe > 0 and roe < roe_min:
        score -= 15
        motivos.append(f"ROE abaixo do mínimo ({roe:.2f}% < {roe_min:.2f}%)")
    if roic_min > 0 and roic > 0 and roic < roic_min:
        score -= 10
        motivos.append(f"ROIC abaixo do mínimo ({roic:.2f}% < {roic_min:.2f}%)")
    if div_ebitda > 0 and div_ebitda > div_max:
        score -= 18
        motivos.append(f"Dívida/EBITDA elevada ({div_ebitda:.2f} > {div_max:.2f})")
    if payout_atual > 0 and payout_atual > payout_max:
        score -= 12
        motivos.append(f"Payout acima do limite ({payout_atual:.2f}% > {payout_max:.2f}%)")
    if cagr_lucro and cagr_lucro < cagr_min:
        score -= 10
        motivos.append(f"CAGR lucros fraco ({cagr_lucro:.2f}% < {cagr_min:.2f}%)")
    if to_float(f.get("status_coleta")) == 0 and f.get("status_coleta") == "ERRO":
        score -= 20
        motivos.append("coleta de fundamentos falhou")

    score = max(0, min(100, score))
    if score >= 75:
        status_q = "SAUDÁVEL"
    elif score >= 55:
        status_q = "ATENÇÃO"
    else:
        status_q = "REVISAR/BLOQUEAR"

    if not is_yes(cfg.get("ativo"), True):
        status_val = "DESATIVADO"
    elif teto_com_margem <= 0 or preco <= 0:
        status_val = "SEM DADOS"
    elif preco <= teto_com_margem and score >= 75:
        status_val = "OPORTUNIDADE"
        motivos.append("preço abaixo do teto com margem e qualidade aceitável")
    elif preco <= teto_ref and score >= 55:
        status_val = "DENTRO DO TETO, SEM MARGEM"
        motivos.append("preço dentro do teto, mas sem margem de segurança")
    elif score < 55:
        status_val = "REVISAR TESE"
    else:
        status_val = "AGUARDAR PREÇO"

    return {
        "preco_atual": preco,
        "dpa_12m_historico": dpa_hist,
        "lpa_atual": lpa,
        "payout_atual_pct": payout_atual,
        "payout_conservador_pct": payout_cons,
        "payout_base_pct": payout_base,
        "dpa_proj_conservador": dpa_cons,
        "dpa_proj_base": dpa_base,
        "dy_alvo_pct": dy_alvo,
        "teto_historico": teto_hist,
        "teto_proj_conservador": teto_cons,
        "teto_proj_base": teto_base,
        "teto_com_margem": teto_com_margem,
        "margem_seguranca_pct": margem_seg,
        "distancia_teto_pct": distancia,
        "score_qualidade": score,
        "status_qualidade": status_q,
        "status_valuation": status_val,
        "motivos": "; ".join(motivos) if motivos else "sem alertas relevantes",
    }

# =============================================================================
# MAIN
# =============================================================================
def main() -> None:
    print("🚀 Fundamentos Job — cache + valuation projetivo")
    if not SHEET_ID:
        print("❌ SHEET_ID vazio.")
        sys.exit(1)

    sh = open_sheet()
    ensure_all_sheets(sh)

    print("📥 Lendo universo de ativos...")
    universe = load_universe(sh)
    if universe.empty:
        print("❌ Nenhum ativo encontrado em carteira_snapshot/ativos_master.")
        return
    print(f"  ✅ Ativos encontrados: {len(universe)}")

    print("📥 Calculando DPA/VPC 12m pela aba proventos...")
    dpa_map = calc_dpa_12m_from_proventos(sh)
    print(f"  ✅ DPA 12m calculado para {len(dpa_map)} ativo(s)")

    print("⚙️ Garantindo valuation_config...")
    df_cfg = ensure_valuation_config(sh, universe)
    cfg_map = cfg_map_from_df(df_cfg)

    cache_rows = []
    hist_rows = []
    valuation_rows = []
    atual = now_str()

    for i, (_, u) in enumerate(universe.iterrows(), start=1):
        tk = norm_ticker(u.get("ticker"))
        if not tk:
            continue
        nome = normalize_name(u.get("nome"))
        classe = str(u.get("classe") or "").strip()
        preco_app = to_float(u.get("preco"))

        print(f"🔎 [{i}/{len(universe)}] {tk} — coletando fundamentos...")
        f = fetch_investidor10(tk, classe)
        f["ticker"] = tk
        f["nome"] = nome
        f["classe"] = classe
        f["atualizado_em"] = atual

        # preço da carteira_snapshot tem prioridade se existir
        if preco_app > 0:
            f["preco"] = preco_app

        cache_rows.append([f.get(h, "") for h in FUNDAMENTOS_CACHE_HEADERS])

        for h in FUNDAMENTOS_CACHE_HEADERS:
            if h in {"ticker", "nome", "classe", "fonte_url", "status_coleta", "erro", "atualizado_em"}:
                continue
            val = f.get(h, "")
            if val not in ("", None) and to_float(val) != 0:
                hist_rows.append([atual, tk, classe, h, val, f.get("fonte_url", "")])

        cfg = cfg_map.get(tk, default_config_for(u.to_dict()))
        dpa_hist = float(dpa_map.get(tk, 0.0) or 0.0)
        val = calc_quality_and_valuation(f, cfg, dpa_hist, preco_app)

        merged = {
            "ticker": tk,
            "nome": nome,
            "classe": classe,
            **val,
            "pl": f.get("pl", 0),
            "pvp": f.get("pvp", 0),
            "roe_pct": f.get("roe_pct", 0),
            "roic_pct": f.get("roic_pct", 0),
            "divida_liquida_ebitda": f.get("divida_liquida_ebitda", 0),
            "cagr_lucros_5a_pct": f.get("cagr_lucros_5a_pct", 0),
            "atualizado_em": atual,
        }
        valuation_rows.append([merged.get(h, "") for h in VALUATION_RESULT_HEADERS])

        time.sleep(SLEEP_BETWEEN_TICKERS)

    print("💾 Salvando fundamentos_cache...")
    ws_cache = ensure_ws(sh, ABA_CACHE, FUNDAMENTOS_CACHE_HEADERS)
    write_replace(ws_cache, FUNDAMENTOS_CACHE_HEADERS, cache_rows)

    print("💾 Salvando valuation_resultado...")
    ws_result = ensure_ws(sh, ABA_RESULT, VALUATION_RESULT_HEADERS)
    write_replace(ws_result, VALUATION_RESULT_HEADERS, valuation_rows)

    print("💾 Acrescentando fundamentos_historico...")
    ws_hist = ensure_ws(sh, ABA_HIST, FUNDAMENTOS_HIST_HEADERS, rows=max(3000, len(hist_rows) + 1000))
    append_rows(ws_hist, hist_rows)

    ok = sum(1 for r in cache_rows if str(r[FUNDAMENTOS_CACHE_HEADERS.index("status_coleta")]).upper() == "OK")
    err = len(cache_rows) - ok
    print("\n✅ fundamentos_job concluído")
    print(f"   Ativos processados: {len(cache_rows)}")
    print(f"   Coletas OK: {ok} | Falhas: {err}")
    print(f"   Valuations gerados: {len(valuation_rows)}")


if __name__ == "__main__":
    main()
