#!/usr/bin/env python3
# jobs/radar_job.py
# -*- coding: utf-8 -*-
"""
RADAR DE OPORTUNIDADES — job automático

Função:
- cria/verifica as abas do Radar;
- lê carteira_snapshot + proventos;
- atualiza radar_config com ativos novos;
- grava snapshot em cotacoes_historico;
- gera eventos de queda/alta/entrada em zona de alerta;
- envia Telegram apenas para eventos novos e fora do cooldown.

Compatível com:
- GitHub Actions/single-user: usa SHEET_ID + GCP_SERVICE_ACCOUNT_JSON;
- VPS multiusuário: rodar_jobs.py injeta SHEET_ID de cada usuário.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
import hashlib
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

TELEGRAM_TOKEN = (
    os.getenv("TELEGRAM_TOKEN")
    or os.getenv("TELEGRAM_BOT_TOKEN")
    or ""
).strip()
TELEGRAM_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()

REQUEST_TIMEOUT = 20
TZ_LABEL = "America/Sao_Paulo"

ABA_CARTEIRA = "carteira_snapshot"
ABA_PROVENTOS = os.getenv("ABA_PROVENTOS_NOVO") or os.getenv("ABA_PROVENTOS") or "proventos"
ABA_CONFIG = "radar_config"
ABA_HIST = "cotacoes_historico"
ABA_EVENTOS = "radar_eventos"
ABA_ALERTAS = "radar_alertas_enviados"

RADAR_CONFIG_HEADERS = [
    "ticker", "nome", "classe", "monitorar", "enviar_telegram",
    "metodo_alerta", "dy_alvo_pct", "pvp_alvo", "preco_alerta_manual",
    "criterio_preco", "queda_dia_pct", "queda_7d_pct", "queda_30d_pct",
    "alta_7d_pct", "alta_30d_pct", "cooldown_dias", "status_estrategico",
    "atualizado_em",
]

COTACOES_HISTORICO_HEADERS = [
    "capturado_em", "ticker", "nome", "classe", "preco", "preco_medio",
    "quantidade", "custo_total", "valor_mercado", "resultado", "peso",
    "dpa_12m", "dy_se_comprar_hoje", "yoc_unitario", "teto_bazin_6",
    "pvp", "preco_alerta_sugerido", "fonte",
]

RADAR_EVENTOS_HEADERS = [
    "data_evento", "ticker", "nome", "classe", "tipo_evento", "nivel",
    "preco_atual", "preco_ref", "variacao_pct", "dpa_12m",
    "dy_atual_pct", "pvp", "preco_alerta_sugerido", "motivo",
    "enviado_telegram", "criado_por", "mensagem_hash",
]

RADAR_ALERTAS_HEADERS = [
    "data_envio", "ticker", "tipo_evento", "nivel", "preco_atual",
    "mensagem_hash", "canal",
]

SHEETS = {
    ABA_CONFIG: RADAR_CONFIG_HEADERS,
    ABA_HIST: COTACOES_HISTORICO_HEADERS,
    ABA_EVENTOS: RADAR_EVENTOS_HEADERS,
    ABA_ALERTAS: RADAR_ALERTAS_HEADERS,
}

# =============================================================================
# HELPERS
# =============================================================================
def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def norm_ticker(x: Any) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(x or "").upper().strip())


def to_float(v: Any) -> float:
    if v is None:
        return 0.0
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        try:
            return float(v)
        except Exception:
            return 0.0
    s = str(v).strip()
    if not s or s.lower() in {"nan", "nat", "none", "null", "—", "-"}:
        return 0.0
    s = s.replace("R$", "").replace("%", "").replace(" ", "")
    s = re.sub(r"[^0-9,\.\-]", "", s)
    if not s or s in {"-", ".", ","}:
        return 0.0
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def pct_to_decimal(v: Any) -> float:
    f = to_float(v)
    if abs(f) > 1.0:
        return f / 100.0
    return f


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


def _sha1(text: str, size: int = 16) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:size]


def _pick_col(df: pd.DataFrame, names: List[str]) -> Optional[str]:
    if df is None or df.empty:
        return None
    lower = {str(c).strip().lower(): c for c in df.columns}
    for n in names:
        if n.lower() in lower:
            return lower[n.lower()]
    return None


def _is_yes(v: Any, default: bool = True) -> bool:
    s = str(v if v is not None else ("Sim" if default else "Não")).strip().lower()
    return s in {"sim", "s", "true", "1", "yes", "y"}

# =============================================================================
# GOOGLE SHEETS
# =============================================================================
def _get_client() -> gspread.Client:
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


def _open_sheet() -> gspread.Spreadsheet:
    if not SHEET_ID:
        raise RuntimeError("SHEET_ID/SHEET_ID_NOVO vazio.")
    return _get_client().open_by_key(SHEET_ID)


def _ensure_ws(sh: gspread.Spreadsheet, title: str, headers: List[str], rows: int = 2000) -> gspread.Worksheet:
    try:
        ws = sh.worksheet(title)
    except Exception:
        ws = sh.add_worksheet(title=title, rows=rows, cols=max(20, len(headers)))
        ws.update("A1", [headers])
        print(f"✅ Aba criada: {title}")
        return ws

    try:
        current = ws.row_values(1)
    except Exception:
        current = []
    if not current:
        ws.update("A1", [headers])
    else:
        missing = [h for h in headers if h not in current]
        if missing:
            ws.update("A1", [current + missing])
            print(f"✅ Aba {title}: colunas adicionadas {missing}")
    return ws


def ensure_radar_tabs(sh: gspread.Spreadsheet) -> None:
    for title, headers in SHEETS.items():
        _ensure_ws(sh, title, headers)


def read_df(sh: gspread.Spreadsheet, title: str, headers: Optional[List[str]] = None) -> pd.DataFrame:
    try:
        ws = sh.worksheet(title)
        vals = ws.get_all_values()
        if not vals or len(vals) < 2:
            return pd.DataFrame(columns=headers or [])
        hdr = [str(h).strip() for h in vals[0]]
        rows = [r + [""] * (len(hdr) - len(r)) for r in vals[1:]]
        return pd.DataFrame(rows, columns=hdr)
    except Exception as e:
        print(f"⚠️ Não consegui ler aba {title}: {e}")
        return pd.DataFrame(columns=headers or [])


def replace_df(sh: gspread.Spreadsheet, title: str, df: pd.DataFrame, headers: List[str]) -> None:
    ws = _ensure_ws(sh, title, headers)
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(columns=headers)
    for h in headers:
        if h not in out.columns:
            out[h] = ""
    out = out[headers]
    values = [headers] + out.fillna("").astype(str).values.tolist()
    ws.clear()
    ws.update("A1", values, value_input_option="USER_ENTERED")


def append_dicts(sh: gspread.Spreadsheet, title: str, rows: List[Dict[str, Any]], headers: List[str]) -> None:
    if not rows:
        return
    ws = _ensure_ws(sh, title, headers)
    values = [[r.get(h, "") for h in headers] for r in rows]
    ws.append_rows(values, value_input_option="USER_ENTERED")

# =============================================================================
# NORMALIZAÇÃO DE DADOS
# =============================================================================
def normalize_carteira(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    d = df.copy()
    d.columns = [str(c).strip().lower() for c in d.columns]

    tk = _pick_col(d, ["ticker", "ativo", "codigo", "papel"])
    qtd = _pick_col(d, ["quantidade", "qtd", "cotas", "acoes", "ações"])
    pm = _pick_col(d, ["preco_medio", "preço_médio", "pm", "custo_medio", "custo_médio"])
    pa = _pick_col(d, ["preco_atual", "preço_atual", "cotacao", "cotação", "preco", "valor_atual"])
    nome = _pick_col(d, ["nome", "nome_ativo", "empresa", "descricao", "descrição"])
    classe = _pick_col(d, ["classe", "tipo_ativo", "tipo", "categoria"])
    custo = _pick_col(d, ["custo_total", "investido", "total_investido", "valor_investido"])
    valor = _pick_col(d, ["valor_mercado", "saldo_bruto", "valor_atual", "patrimonio", "patrimônio"])
    resultado = _pick_col(d, ["resultado", "pl", "lucro", "ganho"])
    peso = _pick_col(d, ["peso", "peso_pct", "percentual"])
    pvp = _pick_col(d, ["pvp", "p_vp", "p/vp", "p_vp_atual", "pvp_atual", "pvpa", "p_vpa"])

    if not tk:
        return pd.DataFrame()

    rows = []
    for _, r in d.iterrows():
        ticker = norm_ticker(r.get(tk))
        if not ticker:
            continue
        q = to_float(r.get(qtd)) if qtd else 0.0
        pmedio = to_float(r.get(pm)) if pm else 0.0
        patual = to_float(r.get(pa)) if pa else 0.0
        ctotal = to_float(r.get(custo)) if custo else q * pmedio
        vmercado = to_float(r.get(valor)) if valor else q * patual
        res = to_float(r.get(resultado)) if resultado else vmercado - ctotal
        rows.append({
            "ticker": ticker,
            "nome": str(r.get(nome, ticker) or ticker).strip() if nome else ticker,
            "classe": str(r.get(classe, "") or "").strip() if classe else "",
            "quantidade": q,
            "preco_medio": pmedio,
            "preco_atual": patual,
            "custo_total": ctotal,
            "valor_mercado": vmercado,
            "resultado": res,
            "peso": to_float(r.get(peso)) if peso else 0.0,
            "pvp": to_float(r.get(pvp)) if pvp else 0.0,
        })
    return pd.DataFrame(rows)


def normalize_proventos(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    out.columns = (
        out.columns.astype(str)
        .str.strip().str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("-", "_", regex=False)
    )
    rename = {
        "ativo": "ticker", "codigo": "ticker", "papel": "ticker",
        "data_pgto": "data_pagamento", "pagamento": "data_pagamento",
        "data_base": "data_com", "datacom": "data_com",
        "valor_liquido": "valor", "provento": "valor", "total": "valor",
        "valor_unitario": "valor_por_cota", "valor_cota": "valor_por_cota", "vpc": "valor_por_cota",
        "tipo_pagamento": "tipo", "evento": "tipo",
        "qtd": "quantidade_na_data", "quantidade": "quantidade_na_data",
    }
    out = out.rename(columns={k: v for k, v in rename.items() if k in out.columns})
    out = out.loc[:, ~out.columns.duplicated()]
    if "ticker" in out.columns:
        out["ticker"] = out["ticker"].fillna("").astype(str).str.upper().str.strip()
    for c in ["data", "data_pagamento", "data_com", "criado_em"]:
        if c in out.columns:
            out[c] = parse_dates(out[c])
    for c in ["valor", "valor_por_cota", "valor_bruto", "ir_retido", "quantidade_na_data", "quantidade_ref"]:
        if c in out.columns:
            out[c] = out[c].apply(to_float)
    if "tipo" in out.columns:
        out["tipo"] = out["tipo"].fillna("").astype(str).str.upper().str.strip()
    return out

# =============================================================================
# MÉTRICAS
# =============================================================================
def _pick_date_col(df: pd.DataFrame) -> Optional[str]:
    for c in ["data_pagamento", "pagamento", "data", "data_com", "data_evento"]:
        if c in df.columns:
            return c
    return None


def calc_dpa_12m(prov: pd.DataFrame, ticker: str) -> Tuple[float, int]:
    """Soma DPA/VPC dos últimos 12 meses reais, sem deixar compras recentes distorcerem."""
    if prov is None or prov.empty or "ticker" not in prov.columns:
        return 0.0, 0
    tk = norm_ticker(ticker)
    df = prov[prov["ticker"].astype(str).str.upper().str.strip().eq(tk)].copy()
    if df.empty:
        return 0.0, 0
    dc = _pick_date_col(df)
    if not dc:
        return 0.0, 0
    df[dc] = parse_dates(df[dc])
    df = df.dropna(subset=[dc])
    today = pd.Timestamp.today()
    start = today - pd.DateOffset(months=12)
    df = df[(df[dc] >= start) & (df[dc] <= today)]
    if df.empty:
        return 0.0, 0

    if "tipo" in df.columns:
        excluir = {"bonificação em frações", "bonificacao em fracoes", "aluguel", "bonificação", "bonificacao"}
        df = df[~df["tipo"].astype(str).str.lower().str.strip().isin(excluir)]
    if df.empty:
        return 0.0, 0

    if "valor_por_cota" in df.columns:
        df["_vpc"] = df["valor_por_cota"].apply(to_float)
    elif "valor" in df.columns and "quantidade_na_data" in df.columns:
        df["_vpc"] = df.apply(lambda r: to_float(r.get("valor")) / to_float(r.get("quantidade_na_data")) if to_float(r.get("quantidade_na_data")) > 0 else 0.0, axis=1)
    else:
        return 0.0, 0

    df = df[df["_vpc"] > 0].copy()
    if df.empty:
        return 0.0, 0

    # Dedup conservador: se a mesma linha exata aparecer duplicada, remove.
    keys = ["ticker", dc, "_vpc"]
    if "tipo" in df.columns:
        keys.append("tipo")
    df = df.drop_duplicates(subset=keys)

    return float(df["_vpc"].sum()), int(len(df))


def default_thresholds(classe: str) -> Dict[str, Any]:
    c = str(classe or "").upper()
    if "FII" in c or "FUNDO" in c or "FIAGRO" in c:
        return {
            "dy_alvo_pct": 8.5,
            "pvp_alvo": 0.95,
            "queda_dia_pct": -2.5,
            "queda_7d_pct": -5.0,
            "queda_30d_pct": -8.0,
            "alta_7d_pct": 6.0,
            "alta_30d_pct": 10.0,
        }
    return {
        "dy_alvo_pct": 8.0,
        "pvp_alvo": "",
        "queda_dia_pct": -4.0,
        "queda_7d_pct": -7.0,
        "queda_30d_pct": -12.0,
        "alta_7d_pct": 8.0,
        "alta_30d_pct": 15.0,
    }


def build_default_config(positions: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, r in positions.iterrows():
        tk = norm_ticker(r.get("ticker"))
        if not tk:
            continue
        th = default_thresholds(r.get("classe"))
        rows.append({
            "ticker": tk,
            "nome": r.get("nome", tk) or tk,
            "classe": r.get("classe", "") or "",
            "monitorar": "Sim",
            "enviar_telegram": "Sim",
            "metodo_alerta": "AUTO",
            "dy_alvo_pct": th["dy_alvo_pct"],
            "pvp_alvo": th["pvp_alvo"],
            "preco_alerta_manual": "",
            "criterio_preco": "MAIS_CONSERVADOR",
            "queda_dia_pct": th["queda_dia_pct"],
            "queda_7d_pct": th["queda_7d_pct"],
            "queda_30d_pct": th["queda_30d_pct"],
            "alta_7d_pct": th["alta_7d_pct"],
            "alta_30d_pct": th["alta_30d_pct"],
            "cooldown_dias": 7,
            "status_estrategico": "APORTE",
            "atualizado_em": now_str(),
        })
    return pd.DataFrame(rows, columns=RADAR_CONFIG_HEADERS)


def merge_config(cfg: pd.DataFrame, positions: pd.DataFrame) -> pd.DataFrame:
    if cfg is None or cfg.empty:
        return build_default_config(positions)
    cfg = cfg.copy()
    cfg.columns = [str(c).strip() for c in cfg.columns]
    for h in RADAR_CONFIG_HEADERS:
        if h not in cfg.columns:
            cfg[h] = ""
    cfg["ticker"] = cfg["ticker"].apply(norm_ticker)
    defaults = build_default_config(positions)
    existing = set(cfg["ticker"].dropna().astype(str))
    missing = defaults[~defaults["ticker"].isin(existing)]
    if not missing.empty:
        cfg = pd.concat([cfg[RADAR_CONFIG_HEADERS], missing[RADAR_CONFIG_HEADERS]], ignore_index=True)
    return cfg[RADAR_CONFIG_HEADERS]


def suggested_alert_price(m: Dict[str, Any], c: Dict[str, Any]) -> Tuple[float, str]:
    dpa = to_float(m.get("dpa_12m"))
    pa = to_float(m.get("preco"))
    pvp = to_float(m.get("pvp"))
    manual = to_float(c.get("preco_alerta_manual"))
    dy_alvo = pct_to_decimal(c.get("dy_alvo_pct"))
    pvp_alvo = to_float(c.get("pvp_alvo"))
    criterio = str(c.get("criterio_preco", "MAIS_CONSERVADOR") or "MAIS_CONSERVADOR").upper().strip()

    candidates: List[Tuple[str, float]] = []
    if dpa > 0 and dy_alvo > 0:
        candidates.append((f"DY alvo {dy_alvo*100:.2f}%", dpa / dy_alvo))
    if pa > 0 and pvp > 0 and pvp_alvo > 0:
        vp_cota = pa / pvp
        candidates.append((f"P/VP alvo {pvp_alvo:.2f}", vp_cota * pvp_alvo))
    if manual > 0:
        candidates.append(("Preço manual", manual))

    if not candidates:
        return 0.0, "sem critério"
    if criterio in {"MANUAL", "PRECO_MANUAL"} and manual > 0:
        return manual, "Preço manual"
    if criterio in {"DY", "DY_ALVO"}:
        for label, price in candidates:
            if label.startswith("DY alvo"):
                return price, label
    if criterio in {"PVP", "P/VP"}:
        for label, price in candidates:
            if label.startswith("P/VP"):
                return price, label
    label, price = min(candidates, key=lambda x: x[1])
    return float(price), f"mais conservador: {label}"


def build_metrics(pos: pd.DataFrame, prov: pd.DataFrame, cfg: pd.DataFrame) -> pd.DataFrame:
    cfg_map = {}
    if cfg is not None and not cfg.empty:
        for _, r in cfg.iterrows():
            cfg_map[norm_ticker(r.get("ticker"))] = r.to_dict()

    rows = []
    for _, r in pos.iterrows():
        tk = norm_ticker(r.get("ticker"))
        if not tk:
            continue
        pa = to_float(r.get("preco_atual"))
        pm = to_float(r.get("preco_medio"))
        qtd = to_float(r.get("quantidade"))
        dpa, eventos = calc_dpa_12m(prov, tk)
        dy = dpa / pa if pa > 0 else 0.0
        yoc = dpa / pm if pm > 0 else 0.0
        teto6 = dpa / 0.06 if dpa > 0 else 0.0
        base = {
            "ticker": tk,
            "nome": r.get("nome", tk) or tk,
            "classe": r.get("classe", "") or "",
            "preco": pa,
            "preco_medio": pm,
            "quantidade": qtd,
            "custo_total": to_float(r.get("custo_total")) or qtd * pm,
            "valor_mercado": to_float(r.get("valor_mercado")) or qtd * pa,
            "resultado": to_float(r.get("resultado")),
            "peso": to_float(r.get("peso")),
            "dpa_12m": dpa,
            "eventos_12m": eventos,
            "dy_se_comprar_hoje": dy,
            "yoc_unitario": yoc,
            "teto_bazin_6": teto6,
            "pvp": to_float(r.get("pvp")),
        }
        alert_price, reason = suggested_alert_price(base, cfg_map.get(tk, {}))
        base["preco_alerta_sugerido"] = alert_price
        base["criterio_alerta_usado"] = reason
        rows.append(base)
    return pd.DataFrame(rows)


def prepare_snapshot(metrics: pd.DataFrame) -> List[Dict[str, Any]]:
    ts = now_str()
    rows = []
    for _, r in metrics.iterrows():
        rows.append({
            "capturado_em": ts,
            "ticker": norm_ticker(r.get("ticker")),
            "nome": r.get("nome", ""),
            "classe": r.get("classe", ""),
            "preco": round(to_float(r.get("preco")), 4),
            "preco_medio": round(to_float(r.get("preco_medio")), 4),
            "quantidade": round(to_float(r.get("quantidade")), 8),
            "custo_total": round(to_float(r.get("custo_total")), 2),
            "valor_mercado": round(to_float(r.get("valor_mercado")), 2),
            "resultado": round(to_float(r.get("resultado")), 2),
            "peso": round(to_float(r.get("peso")), 4),
            "dpa_12m": round(to_float(r.get("dpa_12m")), 8),
            "dy_se_comprar_hoje": round(to_float(r.get("dy_se_comprar_hoje")), 8),
            "yoc_unitario": round(to_float(r.get("yoc_unitario")), 8),
            "teto_bazin_6": round(to_float(r.get("teto_bazin_6")), 4),
            "pvp": round(to_float(r.get("pvp")), 6),
            "preco_alerta_sugerido": round(to_float(r.get("preco_alerta_sugerido")), 4),
            "fonte": "RADAR_JOB",
        })
    return rows

# =============================================================================
# EVENTOS
# =============================================================================
def _history_prices(hist: pd.DataFrame, ticker: str) -> pd.DataFrame:
    if hist is None or hist.empty or "ticker" not in hist.columns:
        return pd.DataFrame()
    h = hist[hist["ticker"].astype(str).str.upper().str.strip().eq(norm_ticker(ticker))].copy()
    if h.empty:
        return h
    h["_dt"] = parse_dates(h["capturado_em"]) if "capturado_em" in h.columns else pd.NaT
    h["_preco"] = h["preco"].apply(to_float) if "preco" in h.columns else 0.0
    h = h.dropna(subset=["_dt"])
    h = h[h["_preco"] > 0].sort_values("_dt")
    return h


def price_before(hist_ticker: pd.DataFrame, days: int) -> float:
    if hist_ticker is None or hist_ticker.empty:
        return 0.0
    target = pd.Timestamp.now() - pd.Timedelta(days=days)
    older = hist_ticker[hist_ticker["_dt"] <= target]
    if not older.empty:
        return float(older.iloc[-1]["_preco"])
    return float(hist_ticker.iloc[0]["_preco"])


def event_hash(ticker: str, tipo: str, nivel: str, preco: float, motivo: str) -> str:
    raw = f"{norm_ticker(ticker)}|{tipo}|{nivel}|{round(preco, 2)}|{motivo[:120]}"
    return _sha1(raw, 16)


def generate_events(metrics: pd.DataFrame, cfg: pd.DataFrame, hist: pd.DataFrame) -> pd.DataFrame:
    if metrics is None or metrics.empty:
        return pd.DataFrame(columns=RADAR_EVENTOS_HEADERS)

    cfg_map = {}
    if cfg is not None and not cfg.empty:
        for _, r in cfg.iterrows():
            cfg_map[norm_ticker(r.get("ticker"))] = r.to_dict()

    events = []
    for _, mr in metrics.iterrows():
        tk = norm_ticker(mr.get("ticker"))
        cfg_r = cfg_map.get(tk, {})
        if not _is_yes(cfg_r.get("monitorar", "Sim"), True):
            continue
        pa = to_float(mr.get("preco"))
        if pa <= 0:
            continue

        h = _history_prices(hist, tk)
        p1 = price_before(h, 1)
        p7 = price_before(h, 7)
        p30 = price_before(h, 30)
        alert_price = to_float(mr.get("preco_alerta_sugerido"))
        dpa = to_float(mr.get("dpa_12m"))
        dy_atual = to_float(mr.get("dy_se_comprar_hoje"))
        pvp = to_float(mr.get("pvp"))

        checks = []
        if alert_price > 0 and pa <= alert_price:
            checks.append({
                "tipo_evento": "ENTROU_PRECO_ALERTA",
                "nivel": "OPORTUNIDADE",
                "preco_ref": alert_price,
                "variacao_pct": ((pa - alert_price) / alert_price) * 100 if alert_price > 0 else 0.0,
                "motivo": f"Preço atual abaixo do alerta sugerido ({mr.get('criterio_alerta_usado', '')}).",
            })

        for label, ref_price, th_col, tipo in [
            ("dia", p1, "queda_dia_pct", "QUEDA_DIA"),
            ("7 dias", p7, "queda_7d_pct", "QUEDA_7D"),
            ("30 dias", p30, "queda_30d_pct", "QUEDA_30D"),
        ]:
            if ref_price > 0:
                var = ((pa - ref_price) / ref_price) * 100
                th = to_float(cfg_r.get(th_col)) or default_thresholds(mr.get("classe", "")).get(th_col, 0)
                if var <= th:
                    nivel = "FORTE" if abs(var) >= abs(th) * 1.5 else "ATENÇÃO"
                    checks.append({
                        "tipo_evento": tipo,
                        "nivel": nivel,
                        "preco_ref": ref_price,
                        "variacao_pct": var,
                        "motivo": f"Queda de {var:.2f}% em {label}, limite configurado {th:.2f}%.",
                    })

        for label, ref_price, th_col, tipo in [
            ("7 dias", p7, "alta_7d_pct", "ALTA_7D"),
            ("30 dias", p30, "alta_30d_pct", "ALTA_30D"),
        ]:
            if ref_price > 0:
                var = ((pa - ref_price) / ref_price) * 100
                th = to_float(cfg_r.get(th_col)) or default_thresholds(mr.get("classe", "")).get(th_col, 0)
                if var >= th:
                    checks.append({
                        "tipo_evento": tipo,
                        "nivel": "ALTA",
                        "preco_ref": ref_price,
                        "variacao_pct": var,
                        "motivo": f"Alta de {var:.2f}% em {label}; pode ter saído da zona de compra.",
                    })

        prio = {"OPORTUNIDADE": 0, "FORTE": 1, "ATENÇÃO": 2, "ALTA": 3}
        checks = sorted(checks, key=lambda x: prio.get(x.get("nivel"), 9))[:2]
        for ev in checks:
            mh = event_hash(tk, ev["tipo_evento"], ev["nivel"], pa, ev["motivo"])
            events.append({
                "data_evento": now_str(),
                "ticker": tk,
                "nome": mr.get("nome", ""),
                "classe": mr.get("classe", ""),
                "tipo_evento": ev["tipo_evento"],
                "nivel": ev["nivel"],
                "preco_atual": round(pa, 4),
                "preco_ref": round(to_float(ev.get("preco_ref")), 4),
                "variacao_pct": round(to_float(ev.get("variacao_pct")), 4),
                "dpa_12m": round(dpa, 8),
                "dy_atual_pct": round(dy_atual * 100, 4),
                "pvp": round(pvp, 6),
                "preco_alerta_sugerido": round(alert_price, 4),
                "motivo": ev["motivo"],
                "enviado_telegram": "Não",
                "criado_por": "radar_job",
                "mensagem_hash": mh,
            })

    return pd.DataFrame(events, columns=RADAR_EVENTOS_HEADERS)


def filter_new_events(events: pd.DataFrame, eventos_antigos: pd.DataFrame, alertas: pd.DataFrame, cfg: pd.DataFrame) -> pd.DataFrame:
    if events is None or events.empty:
        return pd.DataFrame(columns=RADAR_EVENTOS_HEADERS)

    hashes_old = set()
    if eventos_antigos is not None and not eventos_antigos.empty and "mensagem_hash" in eventos_antigos.columns:
        hashes_old |= set(eventos_antigos["mensagem_hash"].astype(str))
    if alertas is not None and not alertas.empty and "mensagem_hash" in alertas.columns:
        hashes_old |= set(alertas["mensagem_hash"].astype(str))

    out = events[~events["mensagem_hash"].astype(str).isin(hashes_old)].copy()
    if out.empty:
        return out

    # Cooldown por ticker + tipo_evento. Evita spam quando preço muda um pouco e hash muda.
    cfg_map = {}
    if cfg is not None and not cfg.empty:
        for _, r in cfg.iterrows():
            cfg_map[norm_ticker(r.get("ticker"))] = r.to_dict()

    recent_pairs = set()
    base = alertas.copy() if alertas is not None else pd.DataFrame()
    if not base.empty and {"data_envio", "ticker", "tipo_evento"}.issubset(base.columns):
        base["_dt"] = parse_dates(base["data_envio"])
        for _, a in base.dropna(subset=["_dt"]).iterrows():
            tk = norm_ticker(a.get("ticker"))
            tipo = str(a.get("tipo_evento") or "")
            cd = int(to_float(cfg_map.get(tk, {}).get("cooldown_dias")) or 7)
            if a["_dt"] >= (pd.Timestamp.now() - pd.Timedelta(days=cd)):
                recent_pairs.add((tk, tipo))

    if recent_pairs:
        out = out[~out.apply(lambda r: (norm_ticker(r.get("ticker")), str(r.get("tipo_evento") or "")) in recent_pairs, axis=1)].copy()

    return out

# =============================================================================
# TELEGRAM
# =============================================================================
def send_telegram(msg: str) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ Telegram: TOKEN ou CHAT_ID não definidos")
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg},
            timeout=REQUEST_TIMEOUT,
        )
        if not r.ok:
            print(f"⚠️ Telegram erro {r.status_code}: {r.text[:200]}")
            return False
        return True
    except Exception as e:
        print(f"⚠️ Telegram exception: {e}")
        return False


def build_message(ev: Dict[str, Any]) -> str:
    tipo = str(ev.get("tipo_evento") or "")
    nivel = str(ev.get("nivel") or "")
    if nivel == "OPORTUNIDADE":
        title = "🟢 RADAR DE OPORTUNIDADE"
    elif nivel == "ALTA":
        title = "🚀 RADAR DE ALTA"
    elif nivel == "FORTE":
        title = "📉 RADAR — QUEDA FORTE"
    else:
        title = "🛰️ RADAR DE ATENÇÃO"

    tk = ev.get("ticker", "")
    nome = ev.get("nome", "") or tk
    preco = brl(ev.get("preco_atual"))
    ref = brl(ev.get("preco_ref")) if to_float(ev.get("preco_ref")) else "—"
    var = pct(ev.get("variacao_pct"))
    dy = pct(ev.get("dy_atual_pct"))
    pvp = to_float(ev.get("pvp"))
    pvp_txt = f"{pvp:.2f}".replace(".", ",") if pvp else "—"
    alerta = brl(ev.get("preco_alerta_sugerido")) if to_float(ev.get("preco_alerta_sugerido")) else "—"
    motivo = str(ev.get("motivo") or "")

    leitura = "Verificar concentração e tese antes de aportar."
    if nivel == "ALTA":
        leitura = "Ativo valorizou forte; evitar compra por impulso."
    elif nivel == "OPORTUNIDADE":
        leitura = "Entrou em zona de preço configurada; conferir caixa e concentração."

    return (
        f"{title}\n\n"
        f"{tk} — {nome}\n\n"
        f"Evento: {tipo}\n"
        f"Nível: {nivel}\n\n"
        f"💰 Preço atual: {preco}\n"
        f"📌 Preço referência: {ref}\n"
        f"📊 Variação: {var}\n"
        f"🎯 Preço alerta: {alerta}\n"
        f"💵 DY se comprar hoje: {dy}\n"
        f"🏢 P/VP: {pvp_txt}\n\n"
        f"Motivo: {motivo}\n\n"
        f"Leitura: {leitura}"
    )

# =============================================================================
# MAIN
# =============================================================================
def run() -> None:
    print("🛰️ Radar de Oportunidades — iniciando")
    print(f"🔐 SHEET_ID set? {'SIM' if SHEET_ID else 'NAO'} | GCP set? {'SIM' if GCP_JSON else 'NAO'} | Telegram set? {'SIM' if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID else 'NAO'}")

    sh = _open_sheet()
    ensure_radar_tabs(sh)

    df_carteira_raw = read_df(sh, ABA_CARTEIRA)
    df_prov_raw = read_df(sh, ABA_PROVENTOS)
    df_config_old = read_df(sh, ABA_CONFIG, RADAR_CONFIG_HEADERS)
    df_hist_old = read_df(sh, ABA_HIST, COTACOES_HISTORICO_HEADERS)
    df_eventos_old = read_df(sh, ABA_EVENTOS, RADAR_EVENTOS_HEADERS)
    df_alertas_old = read_df(sh, ABA_ALERTAS, RADAR_ALERTAS_HEADERS)

    pos = normalize_carteira(df_carteira_raw)
    if pos.empty:
        print("⚠️ carteira_snapshot vazia ou sem ticker. Rode proventos_job/dashboard antes do radar.")
        return
    prov = normalize_proventos(df_prov_raw)

    cfg = merge_config(df_config_old, pos)
    replace_df(sh, ABA_CONFIG, cfg, RADAR_CONFIG_HEADERS)
    print(f"✅ radar_config sincronizado: {len(cfg)} ativos")

    metrics = build_metrics(pos, prov, cfg)
    snapshot_rows = prepare_snapshot(metrics)

    # Eventos usam o histórico ANTES do snapshot atual.
    events_all = generate_events(metrics, cfg, df_hist_old)
    events_new = filter_new_events(events_all, df_eventos_old, df_alertas_old, cfg)

    # Envio Telegram respeitando configuração por ativo.
    cfg_map = {norm_ticker(r.get("ticker")): r.to_dict() for _, r in cfg.iterrows()}
    event_rows = []
    alert_rows = []
    sent = 0
    for _, ev in events_new.iterrows():
        evd = ev.to_dict()
        tk = norm_ticker(evd.get("ticker"))
        enviar = _is_yes(cfg_map.get(tk, {}).get("enviar_telegram", "Sim"), True)
        ok = False
        if enviar:
            ok = send_telegram(build_message(evd))
            if ok:
                sent += 1
                time.sleep(0.4)
        evd["enviado_telegram"] = "Sim" if ok else "Não"
        event_rows.append(evd)
        if ok:
            alert_rows.append({
                "data_envio": now_str(),
                "ticker": tk,
                "tipo_evento": evd.get("tipo_evento"),
                "nivel": evd.get("nivel"),
                "preco_atual": evd.get("preco_atual"),
                "mensagem_hash": evd.get("mensagem_hash"),
                "canal": "telegram",
            })

    append_dicts(sh, ABA_HIST, snapshot_rows, COTACOES_HISTORICO_HEADERS)
    append_dicts(sh, ABA_EVENTOS, event_rows, RADAR_EVENTOS_HEADERS)
    append_dicts(sh, ABA_ALERTAS, alert_rows, RADAR_ALERTAS_HEADERS)

    print(f"📊 Snapshot radar: {len(snapshot_rows)} ativos")
    print(f"🧭 Eventos detectados: {0 if events_all.empty else len(events_all)}")
    print(f"🆕 Eventos novos após anti-spam/cooldown: {len(event_rows)}")
    print(f"📨 Telegram enviados: {sent}")
    print("🏁 Radar concluído")


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print(f"❌ radar_job falhou: {e}")
        raise
