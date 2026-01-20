# utils/gsheets.py
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import secrets
import unicodedata
import re
import time

import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials


# =============================================================================
# Secrets (Somente Banco de Dados do APP)
# =============================================================================
def _get_secret(*keys: str, default=None):
    for k in keys:
        try:
            v = st.secrets[k]
            if v is not None and str(v).strip() != "":
                return v
        except Exception:
            pass
    return default


# APP DB (Planilha NOVA)
SHEET_ID = _get_secret("SHEET_ID_NOVO", "SHEET_ID")
ABA_ATIVOS = _get_secret("ABA_ATIVOS_NOVO", "ABA_ATIVOS", default="ativos_master")
ABA_LANCAMENTOS = _get_secret("ABA_MOVIMENTACOES_NOVO", "ABA_LANCAMENTOS", default="movimentacoes")
ABA_PROVENTOS = _get_secret("ABA_PROVENTOS_NOVO", "ABA_PROVENTOS", default="proventos")
ABA_COTACOES = _get_secret("ABA_COTACOES_NOVO", "ABA_COTACOES", default="cotacoes_cache")
ABA_PROVENTOS_ANUNCIADOS = _get_secret("ABA_PROVENTOS_ANUNCIADOS", default="proventos_anunciados")


# =============================================================================
# Helpers gerais
# =============================================================================
def _make_id(ticker: str, tipo: str, when: datetime | None = None) -> str:
    dt = when or datetime.now()
    ticker = (ticker or "").strip().upper()
    tipo = (tipo or "").strip().upper().replace(" ", "_")
    ts = dt.strftime("%Y%m%d_%H%M%S_%f")
    rand = secrets.token_hex(2).upper()
    return f"{ts}_{ticker or 'UNK'}_{tipo or 'REG'}_{rand}"


def _now_iso_min() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M")


def _ensure_id_and_created_at(row: Dict, default_tipo: str) -> Dict:
    row = dict(row or {})
    ticker = str(row.get("ticker", "")).strip().upper()
    tipo = str(row.get("tipo", "")).strip() or default_tipo
    if not str(row.get("id", "")).strip():
        row["id"] = _make_id(ticker=ticker, tipo=tipo)
    if not str(row.get("criado_em", "")).strip():
        row["criado_em"] = _now_iso_min()
    return row


def _clean_str(s):
    if not isinstance(s, str):
        return str(s)
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    s = re.sub(r"[^a-zA-Z0-9]", "", s)
    return s.lower()


def _to_float_safe(val):
    if val is None or val == "":
        return None
    if isinstance(val, (float, int)):
        return float(val)

    s = str(val).strip()
    if not s:
        return None

    s = s.replace("R$", "").strip()
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")

    try:
        return float(s)
    except Exception:
        return None


def _fmt_float_ptbr(v: float, decimals: int = 8) -> str:
    if v is None:
        return "0,00"
    s = "{:.{}f}".format(float(v), decimals).rstrip("0").rstrip(".")
    return s.replace(".", ",")


# =============================================================================
# Conexão Gspread (Retry + Cache)
# =============================================================================
@st.cache_resource(show_spinner=False)
def _gc():
    sa_info = (
        st.secrets.get("GCP_SERVICE_ACCOUNT")
        or st.secrets.get("gcp_service_account")
        or st.secrets.get("GOOGLE_SERVICE_ACCOUNT")
        or st.secrets.get("google_service_account")
    )

    if not sa_info:
        available = list(getattr(st.secrets, "keys", lambda: [])())
        raise KeyError(
            "Service Account não encontrada em st.secrets. "
            f"Chaves disponíveis: {available}"
        )

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
    return gspread.authorize(creds)


def _execute_with_retry(func, *args, **kwargs):
    retries = 3
    for i in range(retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if "429" in str(e) and i < retries - 1:
                time.sleep(2 * (i + 1))
                continue
            raise


def _open_ws(sheet_id: str, worksheet_name: str, show_error: bool = True):
    if not sheet_id:
        return None
    gc = _gc()
    try:
        sh = _execute_with_retry(gc.open_by_key, sheet_id)
        try:
            return sh.worksheet(worksheet_name)
        except Exception:
            pass

        target_clean = _clean_str(worksheet_name)
        for ws in sh.worksheets():
            if _clean_str(ws.title) == target_clean:
                return ws
        return None
    except Exception as e:
        if show_error:
            st.error(f"Erro ao abrir planilha {sheet_id}: {e}")
        return None


def _read_ws_as_df(sheet_id: str, worksheet_name: str, show_error: bool = True) -> pd.DataFrame:
    try:
        ws = _open_ws(sheet_id, worksheet_name, show_error=show_error)
        if not ws:
            return pd.DataFrame()
        values = _execute_with_retry(ws.get_all_values)
        if not values or len(values) < 2:
            return pd.DataFrame()
        headers = [str(h).strip() for h in values[0]]
        df = pd.DataFrame(values[1:], columns=headers)
        return df
    except Exception:
        return pd.DataFrame()


def _read_first_existing_ws(sheet_id: str, candidates: List[str], show_error: bool = True) -> Tuple[pd.DataFrame, str]:
    for name in candidates:
        df = _read_ws_as_df(sheet_id, name, show_error=show_error)
        if not df.empty:
            return df, name
    return pd.DataFrame(), "unknown"


# =============================================================================
# Header mapping
# =============================================================================
def _norm_header(s: str) -> str:
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.replace("ç", "c")
    s = re.sub(r"[^\w]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


_HEADER_ALIASES: Dict[str, List[str]] = {
    "classe": ["classe", "tipo_ativo", "tipoativo"],
    "ticker": ["ticker", "ativo", "codigo", "código", "papel"],
    "data": ["data", "data_dd_mm_yyyy", "dt", "data_operacao"],
    "tipo_operacao": ["tipo_operacao", "tipo_de_operacao", "operacao", "tipo"],
    "quantidade": ["quantidade", "qtd"],
    "preco_unitario": ["preco_unitario", "preco_por_unidade", "preco"],
    "taxa": ["taxa", "taxas", "custo"],
    "valor": ["valor", "valor_recebido"],
    "valor_por_cota": ["valor_por_cota", "valor_por_cota_r", "vpc"],
    "quantidade_na_data": ["quantidade_na_data", "quantidade", "qtd"],
    # Anunciados
    "tipo_ativo": ["tipo_ativo", "tipo ativo", "classe"],
    "status": ["status", "situacao"],
    "tipo_pagamento": ["tipo_pagamento", "tipo pagamento", "evento"],
    "data_com": ["data_com", "dt_com"],
    "data_pagamento": ["data_pagamento", "dt_pagamento"],
    "quantidade_ref": ["quantidade_ref", "qtd"],
    "fonte_url": ["fonte_url", "url"],
    "capturado_em": ["capturado_em", "criado_em"],
    "fonte_nome": ["fonte_nome", "fonte"],
}


# =============================================================================
# Escrita robusta (sem insert_row) + anti "grid limits"
# =============================================================================
def _col_letter_from_index(idx_1based: int) -> str:
    if idx_1based < 1:
        return "A"
    idx_1based = min(idx_1based, 26)
    return chr(ord("A") + idx_1based - 1)


def _ensure_rows(ws, target_row: int, extra: int = 50):
    """
    Garante que o worksheet tenha pelo menos target_row linhas.
    Evita erro: Range exceeds grid limits.
    """
    try:
        target_row = int(target_row or 1)
        extra = int(extra or 0)
        current = int(getattr(ws, "row_count", 0) or 0)
        if current <= 0:
            current = 1000
        if target_row > current:
            new_rows = target_row + max(0, extra)
            _execute_with_retry(ws.resize, rows=new_rows)
    except Exception:
        # deixa estourar no update caso a API recuse resize
        pass


def _find_next_row_anchor(ws, anchor_col_letter: str, max_scan: int = 8000) -> int:
    """Acha próxima linha livre olhando apenas a coluna âncora (rápido)."""
    anchor_col_letter = (anchor_col_letter or "A").strip().upper()
    rng = f"{anchor_col_letter}2:{anchor_col_letter}{max_scan}"
    col_vals = _execute_with_retry(ws.get, rng) or []
    last = 1
    for i, row in enumerate(col_vals, start=2):
        v = ""
        if isinstance(row, list) and row:
            v = str(row[0]).strip()
        if v and v.lower() != "nan":
            last = i
    return int(last + 1)


def _find_next_row_anchor_from(ws, anchor_col_letter: str, start_row: int = 2, max_scan: int = 8000) -> int:
    """Acha próxima linha livre olhando a coluna âncora, começando em start_row."""
    anchor_col_letter = (anchor_col_letter or "A").strip().upper()
    start_row = max(2, int(start_row or 2))
    rng = f"{anchor_col_letter}{start_row}:{anchor_col_letter}{max_scan}"
    col_vals = _execute_with_retry(ws.get, rng) or []
    last = start_row - 1
    for i, row in enumerate(col_vals, start=start_row):
        v = ""
        if isinstance(row, list) and row:
            v = str(row[0]).strip()
        if v and v.lower() != "nan":
            last = i
    return int(last + 1)


def _is_anchor_filled(ws, anchor_col_letter: str, row_index: int) -> bool:
    try:
        v = _execute_with_retry(ws.acell, f"{anchor_col_letter}{row_index}").value
        return bool(str(v or "").strip())
    except Exception:
        return False


def _sparse_update_cells(ws, row_index: int, updates: Dict[int, Any]) -> None:
    """
    Atualiza só colunas necessárias, na mesma linha.
    Antes garante linhas suficientes (anti grid limit).
    """
    _ensure_rows(ws, row_index, extra=50)
    for col_idx, v in updates.items():
        if v is None:
            continue
        if isinstance(v, str) and v.strip() == "":
            continue
        _execute_with_retry(ws.update_cell, row_index, col_idx, v)


def _append_row_by_header(ws, row_dict: Dict, value_input_option: str = "USER_ENTERED") -> bool:
    if not ws:
        return False

    header = _execute_with_retry(ws.row_values, 1) or []
    header_norm = [_norm_header(str(h)) for h in header]
    if len([h for h in header_norm if h]) < 2:
        return False

    row_norm = {_norm_header(str(k)): v for k, v in (row_dict or {}).items()}

    anchor_letter = "A"
    if "ticker" in header_norm:
        anchor_letter = _col_letter_from_index(header_norm.index("ticker") + 1)

    next_row = _find_next_row_anchor(ws, anchor_letter, max_scan=8000)
    while _is_anchor_filled(ws, anchor_letter, next_row):
        next_row += 1

    updates: Dict[int, Any] = {}
    for idx_1based, hn in enumerate(header_norm, start=1):
        val = None
        if hn in row_norm:
            val = row_norm.get(hn)

        if val is None:
            for canon, aliases in _HEADER_ALIASES.items():
                aliases_norm = [_norm_header(a) for a in aliases]
                if hn in aliases_norm:
                    cand_keys = [_norm_header(canon)] + aliases_norm
                    for ck in cand_keys:
                        if ck in row_norm and row_norm.get(ck) is not None:
                            val = row_norm.get(ck)
                            break
                if val is not None:
                    break

        if val is None:
            continue
        if isinstance(val, str) and val.strip() == "":
            continue

        updates[idx_1based] = val

    if not updates:
        return False

    _sparse_update_cells(ws, next_row, updates)
    return True


# =============================================================================
# Cache loaders
# =============================================================================
def _clear_cached_reads():
    try:
        load_movimentacoes.clear()
        load_proventos.clear()
        load_ativos.clear()
        load_cotacoes.clear()
        load_proventos_anunciados.clear()
    except Exception:
        pass


@st.cache_data(show_spinner=False, ttl=600)
def load_ativos():
    df, _ = _read_first_existing_ws(SHEET_ID, [str(ABA_ATIVOS).strip(), "ativos_master"])
    return df


@st.cache_data(show_spinner=False, ttl=600)
def load_movimentacoes():
    df, _ = _read_first_existing_ws(SHEET_ID, [str(ABA_LANCAMENTOS).strip(), "movimentacoes"])
    return df


@st.cache_data(show_spinner=False, ttl=600)
def load_proventos():
    df, _ = _read_first_existing_ws(SHEET_ID, [str(ABA_PROVENTOS).strip(), "proventos"])
    return df


@st.cache_data(show_spinner=False, ttl=600)
def load_cotacoes() -> pd.DataFrame:
    df, _ = _read_first_existing_ws(SHEET_ID, [str(ABA_COTACOES).strip(), "cotacoes_cache"])
    return df


@st.cache_data(show_spinner=False, ttl=600)
def load_proventos_anunciados() -> pd.DataFrame:
    tab = (ABA_PROVENTOS_ANUNCIADOS or "proventos_anunciados").strip()
    return _read_ws_as_df(SHEET_ID, tab, show_error=False)


# =============================================================================
# PROVENTOS ANUNCIADOS
# =============================================================================
PROVENTOS_ANUNCIADOS_HEADERS = [
    "ticker",
    "tipo_ativo",
    "status",
    "tipo_pagamento",
    "data_com",
    "data_pagamento",
    "valor_por_cota",
    "quantidade_ref",
    "fonte_url",
    "capturado_em",
    "fonte_nome",
]


def ensure_proventos_anunciados_tab() -> bool:
    gc = _gc()
    try:
        sh = _execute_with_retry(gc.open_by_key, SHEET_ID)
        tab = (ABA_PROVENTOS_ANUNCIADOS or "proventos_anunciados").strip()
        try:
            ws = sh.worksheet(tab)
        except Exception:
            ws = sh.add_worksheet(title=tab, rows=2000, cols=15)
        current = _execute_with_retry(ws.get_values, "A1:K1") or [[]]
        if len([x for x in current[0] if x]) < 3:
            _execute_with_retry(
                ws.update,
                "A1",
                [PROVENTOS_ANUNCIADOS_HEADERS],
                value_input_option="USER_ENTERED",
            )
        return True
    except Exception:
        return False


def _prov_anun_key(row: Dict) -> str:
    t = str((row or {}).get("ticker", "")).strip().upper()
    tp = str((row or {}).get("tipo_pagamento", "")).strip().upper()
    dc = str((row or {}).get("data_com", "")).strip()
    dp = str((row or {}).get("data_pagamento", "")).strip()
    return f"{t}|{tp}|{dc}|{dp}"


def _find_existing_row_index(ws, key: str) -> int:
    try:
        values = _execute_with_retry(ws.get_all_values)
        if not values or len(values) < 2:
            return 0
        header = [str(h).strip() for h in values[0]]
        idx = {h.lower().strip(): i for i, h in enumerate(header)}
        req = ["ticker", "tipo_pagamento", "data_com", "data_pagamento"]
        if any(r not in idx for r in req):
            return 0
        for i, row in enumerate(values[1:], start=2):
            r = {
                "ticker": row[idx["ticker"]] if idx["ticker"] < len(row) else "",
                "tipo_pagamento": row[idx["tipo_pagamento"]] if idx["tipo_pagamento"] < len(row) else "",
                "data_com": row[idx["data_com"]] if idx["data_com"] < len(row) else "",
                "data_pagamento": row[idx["data_pagamento"]] if idx["data_pagamento"] < len(row) else "",
            }
            if _prov_anun_key(r) == key:
                return i
        return 0
    except Exception:
        return 0


def append_provento_anunciado(row: Dict) -> bool:
    try:
        if not ensure_proventos_anunciados_tab():
            return False
        tab = (ABA_PROVENTOS_ANUNCIADOS or "proventos_anunciados").strip()
        ws = _open_ws(SHEET_ID, tab, show_error=False)
        if not ws:
            return False

        r = dict(row or {})
        r["ticker"] = str(r.get("ticker", "")).upper().strip()
        if not r.get("capturado_em"):
            r["capturado_em"] = _now_iso_min()

        val_num = _to_float_safe(r.get("valor_por_cota"))
        if val_num is None or val_num <= 0:
            return False
        r["valor_por_cota"] = _fmt_float_ptbr(val_num)

        key = _prov_anun_key(r)
        existing_idx = _find_existing_row_index(ws, key)
        if existing_idx:
            header = _execute_with_retry(ws.row_values, 1) or []
            idx = {str(h).strip().lower(): i for i, h in enumerate(header)}
            if "valor_por_cota" in idx:
                col = idx["valor_por_cota"] + 1
                _execute_with_retry(ws.update_cell, existing_idx, col, r["valor_por_cota"])
                _clear_cached_reads()
                return True
            return False

        ok = _append_row_by_header(ws, r, value_input_option="USER_ENTERED")
        if ok:
            _clear_cached_reads()
            return True
        return False
    except Exception:
        return False


# =============================================================================
# ESCRITA PRINCIPAL (BASE NOVA)
# =============================================================================
def append_provento(row: Dict) -> bool:
    success = False
    row = _ensure_id_and_created_at(row, default_tipo="PROVENTO")
    row_clean = row.copy()

    for col in ["valor", "quantidade_na_data", "valor_por_cota"]:
        if col in row_clean:
            val = _to_float_safe(row_clean[col])
            row_clean[col] = _fmt_float_ptbr(val)

    try:
        tab = (ABA_PROVENTOS or "proventos").strip()
        ws = _open_ws(SHEET_ID, tab, show_error=True)
        if ws and _append_row_by_header(ws, row_clean):
            success = True
            st.toast("✅ Provento Salvo!", icon="💰")
    except Exception as e:
        st.error(f"❌ Erro ao salvar provento: {e}")

    if success:
        load_proventos.clear()
    return success


def append_movimentacao(row: Dict) -> bool:
    success = False
    row = _ensure_id_and_created_at(row, default_tipo="OPERACAO")
    row_clean = row.copy()

    for col in ["quantidade", "preco_unitario", "valor_total", "taxa"]:
        if col in row_clean:
            val = _to_float_safe(row_clean[col])
            row_clean[col] = _fmt_float_ptbr(val)

    try:
        tab = (ABA_LANCAMENTOS or "movimentacoes").strip()
        ws = _open_ws(SHEET_ID, tab, show_error=True)
        if ws and _append_row_by_header(ws, row_clean):
            success = True
            st.toast("✅ Operação Salva!", icon="💾")
    except Exception as e:
        st.error(f"❌ Erro ao salvar operação: {e}")

    if success:
        load_movimentacoes.clear()
    return success


# =============================================================================
# LEGADO — OPERAÇÕES (Espelho)
# Colunas típicas: C ticker, D data, E tipo (Compra/Venda), I qtd, J preço
# =============================================================================
def get_ws_movs_legado():
    sheet_id = st.secrets.get("SHEET_ID_PRINCIPAL")
    aba = st.secrets.get("ABA_PRINCIPAL_MOVIMENTACOES")
    if not sheet_id or not aba:
        raise RuntimeError("Secrets ausentes: SHEET_ID_PRINCIPAL / ABA_PRINCIPAL_MOVIMENTACOES")
    ws = _open_ws(sheet_id, aba, show_error=True)
    if not ws:
        raise RuntimeError("Não foi possível abrir worksheet legado (movimentacoes).")
    return ws


def append_movimentacao_legado(row: Dict, ws=None, state: Optional[Dict] = None) -> bool:
    """
    Espelho para planilha antiga (movimentações).
    Otimizado: se passar ws/state, evita scan a cada linha.
    """
    try:
        if ws is None:
            ws = get_ws_movs_legado()
        if state is None:
            state = {}

        # âncora: coluna C (ticker)
        if not state.get("next_row"):
            state["next_row"] = _find_next_row_anchor(ws, "C", max_scan=8000)
        next_row = int(state["next_row"])

        tipo_in = str(row.get("tipo", "")).strip().upper()
        tipo_legado = {"COMPRA": "Compra", "VENDA": "Venda"}.get(tipo_in, "Compra")

        updates = {
            3: str(row.get("ticker", "")).upper().strip(),  # C
            4: str(row.get("data", "")).strip(),            # D
            5: tipo_legado,                                  # E
            9: _fmt_float_ptbr(_to_float_safe(row.get("quantidade"))),       # I
            10: _fmt_float_ptbr(_to_float_safe(row.get("preco_unitario"))),  # J
        }

        _sparse_update_cells(ws, next_row, updates)
        state["next_row"] = next_row + 1
        return True

    except Exception as e:
        st.error(f"Erro ao salvar na base antiga (movimentacoes): {e}")
        return False


# =============================================================================
# LEGADO — PROVENTOS (Espelho)
# B ticker | C tipo | D data | E quantidade | F unitário (NÃO escreve) | G total
# =============================================================================
def get_ws_proventos_legado():
    sheet_id = st.secrets.get("SHEET_ID_PRINCIPAL")
    aba = st.secrets.get("ABA_PRINCIPAL_PROVENTOS")  # ex: "3. Proventos"
    if not sheet_id or not aba:
        raise RuntimeError("Secrets ausentes: SHEET_ID_PRINCIPAL / ABA_PRINCIPAL_PROVENTOS")
    ws = _open_ws(sheet_id, aba, show_error=True)
    if not ws:
        raise RuntimeError("Não foi possível abrir worksheet legado (proventos).")
    return ws


def append_provento_legado(row: Dict, ws=None, state: Optional[Dict] = None) -> bool:
    """
    Planilha antiga (v4.5) — Aba: 3. Proventos
    Preenche APENAS:
      B ticker
      C tipo provento
      D data
      E quantidade
      G total líquido
    NÃO preencher F (Unitário).
    """
    try:
        if ws is None:
            ws = get_ws_proventos_legado()
        if state is None:
            state = {}

        # ✅ começa depois do cabeçalho. Seus dados começam na linha 4.
        if not state.get("next_row"):
            state["next_row"] = _find_next_row_anchor_from(ws, "B", start_row=4, max_scan=8000)
        next_row = int(state["next_row"])

        tipo_in = str(row.get("tipo", "")).strip().upper()
        tipo_legado = {
            "RENDIMENTO": "Rendimento",
            "DIVIDENDO": "Dividendo",
            "JCP": "JCP",
            "AMORTIZACAO": "Amortização",
            "AMORTIZAÇÃO": "Amortização",
        }.get(tipo_in, "Rendimento")

        updates = {
            2: str(row.get("ticker", "")).upper().strip(),                      # B
            3: tipo_legado,                                                     # C
            4: str(row.get("data", "")).strip(),                                # D
            5: _fmt_float_ptbr(_to_float_safe(row.get("quantidade_na_data"))),  # E
            7: _fmt_float_ptbr(_to_float_safe(row.get("valor"))),               # G
        }

        _sparse_update_cells(ws, next_row, updates)
        state["next_row"] = next_row + 1
        return True

    except Exception as e:
        st.error(f"Erro ao salvar provento na base antiga: {e}")
        return False
