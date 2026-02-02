# utils/gsheets.py
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import unicodedata
import re
import time

import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials



# =============================================================================
# Secrets
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
# Escrita robusta
# =============================================================================
def _col_letter_from_index(idx_1based: int) -> str:
    if idx_1based < 1:
        return "A"
    idx_1based = min(idx_1based, 26)
    return chr(ord("A") + idx_1based - 1)


def _ensure_rows(ws, target_row: int, extra: int = 50):
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
        pass


def _find_next_row_anchor(ws, anchor_col_letter: str, max_scan: int = 8000) -> int:
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
# WRITERS — compatibilidade com pages/04_Adicionar_Operacao.py (base nova + espelho legado)
# =============================================================================
# Observação:
# - append_movimentacao / append_provento gravam na "base nova" (aba normalizada por header).
# - *_legado gravam no "espelho legado" (layout fixo por colunas), sem quebrar quando não existir.
# - get_ws_proventos_legado existe para compatibilidade (algumas páginas importam).

# Sheet/abas legado (se não existir, cai para a mesma planilha nova e não quebra)
SHEET_ID_LEGADO = _get_secret("SHEET_ID_LEGADO", "SHEET_ID_ANTIGO", "SHEET_ID_V4", "SHEET_ID_OLD", default=SHEET_ID)
ABA_MOVIMENTACOES_LEGADO = _get_secret("ABA_MOVIMENTACOES_LEGADO", "ABA_LANCAMENTOS_LEGADO", default="2. Lançamentos (B3)")
ABA_PROVENTOS_LEGADO = _get_secret("ABA_PROVENTOS_LEGADO", default="3. Proventos")


def get_ws_movimentacoes(show_error: bool = True):
    tab = (ABA_LANCAMENTOS or "movimentacoes").strip()
    return _open_ws(SHEET_ID, tab, show_error=show_error)


def get_ws_proventos(show_error: bool = True):
    tab = (ABA_PROVENTOS or "proventos").strip()
    return _open_ws(SHEET_ID, tab, show_error=show_error)


def get_ws_movimentacoes_legado(show_error: bool = False):
    tab = (ABA_MOVIMENTACOES_LEGADO or "movimentacoes").strip()
    return _open_ws(SHEET_ID_LEGADO, tab, show_error=show_error)


def get_ws_proventos_legado(show_error: bool = False):
    tab = (ABA_PROVENTOS_LEGADO or "proventos").strip()
    return _open_ws(SHEET_ID_LEGADO, tab, show_error=show_error)


def append_movimentacao(row: Dict[str, Any]) -> bool:
    """
    Grava 1 movimentação na base nova (aba de movimentações).
    Primeiro tenta por header (novo contrato).
    Se falhar, usa fallback por coluna fixa (blindagem).
    """
    try:
        ws = get_ws_movimentacoes(show_error=True)
        if not ws:
            return False

        r = _ensure_id_and_created_at(
            row,
            default_tipo=str((row or {}).get("tipo", "OPERACAO"))
        )

        # 1️⃣ tentativa principal (por header)
        ok = _append_row_by_header(ws, r)

        # 2️⃣ fallback automático (se header não bater)
        if not ok:
            next_row = _find_next_row_anchor(ws, "A")
            updates = {
                1: r.get("id"),
                2: r.get("data"),
                3: r.get("ticker"),
                4: r.get("tipo"),
                5: r.get("quantidade"),
                6: r.get("preco_unitario"),
                7: r.get("taxa"),
                8: r.get("valor_total"),
            }
            _sparse_update_cells(ws, next_row, updates)
            ok = True

        if ok:
            _clear_cached_reads()

        return bool(ok)

    except Exception:
        return False



def append_provento(row: Dict[str, Any]) -> bool:
    """
    Grava 1 provento na base nova (aba de proventos), por header.
    Espera chaves como: data, ticker, tipo, valor, quantidade_na_data, valor_por_cota...
    """
    try:
        ws = get_ws_proventos(show_error=True)
        if not ws:
            return False

        r = _ensure_id_and_created_at(row, default_tipo=str((row or {}).get("tipo", "PROVENTO")))
        ok = _append_row_by_header(ws, r)
        if ok:
            _clear_cached_reads()
        return bool(ok)
    except Exception:
        return False


def append_movimentacao_legado(row: Dict[str, Any]) -> bool:
    """
    Espelho legado (layout fixo conforme Contrato Seção 9):
    - C (3) ticker | D (4) data | E (5) tipo | I (9) quantidade | J (10) preco_unitario
    """
    try:
        # Tenta abrir a planilha
        ws = get_ws_movimentacoes_legado(show_error=False)
        
        # --- DIAGNÓSTICO DE ERRO ---
        if not ws:
            # Pega o nome que o sistema tentou buscar
            nome_aba = (ABA_MOVIMENTACOES_LEGADO or "movimentacoes").strip()
            # Mostra erro na tela para você ver
            st.error(f"❌ ERRO LEGADO: Não encontrei a aba '{nome_aba}' na planilha. Verifique o nome no secrets.toml ou na planilha.")
            return False
        # ---------------------------

        # Encontra próxima linha vazia baseada na coluna C (Ticker)
        next_row = _find_next_row_anchor(ws, "C", max_scan=8000)
        while _is_anchor_filled(ws, "C", next_row):
            next_row += 1

        # Tratamento do TIPO para o Dropdown do Legado
        tipo_raw = str((row or {}).get("tipo", "")).strip().upper()
        tipo_legado = "Compra" if tipo_raw == "COMPRA" else "Venda"
        
        if tipo_raw not in ["COMPRA", "VENDA"] and tipo_raw:
             tipo_legado = tipo_raw.title()

        updates = {
            3: str((row or {}).get("ticker", "")).strip().upper(),  # Coluna C
            4: str((row or {}).get("data", "")).strip(),            # Coluna D
            5: tipo_legado,                                         # Coluna E
            9: (row or {}).get("quantidade", 0),                    # Coluna I
            10: (row or {}).get("preco_unitario", 0),               # Coluna J
        }
        
        _sparse_update_cells(ws, next_row, updates)
        
        # Sucesso!
        st.toast(f"✅ Gravado no Legado (Linha {next_row})", icon="💾") 
        return True

    except Exception as e:
        st.error(f"❌ ERRO CRÍTICO LEGADO: {e}")
        return False
def _fmt_tipo_provento_legado(tipo: Any) -> str:
    """Mantém a planilha v4.5 feliz (dropdown/cálculos)."""
    s = str(tipo or "").strip()
    up = s.upper()
    if up in ("DIVIDENDO", "DIVIDENDOS"):
        return "Dividendo"
    if up in ("RENDIMENTO", "RENDIMENTOS"):
        return "Rendimento"
    if up in ("JCP", "J\u0301CP"):
        return "JCP"
    if up in ("AMORTIZACAO", "AMORTIZAÇÃO", "AMORTIZACAO", "AMORTIZA\u00c7\u00c3O"):
        return "Amortização"
    # fallback: capitaliza só a 1ª letra (evita tudo MAIÚSCULO)
    return s[:1].upper() + s[1:].lower() if s else ""



def append_provento_legado(row: Dict[str, Any], ws=None) -> bool:
    """
    Espelho legado (layout fixo - OTIMIZADO):
    - B ticker | C tipo | D data | E quantidade | G total líquido
    """
    try:
        if ws is None:
            # Tenta abrir (sem mostrar erro para não travar fluxo visual)
            ws = get_ws_proventos_legado(show_error=False)
        
        if not ws:
            # Se não achou, avisa no console/log mas não trava
            print("⚠️ Aviso: Aba de Proventos Legado não encontrada (3. Proventos).")
            return False

        # --- OTIMIZAÇÃO DE VELOCIDADE (TURBO) ---
        # Baixa a coluna B inteira de uma vez (apenas 1 chamada de API)
        col_b_values = _execute_with_retry(ws.col_values, 2) # 2 = Coluna B (Ticker)
        
        # A próxima linha vazia é o tamanho da lista + 1
        next_row = len(col_b_values) + 1
        
        # Garante que não escrevemos no cabeçalho
        if next_row < 2: 
            next_row = 2

        updates = {
            2: str((row or {}).get("ticker", "")).strip().upper(),         # Coluna B
            3: _fmt_tipo_provento_legado((row or {}).get("tipo", "")),      # Coluna C
            4: str((row or {}).get("data", "")).strip(),                   # Coluna D
            5: (row or {}).get("quantidade_na_data", (row or {}).get("quantidade", "")), # Coluna E
            7: (row or {}).get("valor", ""),                               # Coluna G
        }
        
        _sparse_update_cells(ws, next_row, updates)
        return True

    except Exception as e:
        print(f"Erro ao gravar provento legado: {e}")
        return False

# =============================================================================
# ALIAS PARA COMPATIBILIDADE (FIX: O ERRO ESTAVA AQUI)
# =============================================================================
def load_anuncios():
    """Alias para carregar proventos anunciados."""
    return load_proventos_anunciados()


# =============================================================================
# PROVENTOS ANUNCIADOS — CONTRATO ÚNICO (14 colunas) + UPSERT IDEMPOTENTE
# =============================================================================
# Regra: esta aba é fonte única para "anunciados" e deve ser compatível com jobs/proventos_job.py
# Colunas (ordem fixa):
PROVENTOS_ANUNCIADOS_HEADER_CONTRATO = [
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
    "event_id",
    "ativo",
    "atualizado_em",
    "version_hash",
]

_HEX40 = re.compile(r"^[a-f0-9]{40}$", re.I)

def _pa_norm_ticker(v: Any) -> str:
    if not v:
        return ""
    s = str(v).strip().upper()
    return re.sub(r"[^A-Z0-9]", "", s)

def _pa_norm_date(v: Any) -> str:
    if not v:
        return ""
    if hasattr(v, "strftime"):
        try:
            return v.strftime("%Y-%m-%d")
        except Exception:
            return ""
    s = str(v).strip()
    if not s:
        return ""
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    try:
        dt = datetime.fromisoformat(s.replace("Z", "").split(".")[0])
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""

def _pa_norm_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s:
        return None
    s = re.sub(r"[^0-9,.\-]", "", s)
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

def _pa_sha1(text: str) -> str:
    import hashlib
    return hashlib.sha1(text.encode("utf-8")).hexdigest()

def _pa_event_id(ticker: str, tipo_pagamento: str, data_com: str, data_pagamento: str) -> str:
    key = "|".join([_pa_norm_ticker(ticker), (tipo_pagamento or "").strip().upper(), _pa_norm_date(data_com), _pa_norm_date(data_pagamento)])
    return _pa_sha1(key)

def _pa_version_hash(event_id: str, valor_por_cota: Any, status: str) -> str:
    v = _pa_norm_float(valor_por_cota)
    vtxt = "" if v is None else f"{float(v):.8f}"
    key = "|".join([event_id, vtxt, (status or "").strip().upper()])
    return _pa_sha1(key)

def _pa_normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    r = dict(row or {})
    r["ticker"] = _pa_norm_ticker(r.get("ticker", ""))
    r["tipo_ativo"] = str(r.get("tipo_ativo", "") or "").strip()
    r["status"] = str(r.get("status", "ANUNCIADO") or "ANUNCIADO").strip().upper()
    r["tipo_pagamento"] = str(r.get("tipo_pagamento", "") or "").strip().upper()
    r["data_com"] = _pa_norm_date(r.get("data_com", ""))
    r["data_pagamento"] = _pa_norm_date(r.get("data_pagamento", ""))
    r["valor_por_cota"] = _pa_norm_float(r.get("valor_por_cota", None))
    r["quantidade_ref"] = r.get("quantidade_ref", "")
    r["fonte_url"] = str(r.get("fonte_url", "") or "").strip()
    r["capturado_em"] = str(r.get("capturado_em", "") or _now_iso_min())
    return r

def _pa_col_letter(col_idx_1based: int) -> str:
    s = ""
    n = col_idx_1based
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def _pa_a1(col_idx_1based: int, row_idx_1based: int) -> str:
    return f"{_pa_col_letter(col_idx_1based)}{row_idx_1based}"

def _pa_hmap(header: List[str]) -> Dict[str, int]:
    return {str(c).strip().lower(): i for i, c in enumerate(header, start=1)}

def ensure_proventos_anunciados_tab() -> bool:
    """Garante aba + header no contrato (14 colunas). Não insere linha; reescreve a linha 1."""
    try:
        gc = _gc()
        sh = _execute_with_retry(gc.open_by_key, SHEET_ID)
        tab = (ABA_PROVENTOS_ANUNCIADOS or "proventos_anunciados").strip()

        try:
            ws = sh.worksheet(tab)
        except Exception:
            ws = sh.add_worksheet(title=tab, rows=8000, cols=30)

        vals = _execute_with_retry(ws.get_all_values) or []
        if not vals:
            _execute_with_retry(ws.update, "1:1", [PROVENTOS_ANUNCIADOS_HEADER_CONTRATO], value_input_option="USER_ENTERED")
            return True

        cur = [str(x or "").strip().lower() for x in (vals[0] or [])]
        exp = [str(x or "").strip().lower() for x in PROVENTOS_ANUNCIADOS_HEADER_CONTRATO]

        # se header diferente, força (sem inserir linha)
        if cur[: len(exp)] != exp:
            _execute_with_retry(ws.update, "1:1", [PROVENTOS_ANUNCIADOS_HEADER_CONTRATO], value_input_option="USER_ENTERED")

        return True
    except Exception:
        return False

def append_provento_anunciado_batch(rows: List[Dict[str, Any]]) -> int:
    """
    UPSERT idempotente:
    - Dedup por event_id = sha1(ticker|tipo_pagamento|data_com|data_pagamento)
    - Update se version_hash mudou
    - Cura base: recalcula event_id com datas normalizadas e evita duplicação causada por IDs antigos
    Retorna: quantidade de linhas inseridas + atualizadas
    """
    if not rows:
        return 0

    if not ensure_proventos_anunciados_tab():
        return 0

    gc = _gc()
    sh = _execute_with_retry(gc.open_by_key, SHEET_ID)
    tab = (ABA_PROVENTOS_ANUNCIADOS or "proventos_anunciados").strip()
    ws = sh.worksheet(tab)

    header = PROVENTOS_ANUNCIADOS_HEADER_CONTRATO
    h = _pa_hmap(header)

    all_vals = _execute_with_retry(ws.get_all_values) or []
    if not all_vals:
        _execute_with_retry(ws.update, "1:1", [header], value_input_option="USER_ENTERED")
        all_vals = [header]

    # mapa do que existe — baseado no ID RECALCULADO (não confia no ID da célula)
    existing_row_by_eid: Dict[str, int] = {}
    existing_vhash_by_eid: Dict[str, str] = {}
    existing_eid_cell_by_row: Dict[int, str] = {}

    # correções/curas em lote
    cell_updates: List[Dict[str, Any]] = []

    idx_ticker = h["ticker"]
    idx_tipo = h["tipo_pagamento"]
    idx_dc = h["data_com"]
    idx_dp = h["data_pagamento"]
    idx_eid = h["event_id"]
    idx_ativo = h["ativo"]
    idx_upd = h["atualizado_em"]
    idx_vh = h["version_hash"]

    # varre linhas existentes e cura event_id/version_hash inconsistentes
    for ridx in range(2, len(all_vals) + 1):
        row = all_vals[ridx - 1] if ridx - 1 < len(all_vals) else []
        if not row or all(str(x).strip() == "" for x in row):
            continue

        ticker = row[idx_ticker - 1] if idx_ticker - 1 < len(row) else ""
        tipo = row[idx_tipo - 1] if idx_tipo - 1 < len(row) else ""
        dc = row[idx_dc - 1] if idx_dc - 1 < len(row) else ""
        dp = row[idx_dp - 1] if idx_dp - 1 < len(row) else ""
        eid_calc = _pa_event_id(ticker, tipo, dc, dp)

        eid_cell = row[idx_eid - 1] if idx_eid - 1 < len(row) else ""
        eid_cell = str(eid_cell).strip()

        # guarda para possíveis merges
        existing_eid_cell_by_row[ridx] = eid_cell

        # se já tem outro com mesmo eid_calc, consolida mantendo o mais novo (atualizado_em/capturado_em)
        if eid_calc in existing_row_by_eid:
            keep_row = existing_row_by_eid[eid_calc]
            # timestamps (ISO) — se faltar, cai para string vazia
            def _ts_of(rindex: int) -> str:
                r = all_vals[rindex - 1] if rindex - 1 < len(all_vals) else []
                ts = ""
                if idx_upd - 1 < len(r):
                    ts = str(r[idx_upd - 1]).strip()
                if not ts and h.get("capturado_em") and (h["capturado_em"] - 1) < len(r):
                    ts = str(r[h["capturado_em"] - 1]).strip()
                return ts

            ts_keep = _ts_of(keep_row)
            ts_new = _ts_of(ridx)

            # decide: mantém quem tiver timestamp maior (lexicográfico funciona em ISO YYYY-MM-DD HH:MM)
            if ts_new > ts_keep:
                drop_row, keep_row = keep_row, ridx
                existing_row_by_eid[eid_calc] = keep_row
            else:
                drop_row = ridx

            # soft delete do descartado
            cell_updates.append({"range": _pa_a1(idx_ativo, drop_row), "values": [[0]]})
            cell_updates.append({"range": _pa_a1(idx_upd, drop_row), "values": [[_now_iso_min()]]})
            continue

        existing_row_by_eid[eid_calc] = ridx
        vh_cell = row[idx_vh - 1] if idx_vh - 1 < len(row) else ""
        existing_vhash_by_eid[eid_calc] = str(vh_cell).strip()

        # cura: se event_id da célula está vazio ou diferente do recalculado, atualiza
        if (not eid_cell) or (eid_cell != eid_calc):
            cell_updates.append({"range": _pa_a1(idx_eid, ridx), "values": [[eid_calc]]})

    # prepara inserts/updates
    append_rows: List[List[Any]] = []
    inserted = 0
    updated = 0

    def _make_out(rn: Dict[str, Any], eid: str, vhash: str) -> List[Any]:
        out = [""] * len(header)
        out[h["ticker"] - 1] = rn.get("ticker", "")
        out[h["tipo_ativo"] - 1] = rn.get("tipo_ativo", "")
        out[h["status"] - 1] = rn.get("status", "")
        out[h["tipo_pagamento"] - 1] = rn.get("tipo_pagamento", "")
        out[h["data_com"] - 1] = rn.get("data_com", "")
        out[h["data_pagamento"] - 1] = rn.get("data_pagamento", "")
        out[h["valor_por_cota"] - 1] = "" if rn.get("valor_por_cota") is None else rn.get("valor_por_cota")
        out[h["quantidade_ref"] - 1] = rn.get("quantidade_ref", "")
        out[h["fonte_url"] - 1] = rn.get("fonte_url", "")
        out[h["capturado_em"] - 1] = rn.get("capturado_em", "")
        out[h["event_id"] - 1] = eid
        out[h["ativo"] - 1] = 1
        out[h["atualizado_em"] - 1] = _now_iso_min()
        out[h["version_hash"] - 1] = vhash
        return out

    for row in rows:
        rn = _pa_normalize_row(row)

        # mínimos
        if not rn["ticker"] or not rn["tipo_pagamento"] or not rn["data_com"]:
            continue

        eid = _pa_event_id(rn["ticker"], rn["tipo_pagamento"], rn["data_com"], rn["data_pagamento"])
        vhash = _pa_version_hash(eid, rn.get("valor_por_cota"), rn.get("status"))

        # INSERT
        if eid not in existing_row_by_eid:
            append_rows.append(_make_out(rn, eid, vhash))
            existing_row_by_eid[eid] = -1
            existing_vhash_by_eid[eid] = vhash
            inserted += 1
            continue

        # UPDATE
        sheet_row = existing_row_by_eid[eid]
        prev_vh = existing_vhash_by_eid.get(eid, "")

        # se era uma linha “soft deleted”, reativa
        if sheet_row > 0:
            # reativar
            cell_updates.append({"range": _pa_a1(idx_ativo, sheet_row), "values": [[1]]})

        # se não mudou, não atualiza
        if prev_vh and prev_vh == vhash:
            continue

        if sheet_row > 0:
            # atualiza campos principais + cura datas normalizadas na base
            updates = [
                ("ticker", rn["ticker"]),
                ("tipo_ativo", rn["tipo_ativo"]),
                ("status", rn["status"]),
                ("tipo_pagamento", rn["tipo_pagamento"]),
                ("data_com", rn["data_com"]),
                ("data_pagamento", rn["data_pagamento"]),
                ("valor_por_cota", "" if rn.get("valor_por_cota") is None else rn.get("valor_por_cota")),
                ("quantidade_ref", rn.get("quantidade_ref", "")),
                ("fonte_url", rn.get("fonte_url", "")),
                ("capturado_em", rn.get("capturado_em", "")),
                ("event_id", eid),
                ("atualizado_em", _now_iso_min()),
                ("version_hash", vhash),
            ]
            for col, val in updates:
                cidx = h.get(col.lower())
                if cidx:
                    cell_updates.append({"range": _pa_a1(cidx, sheet_row), "values": [[val]]})

            existing_vhash_by_eid[eid] = vhash
            updated += 1

    # grava inserts (chunks)
    if append_rows:
        CHUNK = 20
        for i in range(0, len(append_rows), CHUNK):
            _execute_with_retry(ws.append_rows, append_rows[i : i + CHUNK], value_input_option="USER_ENTERED")

    # grava curas/updates (batch)
    if cell_updates:
        _execute_with_retry(ws.batch_update, cell_updates)

    # limpa cache
    _clear_cached_reads()
    return int(inserted + updated)

def append_provento_anunciado(row: Dict[str, Any]) -> bool:
    return append_provento_anunciado_batch([row]) > 0

# --- COLE ISSO NO FINAL DO ARQUIVO utils/gsheets.py ---

# =============================================================================
# INBOX RI (Fatos Relevantes)
# =============================================================================
# Contrato da aba 'inbox_ri':
# Colunas: data, ticker, titulo, link, status, resumo_ia

ABA_INBOX_RI = "inbox_ri"

def ensure_inbox_ri_tab() -> bool:
    """Garante que a aba inbox_ri existe com o cabeçalho correto."""
    try:
        gc = _gc()
        sh = _execute_with_retry(gc.open_by_key, SHEET_ID)
        try:
            ws = sh.worksheet(ABA_INBOX_RI)
        except Exception:
            ws = sh.add_worksheet(title=ABA_INBOX_RI, rows=1000, cols=10)
            
        header = ["data", "ticker", "titulo", "link", "status", "resumo_ia"]
        
        vals = _execute_with_retry(ws.get_all_values)
        if not vals:
            _execute_with_retry(ws.update, "1:1", [header], value_input_option="USER_ENTERED")
        return True
    except Exception as e:
        print(f"Erro ao garantir inbox_ri: {e}")
        return False

def load_inbox_ri() -> pd.DataFrame:
    """Lê a caixa de entrada de RI."""
    ensure_inbox_ri_tab() # Garante que existe antes de ler
    return _read_ws_as_df(SHEET_ID, ABA_INBOX_RI, show_error=False)

def mark_ri_as_read(link: str, resumo: str):
    """Marca um item como LIDO e salva o resumo."""
    try:
        ws = _open_ws(SHEET_ID, ABA_INBOX_RI, show_error=False)
        if not ws: return False
        
        # Procura a linha pelo Link (coluna D = índice 4)
        cell = _execute_with_retry(ws.find, link, in_column=4)
        if cell:
            # Atualiza Status (Col E/5) e Resumo (Col F/6)
            row = cell.row
            updates = [
                {"range": f"E{row}", "values": [["LIDO"]]},
                {"range": f"F{row}", "values": [[resumo]]}
            ]
            _execute_with_retry(ws.batch_update, updates)
            _clear_cached_reads()
            return True
    except Exception as e:
        print(f"Erro ao atualizar RI: {e}")
        return False
    
# =============================================================================
# INBOX RI (Fatos Relevantes) - COLE NO FINAL DO ARQUIVO gsheets.py
# =============================================================================

ABA_INBOX_RI = "inbox_ri"

def ensure_inbox_ri_tab() -> bool:
    """Garante que a aba inbox_ri existe na planilha."""
    try:
        gc = _gc()
        sh = _execute_with_retry(gc.open_by_key, SHEET_ID)
        
        # Tenta pegar a aba, se não existir, cria
        try:
            ws = sh.worksheet(ABA_INBOX_RI)
        except Exception:
            ws = sh.add_worksheet(title=ABA_INBOX_RI, rows=1000, cols=10)
            
        # Garante o cabeçalho correto
        header = ["data", "ticker", "titulo", "link", "status", "resumo_ia"]
        vals = _execute_with_retry(ws.get_all_values)
        
        if not vals:
            _execute_with_retry(ws.update, "1:1", [header], value_input_option="USER_ENTERED")
            
        return True
    except Exception as e:
        st.error(f"Erro na aba RI: {e}")
        return False

def load_inbox_ri() -> pd.DataFrame:
    """Lê os dados da aba inbox_ri."""
    ensure_inbox_ri_tab() # Cria se não existir
    return _read_ws_as_df(SHEET_ID, ABA_INBOX_RI, show_error=False)

def mark_ri_as_read(link: str, resumo: str):
    """Marca um Fato Relevante como LIDO e salva o resumo da IA."""
    try:
        ws = _open_ws(SHEET_ID, ABA_INBOX_RI, show_error=False)
        if not ws: return False
        
        # Procura a linha usando o Link (Coluna D = 4) como chave
        cell = _execute_with_retry(ws.find, link, in_column=4)
        if cell:
            row = cell.row
            # Atualiza Status (E) e Resumo (F)
            updates = [
                {"range": f"E{row}", "values": [["LIDO"]]},
                {"range": f"F{row}", "values": [[resumo]]}
            ]
            _execute_with_retry(ws.batch_update, updates)
            _clear_cached_reads() # Limpa cache para atualizar na hora
            return True
    except Exception as e:
        print(f"Erro ao salvar RI: {e}")
        return False
# ... (Mantenha todo o código anterior do gsheets.py) ...

# =============================================================================
# MOMENTO DO APORTE (Abas e Loaders) - COLE NO FINAL DO ARQUIVO gsheets.py
# =============================================================================

ABA_WATCHLIST = _get_secret("ABA_WATCHLIST_APORTE", default="watchlist_aporte")
ABA_REGRAS = _get_secret("ABA_REGRAS_APORTE", default="regras_aporte")
ABA_ALERTAS = _get_secret("ABA_ALERTAS_ATIVOS", default="alertas_ativos")
ABA_LIMITES = _get_secret("ABA_LIMITES_APORTE", default="limites_aporte")

def ensure_aporte_tabs():
    """Garante abas de aporte com headers."""
    try:
        gc = _gc()
        sh = _execute_with_retry(gc.open_by_key, SHEET_ID)
        
        # Mapa: Nome da Aba -> Colunas
        defs = {
            ABA_WATCHLIST: ["ticker", "classe", "ativo"],
            ABA_REGRAS: ["classe", "criterio", "peso", "ativo"],
            ABA_ALERTAS: ["ticker", "classe", "severidade", "motivo", "ativo", "criado_em", "expira_em"],
            ABA_LIMITES: ["classe", "peso_alvo", "min_pct", "max_pct"]
        }

        for tab_name, header in defs.items():
            try:
                ws = sh.worksheet(tab_name)
            except:
                ws = sh.add_worksheet(title=tab_name, rows=100, cols=10)
            
            vals = _execute_with_retry(ws.get_all_values)
            if not vals:
                _execute_with_retry(ws.update, "1:1", [header], value_input_option="USER_ENTERED")
    except Exception as e:
        print(f"Erro Aporte Tabs: {e}")

@st.cache_data(show_spinner=False, ttl=600)
def load_watchlist_aporte() -> pd.DataFrame:
    return _read_ws_as_df(SHEET_ID, ABA_WATCHLIST, show_error=False)

@st.cache_data(show_spinner=False, ttl=600)
def load_regras_aporte() -> pd.DataFrame:
    return _read_ws_as_df(SHEET_ID, ABA_REGRAS, show_error=False)

@st.cache_data(show_spinner=False, ttl=600)
def load_alertas_ativos() -> pd.DataFrame:
    return _read_ws_as_df(SHEET_ID, ABA_ALERTAS, show_error=False)

@st.cache_data(show_spinner=False, ttl=600)
def load_limites_aporte() -> pd.DataFrame:
    return _read_ws_as_df(SHEET_ID, ABA_LIMITES, show_error=False)

# =============================================================================
# WATCHLIST APORTE — AUTO GERAR A PARTIR DA CARTEIRA
# =============================================================================

def _canon_classe_aporte(v: Any) -> str:
    s = str(v or "").strip().upper()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.replace("Ç", "C")

    if s in ("ACAO", "ACOES", "ACAOES", "AÇÃO", "AÇÕES"):
        return "AÇÕES"
    if s in ("FII", "FIIS"):
        return "FII"
    if s in ("FIAGRO", "FIAGROS"):
        return "FIAGRO"
    return str(v or "").strip()

def _to_float_ptbr(x: Any) -> float:
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if not s:
        return 0.0
    s = s.replace("R$", "").strip()
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0

def _write_df_to_ws_overwrite(sheet_id: str, tab_name: str, df: pd.DataFrame) -> bool:
    """
    Overwrite simples e robusto:
    - limpa a aba
    - escreve header + linhas via ws.update (USER_ENTERED)
    """
    try:
        gc = _gc()
        sh = _execute_with_retry(gc.open_by_key, sheet_id)
        try:
            ws = sh.worksheet(tab_name)
        except Exception:
            ws = sh.add_worksheet(title=tab_name, rows=max(200, len(df) + 50), cols=max(10, len(df.columns) + 5))

        _execute_with_retry(ws.clear)

        values = [list(df.columns)]
        if len(df) > 0:
            values += df.astype(str).values.tolist()

        _execute_with_retry(ws.update, "A1", values, value_input_option="USER_ENTERED")
        return True
    except Exception:
        return False

def sync_watchlist_from_carteira(
    overwrite: bool = False,
    only_classes: Tuple[str, ...] = ("FII", "FIAGRO", "AÇÕES"),
) -> Dict[str, Any]:
    """
    Gera a watchlist_aporte baseada na carteira (posições > 0):
    - calcula posição por ticker usando movimentacoes
    - classifica via ativos_master
    - escreve em watchlist_aporte

    overwrite=False: só escreve se watchlist estiver vazia
    overwrite=True : recria tudo
    """
    ensure_aporte_tabs()

    df_watch = load_watchlist_aporte()
    if df_watch is not None and not df_watch.empty and not overwrite:
        return {"written": False, "reason": "watchlist_aporte já possui linhas", "rows": int(len(df_watch))}

    df_mov = load_movimentacoes()
    df_mst = load_ativos()

    if df_mov is None or df_mov.empty:
        return {"written": False, "reason": "movimentacoes vazia", "rows": 0}
    if df_mst is None or df_mst.empty:
        return {"written": False, "reason": "ativos_master vazia", "rows": 0}

    mov = df_mov.copy()
    mov.columns = [str(c).strip().lower() for c in mov.columns]

    # tenta achar colunas mínimas (você pode ter aliases)
    # ticker:
    if "ticker" not in mov.columns:
        for alt in ["ativo", "codigo", "papel"]:
            if alt in mov.columns:
                mov["ticker"] = mov[alt]
                break
    # quantidade:
    if "quantidade" not in mov.columns:
        for alt in ["qtd"]:
            if alt in mov.columns:
                mov["quantidade"] = mov[alt]
                break

    if "ticker" not in mov.columns or "quantidade" not in mov.columns:
        return {"written": False, "reason": "movimentacoes sem ticker/quantidade", "rows": 0}

    mov["ticker"] = mov["ticker"].astype(str).str.strip().str.upper()
    mov["quantidade"] = mov["quantidade"].apply(_to_float_ptbr)

    # sinal de venda (se existir)
    tipo_col = None
    for c in ["tipo", "operacao", "tipo_operacao", "movimento"]:
        if c in mov.columns:
            tipo_col = c
            break
    if tipo_col:
        t = mov[tipo_col].astype(str).str.strip().str.upper()
        is_sell = t.isin(["VENDA", "V", "SELL"])
        mov.loc[is_sell, "quantidade"] = mov.loc[is_sell, "quantidade"] * -1.0

    pos = mov.groupby("ticker", as_index=False)["quantidade"].sum()
    pos = pos[pos["quantidade"] > 0].copy()
    if pos.empty:
        return {"written": False, "reason": "carteira sem posição > 0", "rows": 0}

    mst = df_mst.copy()
    mst.columns = [str(c).strip().lower() for c in mst.columns]
    if "ticker" not in mst.columns:
        for alt in ["ativo", "codigo", "papel"]:
            if alt in mst.columns:
                mst["ticker"] = mst[alt]
                break

    cls_col = None
    for c in ["classe", "tipo_ativo", "tipo", "categoria"]:
        if c in mst.columns:
            cls_col = c
            break
    if "ticker" not in mst.columns or cls_col is None:
        return {"written": False, "reason": "ativos_master sem ticker/classe", "rows": 0}

    mst["ticker"] = mst["ticker"].astype(str).str.strip().str.upper()
    mst["classe_norm"] = mst[cls_col].apply(_canon_classe_aporte)

    base = pos.merge(mst[["ticker", "classe_norm"]], on="ticker", how="left")
    base = base[base["classe_norm"].isin(list(only_classes))].copy()
    base = base.dropna(subset=["classe_norm"])

    out = pd.DataFrame(
        {
            "ticker": base["ticker"],
            "classe": base["classe_norm"],
            "ativo": base["ticker"],  # compatível com seu formato atual
            "desde_em": "",
            "ate_em": "",
        }
    )

    # ordena para ficar bonito
    out["classe"] = pd.Categorical(out["classe"], categories=["FII", "FIAGRO", "AÇÕES"], ordered=True)
    out = out.sort_values(["classe", "ticker"]).reset_index(drop=True)

    tab = (ABA_WATCHLIST or "watchlist_aporte").strip()
    ok = _write_df_to_ws_overwrite(SHEET_ID, tab, out)

    if ok:
        _clear_cached_reads()
        return {"written": True, "rows": int(len(out))}
    return {"written": False, "reason": "falha ao escrever watchlist_aporte", "rows": int(len(out))}
