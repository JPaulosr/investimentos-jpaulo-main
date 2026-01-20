# sync_ativos.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import pandas as pd
import unicodedata
from datetime import datetime


# -------------------------
# Normalização
# -------------------------
def _norm_text(s: str) -> str:
    s = str(s).strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s

def _norm_col(c: str) -> str:
    c = _norm_text(c).lower().strip()
    c = c.replace(" ", "_").replace("-", "_")
    while "__" in c:
        c = c.replace("__", "_")
    return c

def _upper_clean(x) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if s.lower() in ("", "nan", "none", "null", "-"):
        return ""
    return s.upper().strip()


def _find_header_row(values: list[list[str]]) -> int | None:
    """
    Catálogo do Ricardo tem textos acima do cabeçalho.
    Acha a linha do cabeçalho procurando por 'CÓDIGO' (ou CODIGO) e/ou 'NOME'.
    Retorna o índice (0-based) da linha de cabeçalho.
    """
    for i, row in enumerate(values):
        row_norm = [_norm_text(x).upper().strip() for x in row]
        if "CÓDIGO" in row_norm or "CODIGO" in row_norm:
            return i
        # fallback (se alguém mudar o catálogo)
        if "CODIGO" in row_norm and "CLASSE DO ATIVO" in row_norm:
            return i
    return None


# -------------------------
# 1) Sync: compras -> ativos (só tickers)
# -------------------------
def sync_ativos_from_compras(gc, sheet_id: str) -> dict:
    """
    Garante que 'ativos' exista e contenha todos tickers vistos em 'compras'.
    Não inventa classe/subtipo/segmento.
    """
    sh = gc.open_by_key(sheet_id)
    ws_compras = sh.worksheet("compras")
    ws_ativos = sh.worksheet("ativos")

    compras_vals = ws_compras.get_all_values()
    if len(compras_vals) < 2:
        return {"ok": False, "msg": "Aba 'compras' está vazia (sem linhas de dados)."}

    compras = pd.DataFrame(compras_vals[1:], columns=compras_vals[0])
    compras.columns = [_norm_col(c) for c in compras.columns]

    if "ticker" not in compras.columns:
        return {"ok": False, "msg": "Coluna 'ticker' não encontrada em 'compras'."}

    tickers = (
        compras["ticker"].astype(str).map(_upper_clean)
        .replace("", pd.NA).dropna().unique().tolist()
    )
    tickers = sorted(tickers)

    # schema padrão
    headers = ["ticker", "classe", "subtipo", "segmento", "moeda", "ativo", "criado_em"]
    ativos_vals = ws_ativos.get_all_values()
    if not ativos_vals:
        ws_ativos.append_row(headers)
        ativos_vals = [headers]

    # se cabeçalho divergente, normaliza
    current_headers = [_norm_col(x) for x in ativos_vals[0]]
    if current_headers != headers:
        ws_ativos.clear()
        ws_ativos.append_row(headers)
        ativos_vals = [headers]

    if len(ativos_vals) >= 2:
        ativos = pd.DataFrame(ativos_vals[1:], columns=ativos_vals[0])
        ativos.columns = [_norm_col(c) for c in ativos.columns]
    else:
        ativos = pd.DataFrame(columns=headers)

    existentes = set(ativos["ticker"].astype(str).map(_upper_clean)) if not ativos.empty else set()
    novos = [t for t in tickers if t not in existentes]

    if not novos:
        return {"ok": True, "msg": "Nenhum ticker novo. 'ativos' já está sincronizada.", "novos": 0}

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = [[t, "", "", "", "BRL", "1", now] for t in novos]
    ws_ativos.append_rows(rows, value_input_option="RAW")
    return {"ok": True, "msg": f"'ativos' sincronizada: {len(novos)} tickers novos.", "novos": len(novos)}


# -------------------------
# 2) Enriquecer: catálogo -> ativos (classe + segmento + subtipo)
# -------------------------
def enrich_ativos_from_catalog(gc, sheet_id_ativos: str, catalog_sheet_id: str) -> dict:
    import pandas as pd
    import unicodedata

    def norm(s):
        s = str(s).strip().lower()
        s = unicodedata.normalize("NFKD", s)
        s = "".join(c for c in s if not unicodedata.combining(c))
        return s.replace(" ", "_")

    # === abre planilha do app ===
    sh_app = gc.open_by_key(sheet_id_ativos)
    ws_ativos = sh_app.worksheet("ativos")

    ativos_raw = ws_ativos.get_all_values()
    if len(ativos_raw) < 2:
        return {"ok": False, "msg": "Aba 'ativos' vazia."}

    ativos = pd.DataFrame(ativos_raw[1:], columns=ativos_raw[0])
    ativos.columns = [norm(c) for c in ativos.columns]
    ativos["ticker"] = ativos["ticker"].str.upper().str.strip()

    # === abre catálogo ===
    sh_cat = gc.open_by_key(catalog_sheet_id)
    # tenta achar a aba certa do catálogo (tolerante a "12. Base de Dados", etc.)
    all_titles = [ws.title for ws in sh_cat.worksheets()]
    target = None
    for t in all_titles:
        if "base" in t.lower() and "dados" in t.lower():
            target = t
            break
    if not target:
        return {"ok": False, "msg": f"Não achei a aba do catálogo. Abas disponíveis: {all_titles}"}

    ws_cat = sh_cat.worksheet(target)


    cat_raw = ws_cat.get_all_values()
    catalog = pd.DataFrame(cat_raw[1:], columns=cat_raw[0])
    # normaliza nomes das colunas do catálogo (remove acento, espaço, etc.)
    import unicodedata

    def _norm_col(c):
        c = str(c).strip().lower()
        c = unicodedata.normalize("NFKD", c)
        c = "".join(ch for ch in c if not unicodedata.combining(ch))
        c = c.replace(" ", "_")
        return c

    catalog.columns = [_norm_col(c) for c in catalog.columns]

    # acha a coluna "codigo" (pode vir como: "codigo", "código", "ticker", "cod", etc.)
    possible = ["codigo", "cod", "ticker", "cdiigo", "ativo", "code"]
    code_col = next((c for c in possible if c in catalog.columns), None)

    if not code_col:
        return {"ok": False, "msg": f"Catálogo sem coluna de código. Colunas: {list(catalog.columns)}"}

    catalog[code_col] = catalog[code_col].astype(str).str.upper().str.strip()

    catalog.columns = [norm(c) for c in catalog.columns]

    catalog["codigo"] = catalog["codigo"].str.upper().str.strip()

    # === merge ===
    merged = ativos.merge(
        catalog,
        left_on="ticker",
        right_on="codigo",
        how="left",
        suffixes=("", "_cat"),
    )

    # === preenchimento controlado ===
    def fill(col_app, col_cat):
        if col_app not in merged:
            merged[col_app] = ""
        merged[col_app] = merged[col_app].where(
            merged[col_app].astype(str).str.strip() != "",
            merged[col_cat],
        )

    fill("classe", "classe_do_ativo")
    fill("subtipo", "subsetor")
    fill("segmento", "segmento")

    out = merged[ativos.columns]

    # === grava de volta ===
    ws_ativos.clear()
    ws_ativos.append_row(out.columns.tolist())
    ws_ativos.append_rows(out.values.tolist(), value_input_option="RAW")

    return {
        "ok": True,
        "msg": f"Ativos enriquecidos: classe {out['classe'].ne('').sum()}/{len(out)}, "
               f"subtipo {out['subtipo'].ne('').sum()}/{len(out)}, "
               f"segmento {out['segmento'].ne('').sum()}/{len(out)}"
    }
