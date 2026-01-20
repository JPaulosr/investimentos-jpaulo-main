# pages/Robô_Proventos_Anunciados.py
# -*- coding: utf-8 -*-

import re
import time
from datetime import datetime
import pandas as pd
import streamlit as st

from utils.gsheets import (
    ensure_proventos_anunciados_tab,
    append_provento_anunciado,
    append_provento_anunciado_batch,
    load_ativos,
    load_proventos_anunciados,
)

from utils.proventos_fetch import fetch_provento_anunciado


def norm_ticker(t: str) -> str:
    t = (t or "").upper().strip()
    t = re.sub(r"[^A-Z0-9]", "", t)
    return t

def now_iso_min() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M")

def _as_str(d, key, default=""):
    try:
        v = d.get(key, default)
        return "" if v is None else str(v)
    except Exception:
        return default

# pages/Robô_Proventos_Anunciados.py

def _to_float_safe(val):
    if val is None or val == "":
        return None
    if isinstance(val, (float, int)):
        return float(val)
    
    s = str(val).strip().replace("R$", "").strip()
    if not s:
        return None
        
    # Lógica robusta para PT-BR
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
        
    try:
        return float(s)
    except Exception:
        return None

def _fmt_br(val):
    v = _to_float_safe(val)
    if v is None:
        return "0,00"
    # Formata com vírgula para exibição na UI
    return f"{v:.8f}".replace(".", ",").rstrip("0").rstrip(",")

def _valor_ok(v) -> bool:
    vf = _to_float_safe(v)
    return vf is not None and vf > 0

def _fmt_br(val):
    v = _to_float_safe(val)
    if v is None:
        return ""
    s = f"{v:.8f}".rstrip("0").rstrip(".")
    return s.replace(".", ",")

def _norm_key_row(row: dict) -> str:
    def s(x): return str(x or "").strip().upper()
    def money(x):
        v = _to_float_safe(x)
        return "" if v is None else f"{v:.6f}"
    return "|".join([
        s(row.get("ticker")),
        s(row.get("tipo_pagamento")),
        s(row.get("data_com")),
        s(row.get("data_pagamento")),
        money(row.get("valor_por_cota")),
    ])

def _get_existing_keys_set() -> set:
    try:
        df = load_proventos_anunciados()
        if df is None or df.empty:
            return set()

        cols = {c.lower().strip(): c for c in df.columns}
        def getc(name): return cols.get(name, None)

        keys = set()
        for _, r in df.iterrows():
            rr = {
                "ticker": r.get(getc("ticker"), ""),
                "tipo_pagamento": r.get(getc("tipo_pagamento"), ""),
                "data_com": r.get(getc("data_com"), ""),
                "data_pagamento": r.get(getc("data_pagamento"), r.get(getc("data_pagament"), "")),
                "valor_por_cota": r.get(getc("valor_por_cota"), ""),
            }
            keys.add(_norm_key_row(rr))
        return keys
    except Exception:
        return set()

def _already_exists_local(row: dict) -> bool:
    return _norm_key_row(row) in _get_existing_keys_set()

def _build_row_from_fetch(payload: dict, ticker: str) -> dict:
    return {
        "ticker": norm_ticker(ticker),
        "tipo_ativo": _as_str(payload, "tipo_ativo", ""),
        "status": (_as_str(payload, "status", "ANUNCIADO") or "ANUNCIADO").upper(),
        "tipo_pagamento": (_as_str(payload, "tipo_pagamento", "") or "RENDIMENTO").upper(),
        "data_com": str(payload.get("data_com") or "").strip(),
        "data_pagamento": str(payload.get("data_pagamento") or "").strip(),
        "valor_por_cota": _to_float_safe(payload.get("valor_por_cota")),
        "quantidade_ref": _as_str(payload, "quantidade_ref", ""),
        "fonte_url": _as_str(payload, "fonte_url", ""),
        "capturado_em": _as_str(payload, "capturado_em", "") or now_iso_min(),
        "fonte_nome": _as_str(payload, "fonte_nome", ""),
    }

def _fetch_with_retry(ticker: str, logs: list, tries: int = 2, sleep_s: float = 0.8):
    last = []
    for i in range(1, tries + 1):
        logs.append(f"🔄 Tentativa {i}/{tries}...")
        last = fetch_provento_anunciado(ticker=ticker, logs=logs)
        if last:
            return last
        time.sleep(sleep_s)
    return last


st.set_page_config(layout="wide", page_title="Robô de Proventos Anunciados", page_icon="🤖")
st.title("🤖 Robô de Proventos Anunciados")

if "aba_prov_anunciados_ok" not in st.session_state:
    try:
        st.session_state["aba_prov_anunciados_ok"] = bool(ensure_proventos_anunciados_tab())
    except:
        st.session_state["aba_prov_anunciados_ok"] = False

st.session_state.setdefault("fetch_logs", [])
st.session_state.setdefault("last_fetch_row", None)
st.session_state.setdefault("last_fetch_results_list", [])
st.session_state.setdefault("ticker_in", "PETR4")
st.session_state.setdefault("data_com_in", "")
st.session_state.setdefault("data_pag_in", "")
st.session_state.setdefault("valor_pc_in", "")
st.session_state.setdefault("fonte_in", "")
st.session_state.setdefault("lote_resultados", [])


def on_buscar_click():
    st.session_state["last_fetch_row"] = None
    st.session_state["last_fetch_results_list"] = []

    t = norm_ticker(st.session_state.get("ticker_in", ""))
    if not t:
        st.session_state["fetch_logs"].append("❌ Ticker inválido.")
        return

    st.session_state["ticker_in"] = t
    st.session_state["fetch_logs"].append(f"Buscando: {t}")

    try:
        lista_payloads = _fetch_with_retry(ticker=t, logs=st.session_state["fetch_logs"], tries=2, sleep_s=0.8)
        if not lista_payloads:
            st.session_state["fetch_logs"].append("❌ Sem dados futuros encontrados.")
            return

        lista_rows = []
        for p in lista_payloads:
            r = _build_row_from_fetch(p, ticker=t)
            if not _valor_ok(r.get("valor_por_cota")):
                st.session_state["fetch_logs"].append(
                    f"⚠️ Ignorado (valor inválido): pag={r.get('data_pagamento')} tipo={r.get('tipo_pagamento')} val={r.get('valor_por_cota')}"
                )
                continue
            lista_rows.append(r)

        if not lista_rows:
            st.session_state["fetch_logs"].append("❌ Retorno veio sem valores válidos (evitei salvar 0).")
            return

        lista_rows.sort(key=lambda x: x.get("data_pagamento", "9999-99-99"))

        primeiro = lista_rows[0]
        st.session_state["data_com_in"] = primeiro.get("data_com", "")
        st.session_state["data_pag_in"] = primeiro.get("data_pagamento", "")
        st.session_state["valor_pc_in"] = _fmt_br(primeiro.get("valor_por_cota", ""))
        st.session_state["fonte_in"] = primeiro.get("fonte_url", "")
        st.session_state["last_fetch_row"] = primeiro
        st.session_state["last_fetch_results_list"] = lista_rows

        if len(lista_rows) > 1:
            st.session_state["fetch_logs"].append(f"⚠️ Encontrei {len(lista_rows)} pagamentos! Ao salvar, gravarei TODOS eles (lista abaixo).")
        else:
            st.session_state["fetch_logs"].append("✅ Dados preenchidos.")

    except Exception as e:
        st.session_state["fetch_logs"].append(f"❌ Erro: {e}")


def on_salvar_click():
    t = norm_ticker(st.session_state.get("ticker_in", ""))
    if not t:
        return

    lista_pendente = st.session_state.get("last_fetch_results_list", [])

    if lista_pendente and lista_pendente[0].get("ticker") == t:
        st.session_state["fetch_logs"].append(f"💾 Salvando {len(lista_pendente)} registros encontrados (Lote Manual)...")

        count_salvos = 0
        for item in lista_pendente:
            v = _to_float_safe(item.get("valor_por_cota"))
            if v is None or v <= 0:
                st.session_state["fetch_logs"].append(
                    f"⛔ Não salvo (valor inválido): {item.get('data_pagamento')} {item.get('tipo_pagamento')} val={item.get('valor_por_cota')}"
                )
                continue
            item["valor_por_cota"] = float(v)

            if _already_exists_local(item):
                st.session_state["fetch_logs"].append(f"⏭️ {item.get('data_pagamento')} ({item.get('tipo_pagamento')}) já existe.")
                continue

            if append_provento_anunciado(item):
                count_salvos += 1

        if count_salvos > 0:
            st.session_state["fetch_logs"].append(f"✅ {count_salvos} novos anúncios salvos!")
        else:
            st.session_state["fetch_logs"].append("🏁 Nada novo (ou tudo inválido/duplicado).")
        return

    v_manual = _to_float_safe(st.session_state.get("valor_pc_in", "").strip())
    if v_manual is None or v_manual <= 0:
        st.session_state["fetch_logs"].append("⛔ Não salvo: valor manual inválido/vazio.")
        return

    row = {
        "ticker": t,
        "tipo_ativo": "",
        "status": "ANUNCIADO",
        "tipo_pagamento": "RENDIMENTO",
        "data_com": st.session_state.get("data_com_in", "").strip(),
        "data_pagamento": st.session_state.get("data_pag_in", "").strip(),
        "valor_por_cota": float(v_manual),
        "quantidade_ref": "",
        "fonte_url": st.session_state.get("fonte_in", "").strip(),
        "capturado_em": now_iso_min(),
        "fonte_nome": "",
    }

    last = st.session_state.get("last_fetch_row")
    if isinstance(last, dict):
        for k in ["tipo_ativo", "tipo_pagamento", "status", "quantidade_ref", "fonte_nome"]:
            if not row.get(k) and last.get(k):
                row[k] = last.get(k)

    if _already_exists_local(row):
        st.session_state["fetch_logs"].append("⏭️ Já existe na base.")
        return

    if append_provento_anunciado(row):
        st.session_state["fetch_logs"].append("✅ Salvo com sucesso.")
    else:
        st.session_state["fetch_logs"].append("❌ Falha ao salvar.")


def on_buscar_lote_click():
    df_ativos = load_ativos()
    if df_ativos is None or df_ativos.empty:
        st.session_state["fetch_logs"].append("❌ Ativos vazio.")
        return

    cols = {c.lower().strip(): c for c in df_ativos.columns}
    c_ticker = cols.get("ticker") or cols.get("ativo")
    if not c_ticker:
        st.session_state["fetch_logs"].append("❌ Coluna ticker não encontrada.")
        return

    tickers = sorted({norm_ticker(x) for x in df_ativos[c_ticker].tolist() if norm_ticker(x)})
    salvar_auto = bool(st.session_state.get("lote_salvar_auto", True))

    existing_keys = _get_existing_keys_set()

    resultados = []
    rows_to_save = []

    progress_bar = st.progress(0)
    for idx, tk in enumerate(tickers):
        progress_bar.progress((idx + 1) / max(1, len(tickers)))
        try:
            logs_local = []
            lista_payloads = _fetch_with_retry(ticker=tk, logs=logs_local, tries=2, sleep_s=0.6)

            if not lista_payloads:
                resultados.append({"ticker": tk, "resultado": "SEM_DADOS"})
                continue

            for payload in lista_payloads:
                row = _build_row_from_fetch(payload, ticker=tk)

                if not _valor_ok(row.get("valor_por_cota")):
                    resultados.append({
                        "ticker": tk,
                        "resultado": "IGNORADO (VALOR INVÁLIDO)",
                        "data_pagamento": row.get("data_pagamento", ""),
                        "valor": row.get("valor_por_cota", ""),
                        "tipo": row.get("tipo_pagamento", ""),
                    })
                    continue

                key = _norm_key_row(row)
                if key in existing_keys:
                    status = "JA_EXISTE"
                else:
                    if salvar_auto:
                        rows_to_save.append(row)
                        existing_keys.add(key)
                        status = "NA_FILA_SAVE"
                    else:
                        status = "OK (NÃO SALVO)"

                resultados.append({
                    "ticker": tk,
                    "resultado": status,
                    "data_pagamento": row.get("data_pagamento", ""),
                    "valor": row.get("valor_por_cota", ""),
                    "tipo": row.get("tipo_pagamento", ""),
                })

        except Exception as e:
            resultados.append({"ticker": tk, "resultado": f"ERRO: {e}"})

    if rows_to_save:
        qtd = append_provento_anunciado_batch(rows_to_save)
        for r in resultados:
            if r["resultado"] == "NA_FILA_SAVE":
                r["resultado"] = "SALVO (LOTE)" if qtd > 0 else "FALHA (LOTE)"

    st.session_state["lote_resultados"] = resultados
    st.session_state["fetch_logs"].append("✅ Lote finalizado.")


# UI
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.text_input("Ticker", key="ticker_in")
with c2:
    st.text_input("Data Com", key="data_com_in")
with c3:
    st.text_input("Data Pagamento", key="data_pag_in")
with c4:
    st.text_input("Valor (visual BR)", key="valor_pc_in")
st.text_input("Fonte", key="fonte_in")

b1, b2, _ = st.columns([1, 1, 2])
with b1:
    st.button("🔎 Buscar", use_container_width=True, on_click=on_buscar_click)
with b2:
    st.button("💾 Salvar anúncio", use_container_width=True, on_click=on_salvar_click)

if st.session_state["fetch_logs"]:
    st.code("\n".join(st.session_state["fetch_logs"][-12:]))

st.divider()
st.subheader("📦 Lote Inteligente (Batch Save)")
cA, cB = st.columns([1, 3])
with cA:
    st.button("🚀 Rodar Lote", use_container_width=True, on_click=on_buscar_lote_click)
with cB:
    st.checkbox("Salvar automaticamente (Batch)", value=True, key="lote_salvar_auto")

if st.session_state["lote_resultados"]:
    st.dataframe(pd.DataFrame(st.session_state["lote_resultados"]), use_container_width=True)

st.divider()
st.subheader("Prévia do registro (para conferência)")
lista_preview = st.session_state.get("last_fetch_results_list", [])

if lista_preview:
    df_show = pd.DataFrame(lista_preview)
    if "valor_por_cota" in df_show.columns:
        df_show["valor_por_cota"] = df_show["valor_por_cota"].apply(_fmt_br)
    st.dataframe(df_show, use_container_width=True)
else:
    preview = {
        "ticker": norm_ticker(st.session_state.get("ticker_in", "")),
        "data_com": st.session_state.get("data_com_in", ""),
        "data_pagamento": st.session_state.get("data_pag_in", ""),
        "valor_por_cota": st.session_state.get("valor_pc_in", ""),
        "fonte_url": st.session_state.get("fonte_in", ""),
    }
    st.dataframe(pd.DataFrame([preview]), use_container_width=True)
