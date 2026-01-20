# pages/07_Momento_do_Aporte.py
# -*- coding: utf-8 -*-
"""
MOMENTO DO APORTE — Investimentos MD (CONTRATO)

✅ Página de "freio emocional"
- Não recomenda compra/venda
- Ranqueia candidatos por regras determinísticas e explicáveis
- Separação absoluta: FII/FIAGRO vs AÇÕES
- Simulador obrigatório (sandbox, não grava)

Dependências do projeto:
- utils/gsheets.py: load_* + (idealmente) ensure_aporte_tabs + load_*_aporte
- utils/core.py: normalize_* + compute_positions_from_movs + enrich_positions_with_master + compute_allocations + compute_concentration
- utils/aporte_engine.py: build_candidates + rank_* + simulate_aporte + parse_regras_aporte + load_universe_from_watchlist
- utils/alerts.py: motor contratual de alertas (bloqueio/penalidade) para esta página
"""

from __future__ import annotations

import math
from typing import Dict, Tuple, List

import pandas as pd
import streamlit as st

# =========================
# Page config
# =========================
st.set_page_config(layout="wide", page_title="Momento do Aporte", page_icon="🧭")

# =========================
# Imports do seu projeto (blindados)
# =========================
from utils.gsheets import (
    load_movimentacoes,
    load_ativos,
    load_proventos,
    load_cotacoes,
)

# Aporte tabs/loaders (podem ainda não existir no seu gsheets.py; a página não explode)
try:
    from utils.gsheets import (
        ensure_aporte_tabs,
        load_watchlist_aporte,
        load_regras_aporte,
        load_alertas_ativos,
        load_limites_aporte,
    )
    _HAS_APORTE_GSHEETS = True
except Exception:
    _HAS_APORTE_GSHEETS = False
    ensure_aporte_tabs = None
    load_watchlist_aporte = None
    load_regras_aporte = None
    load_alertas_ativos = None
    load_limites_aporte = None

from utils.core import (
    normalize_master_ativos,
    normalize_proventos,
    normalize_cotacoes,
    compute_positions_from_movs,
    enrich_positions_with_master,
    compute_allocations,
    compute_concentration,
)

from utils.aporte_engine import (
    parse_regras_aporte,
    load_universe_from_watchlist,
    build_candidates,
    rank_fii_fiagro,
    rank_acoes,
    simulate_aporte,
)

# Alerts (contratual) — se não existir ainda, a página segue sem penalidade/bloqueio
try:
    from utils.alerts import apply_alerts
    _HAS_ALERTS = True
except Exception:
    _HAS_ALERTS = False
    apply_alert_policy_to_candidates = None


# =========================
# Helpers de formatação
# =========================
def _fmt_brl(v) -> str:
    try:
        x = float(v or 0.0)
        s = f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"R$ {s}"
    except Exception:
        return "R$ 0,00"

def _fmt_pct(v) -> str:
    try:
        x = float(v or 0.0) * 100.0
        s = f"{x:,.2f}".replace(".", ",")
        return f"{s}%"
    except Exception:
        return "0,00%"

def _is_num(x) -> bool:
    try:
        return x is not None and not (isinstance(x, float) and (math.isnan(x) or math.isinf(x)))
    except Exception:
        return False

def _join_chips(x) -> str:
    if isinstance(x, list):
        return " • ".join([str(i) for i in x if str(i).strip() != ""])
    return str(x or "")


# =========================
# Bootstrap de abas (se disponível)
# =========================
if _HAS_APORTE_GSHEETS and ensure_aporte_tabs:
    try:
        ensure_aporte_tabs()
    except Exception:
        # offline/sem permissão: não quebra
        pass


# =========================
# Cabeçalho
# =========================
st.title("🧭 Momento do Aporte")
st.caption(
    "Esta página ordena candidatos por regras determinísticas e explicáveis. "
    "Não é recomendação de compra/venda. Rankings são separados por classe."
)

if not _HAS_APORTE_GSHEETS:
    st.error(
        "Seu utils/gsheets.py ainda não tem as funções do Momento do Aporte "
        "(ensure_aporte_tabs + loaders). Sem isso, esta página não consegue ler watchlist/regras/alertas."
    )
    st.stop()


# =========================
# Carregar dados (fonte de verdade: Sheets)
# =========================
with st.spinner("Carregando base (Sheets)…"):
    df_mov_raw = load_movimentacoes()
    df_mst_raw = load_ativos()
    df_prov_raw = load_proventos()
    df_cot_raw = load_cotacoes()

    df_watch = load_watchlist_aporte()
    df_regras = load_regras_aporte()
    df_alertas = load_alertas_ativos()
    df_limites = load_limites_aporte()

# Normalizações (core)
df_master = normalize_master_ativos(df_mst_raw)
df_prov = normalize_proventos(df_prov_raw)
df_cot = normalize_cotacoes(df_cot_raw)

# Posições + enriquecimento
df_pos = compute_positions_from_movs(df_mov_raw)
df_enriched = enrich_positions_with_master(df_pos, df_master, df_cot)

# Métricas de guardrail
df_alloc_cls, _ = compute_allocations(df_enriched)
df_conc = compute_concentration(df_enriched, alert_pct=10.0)

# =========================
# Guardrails (topo)
# =========================
st.subheader("1) Guardrails (antes de olhar ranking)")

c1, c2, c3 = st.columns([1.1, 1.3, 1.2])

with c1:
    valor_aporte = st.number_input(
        "Valor disponível para aporte (R$)",
        min_value=0.0,
        value=0.0,
        step=50.0,
        format="%.2f",
    )

with c2:
    st.markdown("**Alocação atual (por classe)**")
    if df_alloc_cls is None or df_alloc_cls.empty:
        st.info("Sem alocação (carteira vazia ou dados insuficientes).")
    else:
        view = df_alloc_cls.copy()
        view["peso_fmt"] = view["peso"].apply(_fmt_pct)
        st.dataframe(view[["classe", "peso_fmt"]], use_container_width=True, hide_index=True)

with c3:
    st.markdown("**Concentração (Top)**")
    if df_conc is None or df_conc.empty:
        st.info("Sem dados de concentração.")
    else:
        top = df_conc.head(8).copy()
        top["peso_fmt"] = (top["peso"] * 100.0).apply(lambda x: f"{x:,.2f}%".replace(".", ","))
        st.dataframe(top[["ticker", "classe", "peso_fmt", "alerta"]], use_container_width=True, hide_index=True)

# comparação com alvo (se limites_aporte tiver alvos)
if df_limites is not None and not df_limites.empty:
    dlim = df_limites.copy()
    dlim.columns = [str(c).strip().lower() for c in dlim.columns]

    # tentativa de mapear colunas
    # esperado: classe | peso_alvo (0-1) ou alvo_pct
    if "classe" in dlim.columns and ("peso_alvo" in dlim.columns or "alvo_pct" in dlim.columns):
        st.markdown("**Alocação alvo vs atual (seus limites_aporte)**")
        lim = dlim.copy()
        lim["classe"] = lim["classe"].astype(str).str.strip()
        if "peso_alvo" in lim.columns:
            lim["peso_alvo_num"] = lim["peso_alvo"].apply(lambda x: float(str(x).replace(",", ".") or 0.0))
        else:
            lim["peso_alvo_num"] = lim["alvo_pct"].apply(lambda x: float(str(x).replace(",", ".") or 0.0) / 100.0)

        cur = df_alloc_cls.copy() if df_alloc_cls is not None else pd.DataFrame(columns=["classe", "peso"])
        cur["classe"] = cur["classe"].astype(str).str.strip()
        cur = cur.rename(columns={"peso": "peso_atual"})

        comp = lim.merge(cur, on="classe", how="left")
        comp["peso_atual"] = comp["peso_atual"].fillna(0.0)
        comp["gap"] = comp["peso_alvo_num"] - comp["peso_atual"]
        comp["alvo"] = comp["peso_alvo_num"].apply(_fmt_pct)
        comp["atual"] = comp["peso_atual"].apply(_fmt_pct)
        comp["gap_fmt"] = comp["gap"].apply(lambda x: _fmt_pct(x) if _is_num(x) else "—")

        st.dataframe(comp[["classe", "atual", "alvo", "gap_fmt"]], use_container_width=True, hide_index=True)

# =========================
# Regras e Universo fixo
# =========================
st.subheader("2) Universo fixo e regras (contrato)")

pesos_por_classe = parse_regras_aporte(df_regras)

colu1, colu2 = st.columns([1.2, 1.0])

with colu1:
    st.markdown("**Universo fixo (watchlist_aporte)**")
    n_watch = 0 if df_watch is None else int(len(df_watch))
    st.caption(f"Linhas na watchlist_aporte: {n_watch}")

with colu2:
    st.markdown("**Pesos discretos (regras_aporte)**")
    # mostra resumo por classe
    for cls in ["FII", "FIAGRO", "AÇÕES"]:
        w = pesos_por_classe.get(cls, {})
        w_txt = ", ".join([f"{k}:{int(v)}" for k, v in w.items() if int(v) > 0])
        st.caption(f"{cls}: {w_txt if w_txt else 'sem pesos ativos (fallback padrão)'}")

# =========================
# Construir candidatos por classe (SEPARADOS)
# =========================
univ_fii = load_universe_from_watchlist(df_watch, "FII")
univ_fiagro = load_universe_from_watchlist(df_watch, "FIAGRO")
univ_acoes = load_universe_from_watchlist(df_watch, "AÇÕES")

# base candidates (usa df_enriched como "pos + quotes + master", mas build_candidates espera pos/cot/master separados)
# -> para manter simples e auditável, alimentamos build_candidates com:
#    df_pos (snapshot), df_cot (cotacoes), df_master (master)
df_cand_fii = build_candidates(df_pos, df_cot, df_master, univ_fii, "FII")
df_cand_fiagro = build_candidates(df_pos, df_cot, df_master, univ_fiagro, "FIAGRO")
df_cand_acoes = build_candidates(df_pos, df_cot, df_master, univ_acoes, "AÇÕES")

# adiciona sinais internos úteis (gap_alocacao e concentracao) vindos do enriched
# (não mistura classes: apenas merge por ticker)
aux = df_enriched.copy()
aux = aux[["ticker", "peso"]].copy() if "peso" in aux.columns else aux[["ticker"]].copy()
aux["ticker"] = aux["ticker"].astype(str).str.strip().str.upper().str.replace(" ", "", regex=False)

# concentração: usa "peso" (0-1)
if "peso" in aux.columns:
    aux["concentracao"] = aux["peso"].astype(float)

# gap alocação (classe): se limites tiver classe alvo, calcula por ticker a partir da classe do master
gap_map = {}
if df_limites is not None and not df_limites.empty:
    dlim = df_limites.copy()
    dlim.columns = [str(c).strip().lower() for c in dlim.columns]
    if "classe" in dlim.columns and ("peso_alvo" in dlim.columns or "alvo_pct" in dlim.columns):
        lim = dlim.copy()
        lim["classe"] = lim["classe"].astype(str).str.strip()
        if "peso_alvo" in lim.columns:
            lim["peso_alvo_num"] = lim["peso_alvo"].apply(lambda x: float(str(x).replace(",", ".") or 0.0))
        else:
            lim["peso_alvo_num"] = lim["alvo_pct"].apply(lambda x: float(str(x).replace(",", ".") or 0.0) / 100.0)
        # atual por classe
        cur = df_alloc_cls.copy() if df_alloc_cls is not None else pd.DataFrame(columns=["classe", "peso"])
        cur["classe"] = cur["classe"].astype(str).str.strip()
        cur = cur.rename(columns={"peso": "peso_atual"})
        comp = lim.merge(cur, on="classe", how="left")
        comp["peso_atual"] = comp["peso_atual"].fillna(0.0)
        comp["gap_cls"] = comp["peso_alvo_num"] - comp["peso_atual"]
        gap_map = {str(r["classe"]).strip(): float(r["gap_cls"]) for _, r in comp.iterrows()}

def _inject_aux(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["ticker"] = d["ticker"].astype(str).str.strip().str.upper().str.replace(" ", "", regex=False)
    d = d.merge(aux[["ticker"] + ([c for c in ["concentracao"] if c in aux.columns])], on="ticker", how="left")
    # gap_alocacao (classe)
    if "classe" in d.columns and gap_map:
        d["gap_alocacao"] = d["classe"].astype(str).str.strip().map(lambda x: gap_map.get(str(x).strip(), float("nan")))
    else:
        d["gap_alocacao"] = float("nan")
    return d

df_cand_fii = _inject_aux(df_cand_fii)
df_cand_fiagro = _inject_aux(df_cand_fiagro)
df_cand_acoes = _inject_aux(df_cand_acoes)

# Alerts contratual (bloqueio/penalidade) — se existir
def _apply_alerts(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    if not _HAS_ALERTS or apply_alert_policy_to_candidates is None:
        d["alert_severity"] = ""
        d["penalty_positions"] = 0
        d["blocked"] = False
        return d
    try:
        return apply_alerts(d, df_alertas, classe=str(d.get("classe", [""])[0]) if "classe" in d.columns else "")
    except Exception:
        d["alert_severity"] = ""
        d["penalty_positions"] = 0
        d["blocked"] = False
        return d

df_cand_fii = _apply_alerts(df_cand_fii)
df_cand_fiagro = _apply_alerts(df_cand_fiagro)
df_cand_acoes = _apply_alerts(df_cand_acoes)

# remove bloqueados do ranking (ainda aparecem em "bloqueados")
def _split_blocked(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if df is None or df.empty:
        return df, pd.DataFrame()
    if "blocked" not in df.columns:
        return df, pd.DataFrame()
    return df[df["blocked"] != True].copy(), df[df["blocked"] == True].copy()

df_cand_fii_ok, df_fii_block = _split_blocked(df_cand_fii)
df_cand_fiagro_ok, df_fiagro_block = _split_blocked(df_cand_fiagro)
df_cand_acoes_ok, df_acoes_block = _split_blocked(df_cand_acoes)

# =========================
# Rankings separados
# =========================
st.subheader("3) Rankings separados (sem recomendação)")

tab1, tab2, tab3 = st.tabs(["🏢 FII", "🌾 FIAGRO", "🏭 AÇÕES"])

def _render_rank_table(df_rank: pd.DataFrame, title: str):
    if df_rank is None or df_rank.empty:
        st.info(f"Sem candidatos elegíveis em {title}.")
        return
    view = df_rank.copy()
    view["chips_txt"] = view["chips"].apply(_join_chips)
    cols = [c for c in ["rank_final", "ticker", "score_base", "penalty_positions", "preco_atual", "preco_medio", "desconto_pm", "chips_txt"] if c in view.columns]
    st.dataframe(view[cols], use_container_width=True, hide_index=True)

def _render_blocked(df_block: pd.DataFrame):
    if df_block is None or df_block.empty:
        return
    v = df_block.copy()
    v["motivo"] = v.get("alert_severity", "").astype(str)
    cols = [c for c in ["ticker", "motivo", "preco_atual", "preco_medio"] if c in v.columns]
    with st.expander("Ver bloqueados (alerta VERMELHO/CINZA)", expanded=False):
        st.dataframe(v[cols], use_container_width=True, hide_index=True)

with tab1:
    ranked_fii, revisao_fii = rank_fii_fiagro(df_cand_fii_ok, df_prov, pesos_por_classe.get("FII", {}))
    _render_rank_table(ranked_fii, "FII")
    _render_blocked(df_fii_block)
    if revisao_fii is not None and not revisao_fii.empty:
        with st.expander("Revisão (dados mínimos ausentes)", expanded=False):
            st.dataframe(revisao_fii[["ticker", "gating_ok", "preco_atual", "preco_medio"]], use_container_width=True, hide_index=True)

with tab2:
    ranked_fiagro, revisao_fiagro = rank_fii_fiagro(df_cand_fiagro_ok, df_prov, pesos_por_classe.get("FIAGRO", {}))
    _render_rank_table(ranked_fiagro, "FIAGRO")
    _render_blocked(df_fiagro_block)
    if revisao_fiagro is not None and not revisao_fiagro.empty:
        with st.expander("Revisão (dados mínimos ausentes)", expanded=False):
            st.dataframe(revisao_fiagro[["ticker", "gating_ok", "preco_atual", "preco_medio"]], use_container_width=True, hide_index=True)

with tab3:
    ranked_acoes, revisao_acoes = rank_acoes(df_cand_acoes_ok, pesos_por_classe.get("AÇÕES", {}))
    _render_rank_table(ranked_acoes, "AÇÕES")
    _render_blocked(df_acoes_block)
    if revisao_acoes is not None and not revisao_acoes.empty:
        with st.expander("Revisão (dados mínimos ausentes)", expanded=False):
            st.dataframe(revisao_acoes[["ticker", "gating_ok", "preco_atual", "preco_medio"]], use_container_width=True, hide_index=True)

# =========================
# Simulador (obrigatório)
# =========================
st.subheader("4) Simulador de aporte (sandbox, não grava)")

# universo para simulação: junta elegíveis (não bloqueados)
all_candidates = pd.concat(
    [
        df_cand_fii_ok.assign(_cls="FII"),
        df_cand_fiagro_ok.assign(_cls="FIAGRO"),
        df_cand_acoes_ok.assign(_cls="AÇÕES"),
    ],
    ignore_index=True
)

if all_candidates.empty:
    st.info("Sem candidatos para simulação (watchlist vazia ou tudo bloqueado/revisão).")
    st.stop()

all_candidates["ticker"] = all_candidates["ticker"].astype(str).str.strip().str.upper().str.replace(" ", "", regex=False)

col_s1, col_s2, col_s3 = st.columns([1.2, 1.0, 1.0])

with col_s1:
    ticker_sel = st.selectbox(
        "Selecione um ativo (somente universo elegível)",
        options=sorted(all_candidates["ticker"].unique().tolist()),
        index=0,
    )

with col_s2:
    preco_ref = st.number_input(
        "Preço de referência (opcional)",
        min_value=0.0,
        value=0.0,
        step=0.10,
        format="%.2f",
        help="Se 0, usa o preço atual do cache.",
    )

with col_s3:
    valor_sim = st.number_input(
        "Valor do aporte simulado (R$)",
        min_value=0.0,
        value=float(valor_aporte or 0.0),
        step=50.0,
        format="%.2f",
    )

# detalhe do ativo
row_det = all_candidates[all_candidates["ticker"] == ticker_sel].head(1)
if not row_det.empty:
    r = row_det.iloc[0]
    st.markdown("**Detalhe do ativo (dados atuais)**")
    dcol1, dcol2, dcol3, dcol4 = st.columns(4)
    dcol1.metric("Classe", str(r.get("classe", "")))
    dcol2.metric("Qtd atual", f"{float(r.get('quantidade', 0.0) or 0.0):,.4f}".replace(",", "X").replace(".", ",").replace("X", "."))
    dcol3.metric("PM atual", _fmt_brl(r.get("preco_medio", 0.0)))
    dcol4.metric("Preço atual", _fmt_brl(r.get("preco_atual", 0.0)))

    chips_txt = _join_chips(r.get("chips", []))
    if chips_txt:
        st.caption(chips_txt)

# simula
if valor_sim and float(valor_sim) > 0:
    try:
        pr = None if not preco_ref or float(preco_ref) <= 0 else float(preco_ref)
        res = simulate_aporte(all_candidates, df_prov, ticker_sel, float(valor_sim), preco_ref=pr)

        st.markdown("**Resultado da simulação**")
        scol1, scol2, scol3, scol4 = st.columns(4)

        scol1.metric("Qtd antes", f"{res.qtd_antes:,.4f}".replace(",", "X").replace(".", ",").replace("X", "."))
        scol2.metric("Qtd depois", f"{res.qtd_depois:,.4f}".replace(",", "X").replace(".", ",").replace("X", "."))
        scol3.metric("PM antes", _fmt_brl(res.pm_antes))
        scol4.metric("PM depois", _fmt_brl(res.pm_depois))

        # renda condicional só se FII/FIAGRO
        if _is_num(res.renda_cond_antes) and _is_num(res.renda_cond_depois):
            st.markdown("**Renda condicional (se repetir o último pagamento recebido)**")
            rcol1, rcol2, rcol3 = st.columns(3)
            rcol1.metric("Antes", _fmt_brl(res.renda_cond_antes))
            rcol2.metric("Depois", _fmt_brl(res.renda_cond_depois))
            delta = float(res.renda_cond_depois - res.renda_cond_antes)
            rcol3.metric("Δ (cond.)", _fmt_brl(delta))
        else:
            st.caption("AÇÕES não possuem projeção automática de renda (contrato).")

        st.warning(
            "Sandbox: esta simulação não grava nada e não executa aporte. "
            "Se algo aqui está 'bonito demais', desconfie: seu objetivo é reduzir impulso."
        )

    except Exception as e:
        st.error(f"Não foi possível simular: {e}")
else:
    st.info("Defina um valor de aporte simulado (> 0) para ver o impacto.")
