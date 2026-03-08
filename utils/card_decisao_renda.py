# utils/card_decisao_renda.py
# -*- coding: utf-8 -*-
# ============================================================
# CONTRATO OFICIAL v1.1 — CARD DE DECISÃO POR ATIVO (RENDA)
# ============================================================

from __future__ import annotations
import math
import json
import os
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

# ─────────────────────────────────────────────
# HELPERS INTERNOS E DATACLASSES
# ─────────────────────────────────────────────
def _safe_float(x) -> float:
    try:
        if x is None: return 0.0
        s = str(x).strip()
        if s in ("", "nan", "NaN", "-"): return 0.0
        if "," in s and "." in s: s = s.replace(".", "").replace(",", ".")
        elif "," in s: s = s.replace(",", ".")
        return float(s)
    except Exception:
        return 0.0

def _parse_date(x) -> Optional[date]:
    if x is None: return None
    if isinstance(x, datetime): return x.date()
    if isinstance(x, date): return x
    s = str(x).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try: return datetime.strptime(s, fmt).date()
        except Exception: pass
    return None

@dataclass
class BlocoProvento:
    data_pgto: Optional[date]
    ticker: str
    qtd_base: float
    valor_recebido: float
    vpc_real: float
    flag_qtd: str = ""

@dataclass
class BlocoRendimento12M:
    total_12m: float
    media_mensal_12m: float
    yoc_12m: float
    yield_preco_atual_12m: float
    tempo_carteira_anos: float
    tempo_carteira_str: str
    msg_eficiencia: str

@dataclass
class BlocoQualidade:
    cv: float
    estabilidade: str
    trend6: float
    tendencia_6m: str
    g12: float
    crescimento_12m_str: str
    max_vpc: float
    min_vpc: float
    serie_vpc: List[float] = field(default_factory=list)

@dataclass
class BlocoRetorno:
    payback_anos: float
    payback_str: str
    capital_recuperado_pct: float
    total_acumulado: float
    flag_historico: str = ""

@dataclass
class BlocoValuation:
    dividendo_anual_medio_por_cota: float
    preco_teto: float
    preco_atual: float
    agio_abs: float
    agio_pct: float
    zona: str
    flag_preco: str = ""

@dataclass
class BlocoMagicNumber:
    magic_number: float
    qtd_atual: float
    faltam: float
    renda_mensal_atual: float
    renda_alvo_mensal: float
    definido: bool
    investido_atual: float = 0.0

@dataclass
class DecisaoCard:
    ticker: str
    decisao: str
    score: float
    score_valuation: float
    score_qualidade: float
    score_eficiencia: float
    score_maturidade: float
    motivos: List[str]
    flags_qualidade: List[str]
    modo_conservador: bool
    timestamp: datetime = field(default_factory=datetime.now)
    provento: Optional[BlocoProvento] = None
    rendimento_12m: Optional[BlocoRendimento12M] = None
    qualidade: Optional[BlocoQualidade] = None
    retorno: Optional[BlocoRetorno] = None
    valuation: Optional[BlocoValuation] = None
    magic: Optional[BlocoMagicNumber] = None

# ─────────────────────────────────────────────
# MOTOR PRINCIPAL
# ─────────────────────────────────────────────
class CardDecisaoMotor:
    TAXA_BAZIN = 0.06

    def __init__(self, ticker: str, df_proventos: pd.DataFrame, df_movimentacoes: pd.DataFrame, df_cotacoes: Optional[pd.DataFrame] = None, df_ativos: Optional[pd.DataFrame] = None, renda_alvo_mensal_ativo: Optional[float] = None, hoje: Optional[date] = None):
        self.ticker = str(ticker).upper().strip()
        self.df_prov = self._norm(df_proventos)
        self.df_movs = self._norm(df_movimentacoes)
        self.df_cot = self._norm(df_cotacoes) if df_cotacoes is not None else pd.DataFrame()
        self.df_ativos = self._norm(df_ativos) if df_ativos is not None else pd.DataFrame()
        self.renda_alvo = renda_alvo_mensal_ativo
        if hoje is not None: self.hoje = hoje
        else:
            self.hoje = date.today()
            self._tz_fallback = True
        self._flags: List[str] = []
        self._tz_fallback: bool = hoje is None

    @staticmethod
    def _norm(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty: return pd.DataFrame()
        out = df.copy()
        out.columns = [str(c).strip().lower() for c in out.columns]
        return out

    def _qtd_atual(self) -> float:
        df = self.df_movs
        if df.empty or "ticker" not in df.columns: return 0.0
        df2 = df[df["ticker"].astype(str).str.upper().str.strip() == self.ticker].copy()
        if df2.empty: return 0.0
        df2["quantidade"] = df2["quantidade"].apply(_safe_float)
        df2["tipo_n"] = df2["tipo"].astype(str).str.upper().str.strip()
        saldo = float(df2[df2["tipo_n"] == "COMPRA"]["quantidade"].sum() - df2[df2["tipo_n"] == "VENDA"]["quantidade"].sum())
        return saldo if saldo > 0.001 else 0.0

    def _investido_atual(self) -> float:
        df = self.df_movs
        if df.empty: return 0.0
        df2 = df[df["ticker"].astype(str).str.upper().str.strip() == self.ticker].copy()
        if df2.empty: return 0.0
        df2["quantidade"] = df2["quantidade"].apply(_safe_float)
        df2["preco_unitario"] = df2["preco_unitario"].apply(_safe_float) if "preco_unitario" in df2.columns else 0.0
        df2["tipo_n"] = df2["tipo"].astype(str).str.upper().str.strip()
        if "data" in df2.columns:
            try: df2 = df2.sort_values("data", key=lambda s: pd.to_datetime(s, dayfirst=True, errors="coerce"), ascending=True, na_position="first")
            except Exception: pass
        custo, qtd = 0.0, 0.0
        for _, row in df2.iterrows():
            q, p, tp = _safe_float(row.get("quantidade")), _safe_float(row.get("preco_unitario")), str(row.get("tipo_n", "")).upper()
            if tp == "COMPRA": custo += q * p; qtd += q
            elif tp == "VENDA" and qtd > 0: pm = custo / qtd; custo -= q * pm; qtd -= q
        return max(custo, 0.0)

    def _investido_total_historico(self) -> float:
        df = self.df_movs
        if df.empty: return 0.0
        df2 = df[df["ticker"].astype(str).str.upper().str.strip() == self.ticker].copy()
        if df2.empty: return 0.0
        df2["quantidade"] = df2["quantidade"].apply(_safe_float)
        df2["preco_unitario"] = df2["preco_unitario"].apply(_safe_float) if "preco_unitario" in df2.columns else 0.0
        df2["tipo_n"] = df2["tipo"].astype(str).str.upper().str.strip()
        return float((df2[df2["tipo_n"] == "COMPRA"]["quantidade"] * df2[df2["tipo_n"] == "COMPRA"]["preco_unitario"]).sum())

    def _primeira_compra(self) -> Optional[date]:
        df = self.df_movs
        if df.empty: return None
        df2 = df[df["ticker"].astype(str).str.upper().str.strip() == self.ticker].copy()
        if df2.empty: return None
        df2["tipo_n"] = df2["tipo"].astype(str).str.upper().str.strip()
        df2 = df2[df2["tipo_n"] == "COMPRA"]
        if df2.empty: return None
        df2["dt"] = pd.to_datetime(df2["data"], dayfirst=True, errors="coerce")
        return df2["dt"].min().date() if not df2["dt"].isna().all() else None

    def _preco_atual(self) -> Tuple[float, str]:
        t = self.ticker
        df_c = self.df_cot
        if not df_c.empty and "ticker" in df_c.columns:
            row = df_c[df_c["ticker"].astype(str).str.upper().str.strip() == t]
            if not row.empty:
                for col in ["price", "preco", "close", "cotação", "valor", "ultimo"]:
                    if col in row.columns:
                        v = _safe_float(row.iloc[0][col])
                        if v > 0: return v, ""
        df = self.df_movs
        if not df.empty and "ticker" in df.columns:
            df2 = df[df["ticker"].astype(str).str.upper().str.strip() == t].copy()
            df2["tipo_n"] = df2["tipo"].astype(str).str.upper().str.strip()
            df2 = df2[df2["tipo_n"] == "COMPRA"]
            if not df2.empty:
                if "preco_unitario" in df2.columns:
                    df2["dt"] = pd.to_datetime(df2["data"], dayfirst=True, errors="coerce")
                    df2 = df2.sort_values("dt")
                    v = _safe_float(df2.iloc[-1]["preco_unitario"])
                    if v > 0:
                        self._flags.append("⚠️ preço fallback (último pago)")
                        return v, "⚠️ preço fallback"
        return 0.0, "⚠️ preço indisponível"

    def _prov_ticker(self) -> pd.DataFrame:
        df = self.df_prov
        if df.empty or "ticker" not in df.columns: return pd.DataFrame()
        df2 = df[df["ticker"].astype(str).str.upper().str.strip() == self.ticker].copy()
        if df2.empty: return df2
        df2["data_dt"] = pd.to_datetime(df2.get("data", df2.get("data_pagamento", None)), dayfirst=True, errors="coerce")
        if "status" in df2.columns:
            status_norm = df2["status"].astype(str).str.lower().str.strip()
            df2 = df2[status_norm.isin(["recebido", "pago", "received"])]
        df2 = df2.dropna(subset=["data_dt"])
        return df2.sort_values("data_dt")

    def _detectar_classe_ativo(self) -> str:
        t = self.ticker
        df = self.df_ativos
        if not df.empty and "ticker" in df.columns:
            row = df[df["ticker"].astype(str).str.upper().str.strip() == t]
            if not row.empty:
                for col in ["classe", "tipo_ativo", "categoria", "tipo"]:
                    if col in row.columns:
                        val = str(row.iloc[0][col]).lower().strip()
                        if val in ("fii", "fiagro", "fundo", "fi", "fundo imobiliário", "fundo imobiliario"): return "fii"
                        if any(val.startswith(p) for p in ("acao", "ação", "acão", "stock", "equity")): return "acao"
        if t.endswith("11"): return "fii"
        return "acao"

    _TIPOS_QUALIDADE: dict = {
        "fii":  frozenset({"rendimento", "dividendo", "dividend", "rend"}),
        "acao": frozenset({"jcp", "juros sobre capital proprio", "juros sobre capital próprio", "dividendo", "dividends", "dividend"}),
    }

    def _serie_vpc_qualidade(self, df_t: pd.DataFrame, classe: str) -> List[float]:
        limite = self.hoje - timedelta(days=365)
        df = df_t[df_t["data_dt"].dt.date >= limite].copy()
        if df.empty: return []
        tipos_aceitos = self._TIPOS_QUALIDADE.get(classe, frozenset())
        col_tipo = next((c for c in ["tipo", "tipo_pagamento", "tipo_provento"] if c in df.columns), None)
        if col_tipo and tipos_aceitos:
            tipo_norm = df[col_tipo].astype(str).str.lower().str.strip()
            mask = tipo_norm.apply(lambda v: any(a in v or v in a for a in tipos_aceitos))
            df_filtrado = df[mask].copy()
            if df_filtrado.empty: self._flags.append(f"⚠️ Qualidade: nenhum tipo reconhecido para {classe.upper()} — usando todos os tipos (fallback)")
            else: df = df_filtrado
        col_vpc = next((c for c in ["valor_por_cota", "vpc", "valor_cota"] if c in df.columns), None)
        if col_vpc is None:
            if "valor" in df.columns and "quantidade_na_data" in df.columns:
                df["_vpc"] = df.apply(lambda r: _safe_float(r["valor"]) / _safe_float(r["quantidade_na_data"]) if _safe_float(r["quantidade_na_data"]) > 0 else 0.0, axis=1)
                col_vpc = "_vpc"
            else: return []
        df["_vpc_v"] = df[col_vpc].apply(_safe_float)
        df["_mes"] = df["data_dt"].dt.to_period("M")
        serie = df.groupby("_mes")["_vpc_v"].mean().sort_index().tolist()
        return [float(v) for v in serie if v > 0]

    def _serie_vpc_12m(self, df_t: pd.DataFrame) -> List[float]:
        limite = self.hoje - timedelta(days=365)
        df = df_t[df_t["data_dt"].dt.date >= limite].copy()
        if df.empty: return []
        col_vpc = next((c for c in ["valor_por_cota", "vpc", "valor_cota"] if c in df.columns), None)
        if col_vpc is None:
            if "valor" in df.columns and "quantidade_na_data" in df.columns:
                df["_vpc"] = df.apply(lambda r: _safe_float(r["valor"]) / _safe_float(r["quantidade_na_data"]) if _safe_float(r["quantidade_na_data"]) > 0 else 0.0, axis=1)
                col_vpc = "_vpc"
            else: return []
        df["_vpc_v"] = df[col_vpc].apply(_safe_float)
        df["_mes"] = df["data_dt"].dt.to_period("M")
        serie = df.groupby("_mes")["_vpc_v"].mean().sort_index().tolist()
        return [float(v) for v in serie if v > 0]

    def _total_12m(self, df_t: pd.DataFrame) -> float:
        limite = self.hoje - timedelta(days=365)
        df = df_t[df_t["data_dt"].dt.date >= limite]
        if df.empty: return 0.0
        col = next((c for c in ["valor", "valor_total", "total"] if c in df.columns), None)
        return float(df[col].apply(_safe_float).sum()) if col else 0.0

    def _total_acumulado(self, df_t: pd.DataFrame) -> float:
        if df_t.empty: return 0.0
        col = next((c for c in ["valor", "valor_total", "total"] if c in df_t.columns), None)
        return float(df_t[col].apply(_safe_float).sum()) if col else 0.0

    def _ultimo_provento(self, df_t: pd.DataFrame) -> Optional[pd.Series]:
        return df_t.iloc[-1] if not df_t.empty else None

    def _calc_bloco_provento(self, df_t: pd.DataFrame, qtd_atual: float) -> BlocoProvento:
        ult = self._ultimo_provento(df_t)
        if ult is None: return BlocoProvento(data_pgto=None, ticker=self.ticker, qtd_base=0.0, valor_recebido=0.0, vpc_real=0.0, flag_qtd="⚠️ sem proventos")
        data_pgto = ult["data_dt"].date() if pd.notnull(ult["data_dt"]) else None
        qtd_base, flag_qtd, valor, vpc_real = 0.0, "", 0.0, 0.0
        for c in ["quantidade_na_data", "quantidade", "qtd"]:
            if c in ult.index:
                qtd_base = _safe_float(ult[c]); break
        if qtd_base <= 0:
            qtd_base = qtd_atual; self._flags.append("⚠️ qtd_ref veio de posição atual (fallback)"); flag_qtd = "⚠️ qtd_ref fallback"
        for c in ["valor", "valor_total", "total"]:
            if c in ult.index:
                valor = _safe_float(ult[c]); break
        vpc_real = valor / qtd_base if qtd_base > 0 else 0.0
        if vpc_real <= 0:
            for c in ["valor_por_cota", "vpc"]:
                if c in ult.index:
                    vpc_real = _safe_float(ult[c]); break
        return BlocoProvento(data_pgto=data_pgto, ticker=self.ticker, qtd_base=qtd_base, valor_recebido=valor if valor > 0 else qtd_base * vpc_real, vpc_real=vpc_real, flag_qtd=flag_qtd)

    def _calc_bloco_rendimento(self, df_t: pd.DataFrame, qtd_atual: float, investido_atual: float, preco_atual: float) -> BlocoRendimento12M:
        total_12m = self._total_12m(df_t)
        media_mensal = total_12m / 12.0
        yoc = (total_12m / investido_atual * 100) if investido_atual > 0 else 0.0
        yield_pa = (total_12m / (qtd_atual * preco_atual) * 100) if qtd_atual > 0 and preco_atual > 0 else 0.0
        primeira = self._primeira_compra()
        if primeira:
            anos = (self.hoje - primeira).days / 365.25
            tempo_str = f"{int(anos)}a {int((anos % 1) * 12)}m"
        else: anos, tempo_str = 0.0, "—"
        diff = yoc - yield_pa
        if diff > 0.3: msg = "📌 Você está melhor posicionado que quem compra hoje (YoC > Yield atual)."
        elif abs(diff) <= 0.3: msg = "📌 Posição equivalente ao mercado atual (YoC ≈ Yield atual)."
        else: msg = "📌 O mercado está mais eficiente que seu PM — atenção ao preço/PM."
        return BlocoRendimento12M(total_12m=total_12m, media_mensal_12m=media_mensal, yoc_12m=yoc, yield_preco_atual_12m=yield_pa, tempo_carteira_anos=anos, tempo_carteira_str=tempo_str, msg_eficiencia=msg)

    def _calc_bloco_qualidade(self, serie_vpc: List[float]) -> BlocoQualidade:
        if not serie_vpc or len(serie_vpc) < 2: return BlocoQualidade(cv=0.0, estabilidade="—", trend6=0.0, tendencia_6m="—", g12=0.0, crescimento_12m_str="—", max_vpc=max(serie_vpc) if serie_vpc else 0.0, min_vpc=min(serie_vpc) if serie_vpc else 0.0, serie_vpc=serie_vpc)
        arr = np.array(serie_vpc)
        media, std = arr.mean(), arr.std(ddof=0)
        cv = float(std / media) if media > 0 else 0.0
        estabilidade = "Alta" if cv < 0.05 else "Moderada" if cv < 0.10 else "Baixa"
        if len(arr) >= 12: m6_rec, m6_ant = arr[-6:].mean(), arr[-12:-6].mean()
        elif len(arr) >= 6: m6_rec, m6_ant = arr[-3:].mean() if len(arr) >= 3 else arr.mean(), arr[:3].mean() if len(arr) >= 3 else arr.mean()
        else: m6_rec, m6_ant = arr[-1], arr[0]
        trend6 = float((m6_rec / m6_ant - 1) * 100) if m6_ant > 0 else 0.0
        tendencia_6m = "Crescente" if trend6 > 3.0 else "Estável" if trend6 >= -3.0 else "Leve queda" if trend6 > -8.0 else "Queda relevante"
        g12 = float((arr[-1] / arr[0] - 1) * 100) if len(arr) >= 2 and arr[0] > 0 else 0.0
        return BlocoQualidade(cv=cv, estabilidade=estabilidade, trend6=trend6, tendencia_6m=tendencia_6m, g12=g12, crescimento_12m_str=f"{g12:+.1f}%", max_vpc=float(arr.max()), min_vpc=float(arr.min()), serie_vpc=serie_vpc)

    def _calc_bloco_retorno(self, df_t: pd.DataFrame, investido_atual: float, total_12m: float) -> BlocoRetorno:
        if total_12m > 0 and investido_atual > 0: payback, payback_str = investido_atual / total_12m, f"{investido_atual / total_12m:.1f} anos (estimado, base 12M)"
        else: payback, payback_str = float("inf"), "∞ (sem proventos nos últimos 12M)"
        total_acum, investido_hist, flag = self._total_acumulado(df_t), self._investido_total_historico(), ""
        if investido_hist <= 0: investido_hist = investido_atual; flag = "⚠️ histórico parcial"; self._flags.append(flag)
        return BlocoRetorno(payback_anos=payback, payback_str=payback_str, capital_recuperado_pct=(total_acum / investido_hist * 100) if investido_hist > 0 else 0.0, total_acumulado=total_acum, flag_historico=flag)

    def _calc_bloco_valuation(self, serie_vpc: List[float], df_t: pd.DataFrame, classe: str, preco_atual: float, flag_preco: str) -> BlocoValuation:
        if classe == "fii":
            # FII: Média dos últimos 6 meses * 12
            vpc_ref = serie_vpc[-6:] if len(serie_vpc) >= 6 else serie_vpc
            div_anual = float(np.mean(vpc_ref)) * 12.0 if vpc_ref else 0.0
        else:
            # Ação: Soma real do VPC nos últimos 12 meses
            limite = self.hoje - timedelta(days=365)
            df = df_t[df_t["data_dt"].dt.date >= limite].copy()
            if df.empty:
                div_anual = 0.0
            else:
                col_vpc = next((c for c in ["valor_por_cota", "vpc", "valor_cota"] if c in df.columns), None)
                if col_vpc is None and "valor" in df.columns and "quantidade_na_data" in df.columns:
                    df["_vpc"] = df.apply(lambda r: _safe_float(r["valor"]) / _safe_float(r["quantidade_na_data"]) if _safe_float(r["quantidade_na_data"]) > 0 else 0.0, axis=1)
                    col_vpc = "_vpc"
                if col_vpc:
                    div_anual = float(df[col_vpc].apply(_safe_float).sum())
                else:
                    div_anual = 0.0

        preco_teto = div_anual / self.TAXA_BAZIN if div_anual > 0 else 0.0
        agio_abs, agio_pct = (preco_atual - preco_teto, (preco_atual / preco_teto - 1) * 100) if preco_teto > 0 and preco_atual > 0 else (0.0, 0.0)
        zona = "⚠️ Valuation indisponível" if preco_teto == 0 else "🟢 Zona de aporte" if preco_atual <= preco_teto else "🟡 Neutro" if preco_atual <= preco_teto * 1.10 else "🔴 Evitar" if preco_atual <= preco_teto * 1.30 else "🔴 Descolamento alto"
        return BlocoValuation(dividendo_anual_medio_por_cota=div_anual, preco_teto=preco_teto, preco_atual=preco_atual, agio_abs=agio_abs, agio_pct=agio_pct, zona=zona, flag_preco=flag_preco)

    def _calc_bloco_magic(self, serie_vpc: List[float], qtd_atual: float, media_mensal_12m: float, investido_atual: float) -> BlocoMagicNumber:
        if not self.renda_alvo or self.renda_alvo <= 0: return BlocoMagicNumber(magic_number=0.0, qtd_atual=qtd_atual, faltam=0.0, renda_mensal_atual=media_mensal_12m, renda_alvo_mensal=0.0, definido=False, investido_atual=investido_atual)
        vpc_ref = float(np.mean(serie_vpc[-6:])) if len(serie_vpc) >= 6 else float(np.mean(serie_vpc)) if serie_vpc else 0.0
        magic, faltam = (self.renda_alvo / vpc_ref, max(0.0, (self.renda_alvo / vpc_ref) - qtd_atual)) if vpc_ref > 0 else (0.0, 0.0)
        return BlocoMagicNumber(magic_number=magic, qtd_atual=qtd_atual, faltam=faltam, renda_mensal_atual=media_mensal_12m, renda_alvo_mensal=self.renda_alvo, definido=True, investido_atual=investido_atual)

    @staticmethod
    def _score_valuation(v: BlocoValuation) -> float:
        if v.preco_teto == 0: return 10.0
        if v.preco_atual <= v.preco_teto: return 45.0
        elif v.agio_pct <= 10.0: return 28.0
        elif v.agio_pct <= 30.0: return 11.0
        return 0.0

    @staticmethod
    def _score_qualidade(q: BlocoQualidade) -> float:
        s = 18.0 if q.estabilidade == "Alta" else 12.0 if q.estabilidade == "Moderada" else 5.0
        s += 12.0 if q.tendencia_6m == "Crescente" else 9.0 if q.tendencia_6m == "Estável" else 5.0 if q.tendencia_6m == "Leve queda" else 0.0
        return min(30.0, s)

    @staticmethod
    def _score_eficiencia(r: BlocoRendimento12M) -> float:
        diff = r.yoc_12m - r.yield_preco_atual_12m
        return 15.0 if diff >= 0.3 else 9.0 if abs(diff) < 0.3 else 4.0

    @staticmethod
    def _score_maturidade(m: BlocoMagicNumber, retorno: BlocoRetorno) -> float:
        s = 5.0 if not m.definido else 8.0 if m.qtd_atual < 0.5 * m.magic_number else 5.0 if m.qtd_atual < m.magic_number else 2.0
        return min(10.0, s + 1.0) if retorno.capital_recuperado_pct >= 10.0 else s

    def _gerar_decisao(self, score: float, v: BlocoValuation, q: BlocoQualidade, r: BlocoRendimento12M) -> Tuple[str, List[str]]:
        motivos = []
        if v.agio_pct > 10.0 and q.trend6 <= -3.0:
            motivos.append(f"Acima do teto em {v.agio_pct:.1f}% + tendência negativa ({q.trend6:.1f}%)")
            motivos.append("Regra de prudência: Valuation + Tendência negativa → 🔴 automático")
            return "🔴 AGUARDAR", motivos
        if q.cv >= 0.10 and q.trend6 <= -3.0:
            motivos.append(f"Instabilidade alta (CV={q.cv:.2f}) + tendência negativa ({q.trend6:.1f}%)")
            motivos.append("Regra de prudência: Qualidade deteriorada → 🔴 automático")
            return "🔴 AGUARDAR", motivos

        if score >= 70 and v.preco_teto > 0 and v.preco_atual <= v.preco_teto:
            decisao = "🟢 APORTAR"
            motivos.extend([f"Score {score:.0f}/100 — acima do limiar de aporte (≥70)", "Cotação abaixo do preço teto (Bazin 6%)"])
            if q.tendencia_6m in ("Crescente", "Estável"): motivos.append(f"Tendência de rendimento {q.tendencia_6m.lower()}")
            if r.yoc_12m > r.yield_preco_atual_12m: motivos.append("YoC acima do Yield atual — posição eficiente")
        elif score >= 40 or (score >= 70 and (v.preco_teto == 0 or v.preco_atual > v.preco_teto)):
            decisao = "🟡 MANTER"
            motivos.append(f"Score {score:.0f}/100 — zona de manutenção (40–69)")
            if v.agio_pct > 0: motivos.append(f"Cotação acima do teto em {v.agio_pct:.1f}% (faixa aceitável)")
            if q.tendencia_6m not in ("Crescente", "Estável"): motivos.append(f"Tendência {q.tendencia_6m.lower()} — monitorar")
        else:
            decisao = "🔴 AGUARDAR"
            motivos.append(f"Score {score:.0f}/100 — abaixo do limiar mínimo (<40)")
            if v.agio_pct > 10.0: motivos.append(f"Cotação significativamente acima do teto (+{v.agio_pct:.1f}%)")
            if q.tendencia_6m in ("Leve queda", "Queda relevante"): motivos.append(f"Tendência {q.tendencia_6m.lower()}")
            if r.yield_preco_atual_12m > 0 and r.yield_preco_atual_12m < 6.0: motivos.append(f"Yield atual ({r.yield_preco_atual_12m:.2f}%) abaixo da meta de 6%")
        return decisao, motivos

    def calcular(self) -> Optional[DecisaoCard]:
        self._flags = []
        if getattr(self, "_tz_fallback", False): self._flags.append("⚠️ 'hoje' não foi passado pelo caller — usando date.today()")
        qtd_atual = self._qtd_atual()
        if qtd_atual <= 0: return None
        investido_atual = self._investido_atual()
        preco_atual, flag_preco = self._preco_atual()
        if flag_preco: self._flags.append(flag_preco)
        df_t = self._prov_ticker()
        total_12m = self._total_12m(df_t)
        modo_conservador = False
        if total_12m == 0 or investido_atual == 0 or preco_atual == 0:
            modo_conservador = True; self._flags.append("⚠️ dados incompletos — modo conservador ativado")
        serie_vpc = self._serie_vpc_12m(df_t)
        classe_ativo = self._detectar_classe_ativo()
        serie_vpc_qualidade = self._serie_vpc_qualidade(df_t, classe_ativo)
        if not serie_vpc_qualidade: serie_vpc_qualidade = serie_vpc

        b_prov = self._calc_bloco_provento(df_t, qtd_atual)
        b_rend = self._calc_bloco_rendimento(df_t, qtd_atual, investido_atual, preco_atual)
        b_qual = self._calc_bloco_qualidade(serie_vpc_qualidade)
        b_ret = self._calc_bloco_retorno(df_t, investido_atual, total_12m)
        b_val = self._calc_bloco_valuation(serie_vpc, df_t, classe_ativo, preco_atual, flag_preco)
        b_mag = self._calc_bloco_magic(serie_vpc, qtd_atual, b_rend.media_mensal_12m, investido_atual)

        sv, sq, se, sm = self._score_valuation(b_val), self._score_qualidade(b_qual), self._score_eficiencia(b_rend), self._score_maturidade(b_mag, b_ret)
        score = sv + sq + se + sm
        if modo_conservador and score >= 70: score = 65.0
        if sum(1 for f in self._flags if "⚠️" in f) >= 2 and score >= 70: score = 65.0

        decisao, motivos = self._gerar_decisao(score, b_val, b_qual, b_rend)

        return DecisaoCard(ticker=self.ticker, decisao=decisao, score=score, score_valuation=sv, score_qualidade=sq, score_eficiencia=se, score_maturidade=sm, motivos=motivos, flags_qualidade=list(self._flags), modo_conservador=modo_conservador, provento=b_prov, rendimento_12m=b_rend, qualidade=b_qual, retorno=b_ret, valuation=b_val, magic=b_mag)

# ─────────────────────────────────────────────
# RENDERIZADORES
# ─────────────────────────────────────────────
def _brl(v: float) -> str:
    try: return "R$ " + f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception: return f"R$ {v}"

def _pct(v: float) -> str: return f"{v:.2f}%"

def card_to_telegram(card: DecisaoCard) -> str:
    if card is None: return ""
    prov, r12, q, ret, v, m = card.provento, card.rendimento_12m, card.qualidade, card.retorno, card.valuation, card.magic
    lines = [f"━━━━━━━━━━━━━━━━━━━━━━", f"🏷️ {card.ticker}", f"━━━━━━━━━━━━━━━━━━━━━━", "", "💰 PROVENTO RECEBIDO (REAL)"]
    if prov and prov.data_pgto:
        lines.extend([f"🕒 {prov.data_pgto.strftime('%d/%m/%Y')}  |  Base: {prov.qtd_base:.0f} cotas", f"💵 Recebido: {_brl(prov.valor_recebido)}  |  VPC: {_brl(prov.vpc_real)}"])
        if prov.flag_qtd: lines.append(f"   {prov.flag_qtd}")
    lines.extend(["", "📊 RENDIMENTO 12M (REAL)"])
    if r12:
        lines.extend([f"• Total recebido: {_brl(r12.total_12m)}", f"• Média mensal: {_brl(r12.media_mensal_12m)}", f"• YoC: {_pct(r12.yoc_12m)}  |  Yield atual: {_pct(r12.yield_preco_atual_12m)}", f"• Tempo em carteira: {r12.tempo_carteira_str}", r12.msg_eficiencia])
    lines.extend(["", "📈 QUALIDADE DO RENDIMENTO"])
    if q:
        lines.extend([f"• Estabilidade: {q.estabilidade} (CV={q.cv:.3f})", f"• Tendência 6M: {q.tendencia_6m} ({q.trend6:+.1f}%)", f"• Crescimento 12M: {q.crescimento_12m_str}"])
        if q.max_vpc > 0: lines.append(f"• VPC máx: {_brl(q.max_vpc)}  |  VPC mín: {_brl(q.min_vpc)}")
    lines.extend(["", "🕒 RETORNO DO CAPITAL"])
    if ret:
        lines.extend([f"• Payback: {ret.payback_str}", f"• Capital recuperado: {_pct(ret.capital_recuperado_pct)}"])
        if ret.flag_historico: lines.append(f"   {ret.flag_historico}")
    lines.extend(["", "🎯 VALUATION (BAZIN 6%)"])
    if v:
        sinal = "+" if v.agio_abs >= 0 else ""
        lines.extend([f"• Div. anual médio/cota: {_brl(v.dividendo_anual_medio_por_cota)}", f"• Preço teto: {_brl(v.preco_teto)}  |  Cotação: {_brl(v.preco_atual)}", f"• Ágio: {sinal}{v.agio_pct:.1f}% ({sinal}{_brl(v.agio_abs)})", f"• Zona: {v.zona}"])
        if v.flag_preco: lines.append(f"   {v.flag_preco}")
    lines.extend(["", "🎯 POSIÇÃO NA ESTRATÉGIA"])
    if m:
        if m.definido: lines.extend([f"• Magic Number: {m.magic_number:.0f} cotas", f"• Atual: {m.qtd_atual:.0f} cotas  |  Investido: {_brl(m.investido_atual)}", f"• Faltam: {m.faltam:.0f} cotas  |  Renda atual: {_brl(m.renda_mensal_atual)}/mês", f"• Alvo: {_brl(m.renda_alvo_mensal)}/mês"])
        else: lines.extend(["• Magic Number: — (defina meta mensal por ativo)", f"• Atual: {m.qtd_atual:.0f} cotas  |  Investido: {_brl(m.investido_atual)}", f"• Renda atual: {_brl(m.renda_mensal_atual)}/mês"])
    lines.extend(["", "────────────────────", f"🚦 {card.decisao}", f"Score: {card.score:.0f}/100  (V:{card.score_valuation:.0f} Q:{card.score_qualidade:.0f} E:{card.score_eficiencia:.0f} M:{card.score_maturidade:.0f})", "", "Motivos:"])
    for mot in card.motivos: lines.append(f"• {mot}")
    if card.flags_qualidade:
        lines.extend(["", "⚙️ Flags:"])
        for fl in card.flags_qualidade: lines.append(f"  {fl}")
    if card.modo_conservador: lines.append("⚠️ Modo conservador ativo (dados incompletos)")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━")
    return "\n".join(lines)

def card_to_streamlit(card: DecisaoCard) -> None:
    try: import streamlit as st
    except ImportError: print(card_to_telegram(card)); return
    if card is None: st.info("Ativo fora da carteira — card não gerado."); return
    prov, r12, q, ret, v, m = card.provento, card.rendimento_12m, card.qualidade, card.retorno, card.valuation, card.magic
    cor_decisao = {"🟢 APORTAR": "normal", "🟡 MANTER": "off", "🔴 AGUARDAR": "inverse"}
    st.subheader(f"📋 Card de Decisão — {card.ticker}")
    if card.flags_qualidade: st.caption("⚙️ " + " | ".join(card.flags_qualidade))
    with st.expander("💰 Provento Recebido (Real)", expanded=True):
        if prov and prov.data_pgto:
            c1, c2, c3 = st.columns(3)
            c1.metric("Data", prov.data_pgto.strftime("%d/%m/%Y")); c2.metric("Recebido", _brl(prov.valor_recebido)); c3.metric("VPC", _brl(prov.vpc_real))
            if prov.flag_qtd: st.caption(prov.flag_qtd)
    with st.expander("📊 Rendimento 12M (Real)", expanded=True):
        if r12:
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total 12M", _brl(r12.total_12m)); c2.metric("Média Mensal", _brl(r12.media_mensal_12m)); c3.metric("YoC 12M", _pct(r12.yoc_12m)); c4.metric("Yield Atual", _pct(r12.yield_preco_atual_12m))
            st.caption(f"Tempo em carteira: {r12.tempo_carteira_str}"); st.info(r12.msg_eficiencia)
    with st.expander("📈 Qualidade do Rendimento", expanded=False):
        if q:
            c1, c2, c3 = st.columns(3)
            c1.metric("Estabilidade", q.estabilidade, f"CV={q.cv:.3f}"); c2.metric("Tendência 6M", q.tendencia_6m, f"{q.trend6:+.1f}%"); c3.metric("Crescimento 12M", q.crescimento_12m_str)
            if q.max_vpc > 0: st.caption(f"VPC máx: {_brl(q.max_vpc)} | VPC mín: {_brl(q.min_vpc)}")
    with st.expander("🕒 Retorno do Capital", expanded=False):
        if ret:
            c1, c2 = st.columns(2)
            c1.metric("Payback Estimado", ret.payback_str if not math.isinf(ret.payback_anos) else "∞"); c2.metric("Capital Recuperado", _pct(ret.capital_recuperado_pct))
            if ret.flag_historico: st.caption(ret.flag_historico)
    with st.expander("🎯 Valuation (Bazin 6%)", expanded=True):
        if v:
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Div. Anual/Cota", _brl(v.dividendo_anual_medio_por_cota)); c2.metric("Preço Teto", _brl(v.preco_teto)); c3.metric("Cotação Atual", _brl(v.preco_atual))
            c4.metric("Ágio", f"{'+' if v.agio_pct >= 0 else ''}{v.agio_pct:.1f}%", _brl(v.agio_abs))
            st.markdown(f"**Zona:** {v.zona}")
            if v.flag_preco: st.caption(v.flag_preco)
    with st.expander("🎯 Posição na Estratégia", expanded=False):
        if m:
            if m.definido:
                c1, c2, c3 = st.columns(3)
                c1.metric("Magic Number", f"{m.magic_number:.0f} cotas"); c2.metric("Atual / Faltam", f"{m.qtd_atual:.0f} / {m.faltam:.0f}"); c3.metric("Renda Atual", _brl(m.renda_mensal_atual))
            else: st.info(f"Magic Number não definido. Atual: {m.qtd_atual:.0f} cotas | Renda: {_brl(m.renda_mensal_atual)}/mês")
    st.divider()
    col_d, col_s = st.columns([2, 1])
    with col_d: st.metric(label="🚦 Decisão Automática", value=card.decisao, delta=f"Score {card.score:.0f}/100", delta_color=cor_decisao.get(card.decisao, "off"))
    with col_s: st.markdown(f"**Parciais:** V={card.score_valuation:.0f} | Q={card.score_qualidade:.0f} | E={card.score_eficiencia:.0f} | M={card.score_maturidade:.0f}")
    st.markdown("**Motivos:**")
    for mot in card.motivos: st.markdown(f"• {mot}")
    if card.modo_conservador: st.warning("⚠️ Modo conservador ativo — dados incompletos.")

# ─────────────────────────────────────────────
# INSIDES (SISTEMA) — determinístico e auditável
# ─────────────────────────────────────────────
def build_insides_sistema(card: "DecisaoCard") -> str:
    if card is None: return ""
    v, q, r, lines = card.valuation, card.qualidade, card.rendimento_12m, []

    lines.append(f"📎 INSIDES (SISTEMA) — {card.ticker}")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("📊 Leitura direta dos indicadores:")
    lines.append("")

    if v:
        sinal = "+" if v.agio_pct >= 0 else ""
        lines.append(f"  Valuation: cotação {_brl(v.preco_atual)} vs teto {_brl(v.preco_teto)} ⇒ ágio {sinal}{v.agio_pct:.1f}% ⇒ {v.zona}")
    else: lines.append("  Valuation: dados insuficientes")

    if q: lines.append(f"  Qualidade: CV {q.cv:.3f} ({q.estabilidade}; CV=instabilidade, >0,10=instável) | tendência 6M {q.trend6:.1f}% ({q.tendencia_6m})")
    else: lines.append("  Qualidade: dados insuficientes")

    if r:
        diff = r.yoc_12m - r.yield_preco_atual_12m
        efi_msg = "YoC > Yield ⇒ posição eficiente" if diff > 0.3 else "YoC ≈ Yield ⇒ equivalente ao mercado" if abs(diff) <= 0.3 else "YoC < Yield ⇒ mercado mais eficiente"
        lines.append(f"  Rendimento: média 12M {_brl(r.media_mensal_12m)}/mês | YoC {r.yoc_12m:.2f}% vs Yield {r.yield_preco_atual_12m:.2f}% ⇒ {efi_msg}")
    else: lines.append("  Rendimento: dados insuficientes")

    lines.extend(["", "🔒 Regra acionada:", ""])
    decisao = card.decisao
    if "🔴" in decisao:
        if v and v.agio_pct > 10.0 and q and q.trend6 <= -3.0:
            lines.extend([f"  Gate A: ágio {v.agio_pct:.1f}% (>10%) e tendência {q.trend6:.1f}% (≤-3%)", "  ⇒ Bloqueio automático. Caro para uma renda em queda."])
            if q.cv >= 0.10: lines.append(f"  Obs: CV {q.cv:.3f} também indica instabilidade (reforça o alerta).")
        elif q and q.cv >= 0.10 and q.trend6 <= -3.0:
            lines.extend([f"  Gate B: CV {q.cv:.3f} (≥0,10) e tendência {q.trend6:.1f}% (≤-3%)", "  ⇒ Bloqueio automático. Proventos instáveis e em queda."])
        else:
            lines.extend([f"  Score {card.score:.0f}/100 abaixo do limiar mínimo (<40)", "  ⇒ Bloqueio por pontuação insuficiente."])
    elif "🟡" in decisao:
        lines.append(f"  Score {card.score:.0f}/100 na faixa intermediária (40–69)")
        if v and v.preco_atual > v.preco_teto: lines.append(f"  Ou cotação acima do teto (ágio {v.agio_pct:.1f}%) ⇒ sem aprovação verde.")
        lines.append("  ⇒ Status MANTER. Nem aporte nem saída.")
    elif "🟢" in decisao:
        lines.extend([f"  Score {card.score:.0f}/100 (≥70) e cotação ≤ teto Bazin", "  ⇒ Status APORTAR. Valuation saudável + qualidade OK."])

    lines.append("")
    if "🔴" in decisao:
        lines.extend(["🔓 O que precisa mudar para liberar o aporte:", ""])
        if v and v.agio_pct > 10:
            lines.append(f"  • Preço cair para ≤ {_brl(v.preco_teto * 1.10)} (até +10% do teto Bazin {_brl(v.preco_teto)})")
        if q and q.trend6 <= -3.0: lines.append(f"  • Tendência 6M melhorar para Estável (≥-3%)")
        if q and q.cv >= 0.10: lines.append(f"  • CV cair abaixo de 0,10 (proventos ficarem mais regulares)")
    elif "🟡" in decisao:
        lines.extend(["🔓 Para chegar ao verde:", "", f"  • Score subir para ≥70 (atual: {card.score:.0f})"])
        if v: lines.append(f"  • Cotação abaixo do teto ({_brl(v.preco_teto)})")

    lines.append("━━━━━━━━━━━━━━━━━━━━━━")
    return "\n".join(lines)


# ─────────────────────────────────────────────
# INSIDES (IA) — explicação Gemini (Mentoria Humana)
# ─────────────────────────────────────────────
def build_insides_ai(card: "DecisaoCard", api_key: str = "", model: str = "") -> str:
    if card is None or not api_key: return ""
    import json
    v, q, r, ret = card.valuation, card.qualidade, card.rendimento_12m, card.retorno
    
    payload = {
        "ticker": card.ticker, "decisao": card.decisao, "score": round(card.score, 1),
        "valuation": {"preco_teto": round(v.preco_teto, 2) if v else None, "preco_atual": round(v.preco_atual, 2) if v else None, "agio_pct": round(v.agio_pct, 1) if v else None},
        "qualidade": {"cv": round(q.cv, 3) if q else None, "tendencia_6m": q.tendencia_6m if q else None, "trend6_pct": round(q.trend6, 1) if q else None},
        "rendimento": {
            "yield_atual_pct": round(r.yield_preco_atual_12m, 2) if r else None,
            "yoc_12m_pct": round(r.yoc_12m, 2) if r else None,
            "media_mensal_reais": round(r.media_mensal_12m, 2) if r else None
        },
        "posicao": {
            "investido": round(card.magic.investido_atual, 2) if card.magic else 0.0,
            "cotas": round(card.magic.qtd_atual, 2) if card.magic else 0.0
        },
        "motivos_tecnicos": card.motivos
    }

    prompt = f"""Você é um mentor financeiro especialista em GERAÇÃO DE RENDA E APOSENTADORIA, conversando com um investidor de forma extremamente didática, humana e paciente.
O motor do sistema gerou uma recomendação. Sua missão é traduzir os dados, EXPLICAR o que cada métrica significa (como você fazia brilhantemente) e ADICIONAR a análise sobre a qualidade da renda.
NUNCA invente projeções futuras que não estejam nos dados.

Use EXATAMENTE a estrutura de títulos abaixo:

**TRADUÇÃO RÁPIDA**
[Vá direto ao ponto: O sistema mandou {card.decisao}. O que isso significa na prática para a carteira de aposentadoria dele hoje?]

**DEBULHANDO O ECONOMÊS (O QUE OS NÚMEROS DIZEM)**
[Explique em bullets detalhados e como um professor:
- Score: Explique a nota {card.score:.0f}/100 (lembre a regra de forma natural: 0 a 39 é alerta, 40 a 69 é neutro/manter, 70 para cima é aporte).
- Valuation: Explique o Preço Atual vs Preço Teto, e ensine o que significa o Ágio atual de forma simples.
- Foco em Renda: Fale da renda mensal média. Ensine a diferença do YoC atual vs Yield Atual para mostrar se ele está recebendo bem pelo que pagou.
- Qualidade e Previsibilidade: Explique o CV (diga se a renda é uma montanha-russa ou estável) e a Tendência dos proventos.]

**O QUE PRECISARIA ACONTECER PARA MUDAR**
[Liste 2 ou 3 eventos práticos focados no que precisa melhorar para esse ativo destravar um novo aporte.]

**AÇÃO PRÁTICA (MENTORIA)**
[Uma frase final de conselho impactante focando na proteção do patrimônio e geração de renda.]

Dados do ativo:
{json.dumps(payload, ensure_ascii=False, indent=2)}"""

    import urllib.request, urllib.error, time
    _t0, _budget = time.monotonic(), 90.0

    def _call(model_id: str) -> str:
        if (time.monotonic() - _t0) > _budget: raise TimeoutError('timeout')
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_id}:generateContent?key={api_key}"
        body = json.dumps({"contents": [{"role": "user", "parts": [{"text": prompt}]}], "generationConfig": {"temperature": 0.4, "maxOutputTokens": 3000}}).encode("utf-8")
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=45) as resp:
            return json.loads(resp.read().decode("utf-8")).get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "").strip()

    modelos_para_testar = [
        model if model else None,
        "gemini-2.5-flash",
        "gemini-2.0-flash-lite",
        "gemini-2.0-flash",
        "gemini-1.5-flash-8b",
        "gemini-1.5-pro",
    ]
    modelos_para_testar = [m for m in modelos_para_testar if m]

    for mod in modelos_para_testar:
        try:
            print(f"🤖 IA: Tentando conectar com o modelo {mod}...")
            text = _call(mod)
            if not text:
                continue
            # Se texto veio truncado, tenta 1x a mais com pequeno delay
            if _looks_truncated(text):
                print(f"⚠️ Texto truncado no modelo {mod}, tentando novamente...")
                time.sleep(3)
                text2 = _call(mod)
                if text2 and not _looks_truncated(text2):
                    text = text2
                    print(f"✅ Retry resolveu o truncamento para {mod}")
                else:
                    print(f"⚠️ Retry não resolveu — fallback será usado pelo PDF")
            print(f"✅ IA Sucesso! Modelo usado: {mod}")
            return text
        except Exception as e:
            err_str = str(e)
            print(f"⚠️ Modelo {mod} falhou. Motivo: {err_str}")
            if "429" in err_str:
                print(f"⏳ Rate limit da API atingido. Respirando por 15 segundos...")
                time.sleep(15)

    print("❌ Todos os modelos da IA falharam. Verifique se a sua API Key do Google está ativa no AI Studio.")
    return ""
# ─────────────────────────────────────────────
# INSIDES IA — RELATÓRIO MENSAL DE PROVENTOS
# ─────────────────────────────────────────────
def build_relatorio_ia(
    total_mes: float,
    media_12m: float,
    rank_mes,
    total_12m: float,
    top5_dict: list,          # lista de dicts: [{"ticker": ..., "total": ..., "pct": ...}]
    pend_total: float,
    proj_total: float,
    mes_nome: str,
    ano: int,
    historico_meses: list,    # lista de dicts: [{"label": "Jan/2026", "total": 839.72, "yoy": +12.4}, ...]
    api_key: str = "",
    model: str = "gemini-2.5-flash",
) -> str:
    """Gera análise IA do relatório mensal de proventos. Retorna texto puro para inserir no PDF."""
    if not api_key:
        return ""

    import json, urllib.request, urllib.error, time

    diff = total_mes - media_12m
    diff_pct = (diff / media_12m * 100.0) if media_12m > 0 else 0.0
    status_vs_media = f"{'acima' if diff >= 0 else 'abaixo'} da média em R$ {abs(diff):.2f} ({abs(diff_pct):.1f}%)"

    payload = {
        "mes_referencia": f"{mes_nome}/{ano}",
        "total_recebido_real": round(total_mes, 2),
        "media_12m": round(media_12m, 2),
        "status_vs_media": status_vs_media,
        "rank_mes_12m": rank_mes,
        "total_12m": round(total_12m, 2),
        "total_pendente": round(pend_total, 2),
        "projecao_total_mes": round(proj_total, 2),
        "top5_pagadores": top5_dict,
        "historico_recente": historico_meses[-6:] if len(historico_meses) >= 6 else historico_meses,
    }

    prompt = f"""Você é um mentor financeiro especialista em GERAÇÃO DE RENDA E APOSENTADORIA, conversando com um investidor de forma humana, didática e encorajadora.
O sistema gerou o relatório mensal de proventos. Sua missão é fazer uma análise COMPLETA e DETALHADA, como um mentor experiente faria.
NUNCA invente números além dos fornecidos. Use SOMENTE os dados do JSON abaixo. Escreva em português claro, sem economês.

Escreva em texto corrido (sem markdown, sem asteriscos, sem travessões decorativos). Use APENAS estas seções em maiúsculas como títulos:

ANALISE DO MES
Escreva 3 a 4 frases completas explicando como foi o mês: valor recebido, comparação com a média dos 12 meses, o que o ranking significa na prática para o investidor. Seja específico com os números.

DESTAQUES E PONTOS DE ATENCAO
Escreva 3 a 4 frases completas sobre: quais ativos foram os maiores pagadores e qual percentual do mês representaram (concentração). Explique o risco de concentração se houver. Fale dos pendentes: quanto ainda vai entrar e como fica a projeção total do mês quando tudo chegar.

TENDENCIA DA BOLA DE NEVE
Escreva 3 a 4 frases completas analisando o histórico recente mês a mês. A renda está crescendo ou caindo? Há sazonalidade (meses que sempre pagam mais ou menos)? O investidor está no caminho certo considerando o crescimento anual?

MENSAGEM FINAL
Escreva 2 frases de encorajamento genuíno, focando na consistência da jornada de longo prazo e na importância de cada provento reinvestido.

Dados do relatório:
{json.dumps(payload, ensure_ascii=False, indent=2)}"""

    def _call(model_id: str) -> str:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_id}:generateContent?key={api_key}"
        body = json.dumps({
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.5, "maxOutputTokens": 2048}
        }).encode("utf-8")
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=45) as resp:
            return json.loads(resp.read().decode("utf-8")).get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "").strip()

    modelos = [model, "gemini-2.5-flash", "gemini-2.0-flash-lite", "gemini-1.5-flash-8b"]
    modelos = [m for m in modelos if m]
    for mod in modelos:
        try:
            txt = _call(mod)
            if txt:
                print(f"✅ Relatório IA gerado com modelo {mod}")
                return txt
        except Exception as e:
            print(f"⚠️ Modelo {mod} falhou no relatório IA: {e}")
            if "429" in str(e):
                time.sleep(10)
    return ""


# ─────────────────────────────────────────────
# PDF SAFE TEXT (sanitização + fallback)
# ─────────────────────────────────────────────
import re as _re_pdfsafe

def _sanitize_ai_text_for_pdf(s: str) -> str:
    """Deixa o texto 100% estável para renderização no PDF (ReportLab Paragraph)."""
    if not s:
        return ""

    # Normaliza quebras
    s = str(s).replace("\r\n", "\n").replace("\r", "\n")

    # Remove markdown comum
    s = _re_pdfsafe.sub(r"\*\*(.*?)\*\*", r"\1", s)   # **negrito**
    s = _re_pdfsafe.sub(r"__(.*?)__", r"\1", s)           # __negrito__
    s = _re_pdfsafe.sub(r"^\s*#+\s*", "", s, flags=_re_pdfsafe.M)  # headers
    s = s.replace("`", "")

    # Troca emojis/símbolos por texto
    s = (s.replace("🔴", "VERMELHO")
           .replace("🟡", "AMARELO")
           .replace("🟢", "VERDE")
           .replace("✅", "OK")
           .replace("⚠️", "ALERTA")
           .replace("⚠", "ALERTA"))

    # Remove ícones decorativos
    s = (s.replace("📎", "")
           .replace("🔒", "")
           .replace("🔓", "")
           .replace("📊", "")
           .replace("📈", "")
           .replace("🧠", "")
           .replace("💰", "")
           .replace("🕒", "")
           .replace("🎯", "")
           .replace("🚦", "")
           .replace("⚙️", "")
           .replace("🗣️", "")
           .replace("🔍", "")
           .replace("🚧", ""))

    # Normaliza setas/traços “estranhos”
    s = (s.replace("–", "-")
           .replace("—", "-")
           .replace("→", "->")
           .replace("⇒", "=>")
           .replace("’", "'").replace("‘", "'")
           .replace("“", '"').replace("”", '"'))

    # Limita tamanho (evita resposta enorme travar PDF / Telegram)
    if len(s) > 8000:
        s = s[:8000] + "\n\n[texto encurtado automaticamente]"

    return s.strip()


def _looks_truncated(s: str) -> bool:
    """Detecta texto incompleto vindo da IA.

    Casos tratados:
    - Texto muito curto (< 200 chars)
    - Última linha não termina com pontuação conclusiva (frase cortada no meio)
    - Tem título de seção mas corpo muito curto (título sem conteúdo)
    - Não tem seções reconhecíveis e texto pequeno
    """
    if not s:
        return True
    ss = s.strip()
    if len(ss) < 200:
        return True

    # ── Última linha não termina com pontuação: frase cortada ──
    last_line = ""
    for line in reversed(ss.splitlines()):
        if line.strip():
            last_line = line.strip()
            break
    pontuacao_final = (".", "!", "?", "…", '"', "'", ")", "]", "—")
    if last_line and not last_line.endswith(pontuacao_final):
        is_titulo = last_line.isupper() and len(last_line) <= 80
        if not is_titulo:
            return True

    ss_up = ss.upper()

    # ── Tem título de seção mas corpo muito curto ──
    # Ex: "TRADUÇÃO RÁPIDA\n" seguido de quase nada
    has_section_title = any(k in ss_up for k in ("TRADU", "DEBULH", "RECADO", "AÇÃO PRÁTICA", "MENTOR"))
    if has_section_title and len(ss) < 400:
        return True

    # ── Verifica estrutura mínima: ao menos 2 seções com conteúdo ──
    has_numbered = ("1)" in ss and "2)" in ss and "3)" in ss)
    secoes = sum([
        1 if "TRADU" in ss_up or "RECADO" in ss_up else 0,
        1 if "DEBULH" in ss_up or "ECONOM" in ss_up else 0,
        1 if "PRECISA" in ss_up or "MUDAR" in ss_up or "ACONTEC" in ss_up else 0,
        1 if "PRÁTICA" in ss_up or "MENTOR" in ss_up or "CONSELHO" in ss_up or "AÇÃO" in ss_up else 0,
    ])
    has_titled = secoes >= 2

    if not has_numbered and not has_titled and len(ss) < 700:
        return True
    return False


def _fallback_explicacao_humana(card: "DecisaoCard") -> str:
    """Explicação humana determinística baseada no card (não usa IA)."""
    v = getattr(card, "valuation", None)
    q = getattr(card, "qualidade", None)
    r = getattr(card, "rendimento_12m", None)
    ret = getattr(card, "retorno", None)

    def _brl_(x):
        try:
            return "R$ " + f"{float(x):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        except Exception:
            return str(x)

    linhas = []
    linhas.append(f"1) Recado principal")
    linhas.append(f"Decisão do sistema: {getattr(card, 'decisao', 'N/D')} (score {getattr(card, 'score', 0):.0f}/100).")
    linhas.append("")
    linhas.append("2) Tradução dos números")
    if v:
        linhas.append(f"- Preço vs teto: cotação {_brl_(getattr(v,'preco_atual', 'N/D'))} vs teto {_brl_(getattr(v,'preco_teto','N/D'))} | ágio {getattr(v,'agio_pct','N/D'):+.1f}%.")
        try:
            linhas.append(f"- Zona: {getattr(v,'zona','N/D')}.")
        except Exception:
            pass
    else:
        linhas.append("- Preço vs teto: não disponível.")
    if q:
        linhas.append(f"- Tendência 6M: {getattr(q,'trend6','N/D'):+.1f}% ({getattr(q,'tendencia_6m','N/D')}).")
        linhas.append(f"- Regularidade: CV {getattr(q,'cv','N/D'):.3f} (instável quando > 0,10).")
    else:
        linhas.append("- Tendência/regularidade: não disponível.")
    if r:
        linhas.append(f"- Renda 12M: média {_brl_(getattr(r,'media_mensal_12m','N/D'))}/mês | YoC {getattr(r,'yoc_12m','N/D'):.2f}% | yield atual {getattr(r,'yield_preco_atual_12m','N/D'):.2f}%.")
    if ret:
        linhas.append(f"- Payback: {getattr(ret,'payback_str','N/D')}.")
    linhas.append("")
    linhas.append("3) O que eu faria na prática")

    # ✅ FIX: texto baseado na decisão real do card (não mais texto fixo de VERMELHO)
    decisao_str = str(getattr(card, "decisao", "") or "")
    agio  = getattr(v, "agio_pct", 0) if v else 0
    cv    = getattr(q, "cv", 0) if q else 0
    trend = str(getattr(q, "tendencia_6m", "") if q else "")

    if "APORTAR" in decisao_str or "🟢" in decisao_str:
        linhas.append(f"- Ativo em zona de aporte: preço abaixo do teto com ágio de {agio:+.1f}% — boa margem de segurança.")
        if cv > 0.10:
            linhas.append(f"- Regularidade ainda instável (CV {cv:.3f}) — considere aportar em parcelas menores.")
        else:
            linhas.append(f"- Regularidade adequada (CV {cv:.3f}) — ativo consistente na geração de renda.")
        linhas.append("- Recomendação: aproveitar o preço atual para aumentar posição gradualmente.")
    elif "MANTER" in decisao_str or "🟡" in decisao_str:
        linhas.append(f"- Ativo em zona neutra: ágio de {agio:+.1f}% em relação ao teto — não está barato o suficiente para aportar agressivamente.")
        linhas.append("- Recomendação: manter a posição atual e monitorar. Não vender, mas também não aportar forte agora.")
    else:  # AGUARDAR / VERMELHO
        linhas.append(f"- Ativo acima do preço teto: ágio de {agio:+.1f}% indica preço esticado.")
        if "queda" in trend.lower():
            linhas.append("- Tendência de queda reforça a cautela — aguarde estabilização antes de agir.")
        linhas.append("- Recomendação: aguardar ajuste de preço e reavaliar quando o ágio cair.")

    return "\n".join(linhas).strip()

# ─────────────────────────────────────────────
# INSIDES (IA) — gerador de PDF (REPORTLAB)
# ─────────────────────────────────────────────
def build_insides_ia_pdf(card: "DecisaoCard", texto_ia: str, insides_sistema: str = "") -> Optional["io.BytesIO"]:
    """Gera o PDF do INSIDES (IA). Nunca deve quebrar por Markdown/emoji/truncamento.

    - Sanitiza o texto vindo da IA
    - Se vier truncado (ou vazio), adiciona complemento determinístico
    - Usa ReportLab (wrap confiável)
    """
    if not card:
        return None

    try:
        import io
        from xml.sax.saxutils import escape
        from datetime import datetime
        from reportlab.lib.pagesizes import A4
        from reportlab.lib import colors
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle

        # 1) texto IA "pdf-safe" + fallback se truncado
        base_txt = _sanitize_ai_text_for_pdf(texto_ia or "")
        if _looks_truncated(base_txt):
            complemento = _sanitize_ai_text_for_pdf(_fallback_explicacao_humana(card))
            if base_txt:
                base_txt = base_txt + "\n\n[Complemento automático]\n" + complemento
            else:
                base_txt = complemento

        # 2) prepara doc
        buf = io.BytesIO()
        doc = SimpleDocTemplate(
            buf,
            pagesize=A4,
            rightMargin=30,
            leftMargin=30,
            topMargin=30,
            bottomMargin=30
        )

        styles = getSampleStyleSheet()
        style_title = ParagraphStyle(
            "TitleStyle",
            parent=styles["Heading1"],
            alignment=1,
            fontSize=14,
            spaceAfter=14
        )
        style_heading = ParagraphStyle(
            "Heading2Style",
            parent=styles["Heading2"],
            fontSize=12,
            spaceAfter=10,
            textColor=colors.HexColor("#1A365D")
        )
        style_normal = ParagraphStyle(
            "NormalStyle",
            parent=styles["Normal"],
            fontSize=10,
            spaceAfter=6,
            leading=14
        )
        style_bullet = ParagraphStyle(
            "BulletStyle",
            parent=styles["Normal"],
            fontSize=10,
            spaceAfter=4,
            leading=14,
            leftIndent=15
        )
        style_sub = ParagraphStyle(
            "SubStyle",
            parent=styles["Normal"],
            fontSize=9,
            spaceAfter=6,
            textColor=colors.darkgrey
        )

        elements = []

        elements.append(Paragraph(f"<b>Visão Estratégica — {escape(str(card.ticker))}</b>", style_title))
        elements.append(Paragraph(f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}", style_sub))
        elements.append(Spacer(1, 10))

        # 3) renderiza texto
        def clean_text_line(t: str) -> str:
            # remove qualquer resto de markup estranho (sem perder conteúdo)
            return (t or "").strip()

        # escape para evitar markup quebrar, depois reintroduz <b> simples se vier no padrão "TÍTULO:".
        safe_all = escape(base_txt)

        # suporte opcional: se vier "TÍTULO:" sozinho, transforma em heading
        for raw in safe_all.splitlines():
            line = clean_text_line(raw)
            if not line:
                elements.append(Spacer(1, 4))
                continue

            # bullets
            if line.startswith(("•", "-", "*")):
                # remove marcador duplicado visual
                line2 = line
                elements.append(Paragraph(line2, style_bullet))
                continue

            # headings (heurística)
            if (len(line) <= 60 and line.endswith(":")) or line.upper() == line and len(line) <= 70:
                elements.append(Spacer(1, 6))
                elements.append(Paragraph(f"<b>{line}</b>", style_heading))
                continue

            elements.append(Paragraph(line, style_normal))

        elements.append(Spacer(1, 18))

        # 4) dados técnicos (sempre)
        elements.append(Paragraph("<b>Dados Técnicos do Motor</b>", style_heading))
        decisao = escape(str(getattr(card, "decisao", "N/D")))
        score = getattr(card, "score", 0.0)
        elements.append(Paragraph(f"<b>Decisão Oficial:</b> {decisao} (Score: {score:.0f}/100)", style_normal))
        
        # A função de formatar o dinheiro PRECISA vir antes agora:
        def _brl_(v):
            try:
                return "R$ " + f"{float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            except Exception:
                return str(v)

        # --- NOVA LINHA COM O TOTAL INVESTIDO ---
        qtd_atual = getattr(card.magic, "qtd_atual", 0) if getattr(card, "magic", None) else 0
        inv_atual = getattr(card.magic, "investido_atual", 0) if getattr(card, "magic", None) else 0
        elements.append(Paragraph(f"<b>Posição Atual:</b> {qtd_atual:.0f} cotas | <b>Total Investido:</b> {_brl_(inv_atual)}", style_normal))
        # ----------------------------------------
        
        elements.append(Spacer(1, 10))

        def _brl_(v):
            try:
                return "R$ " + f"{float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            except Exception:
                return str(v)

        def add_table(headers, row_data, col_widths):
            t = Table([headers, row_data], colWidths=col_widths)
            t.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#F0F4F8")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
                ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("BOTTOMPADDING", (0, 0), (-1, 0), 6),
                ("GRID", (0, 0), (-1, -1), 1, colors.HexColor("#D1D5DB")),
            ]))
            elements.append(t)
            elements.append(Spacer(1, 10))

        v = getattr(card, "valuation", None)
        q = getattr(card, "qualidade", None)
        r = getattr(card, "rendimento_12m", None)

        if v:
            add_table(
                ["Div.anual", "Preço teto", "Cotação", "Ágio", "Zona"],
                [
                    _brl_(getattr(v, "dividendo_anual_medio_por_cota", "")),
                    _brl_(getattr(v, "preco_teto", "")),
                    _brl_(getattr(v, "preco_atual", "")),
                    f"{'+' if getattr(v, 'agio_pct', 0) >= 0 else ''}{getattr(v, 'agio_pct', 0):.1f}%",
                    escape(str(getattr(v, "zona", ""))),
                ],
                [80, 80, 80, 60, 200],
            )

        if q:
            add_table(
                ["Estabilidade", "CV", "Tendência 6M", "Trend6", "Cresc.12M"],
                [
                    escape(str(getattr(q, "estabilidade", ""))),
                    f"{getattr(q, 'cv', 0):.3f}",
                    escape(str(getattr(q, "tendencia_6m", ""))),
                    f"{getattr(q, 'trend6', 0):+.1f}%",
                    escape(str(getattr(q, "crescimento_12m_str", ""))),
                ],
                [100, 60, 110, 80, 100],
            )

        # (opcional) renda/retorno pode existir — deixa para versões futuras, sem quebrar nada
        doc.build(elements)

        buf.name = f"Visao_Estrategica_{card.ticker}_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf"
        buf.seek(0)
        return buf

    except Exception as e:
        print(f"❌ ERRO REPORTLAB (PDF): {e}")
        return None

# ─────────────────────────────────────────────
# AUDITORIA PERSISTENTE E ENTRY POINT
# ─────────────────────────────────────────────
_AUDIT_FILE_DEFAULT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "card_decisao_audit.jsonl")

def registrar_auditoria(card: DecisaoCard, filepath: Optional[str] = None) -> bool:
    if card is None: return False
    path = filepath or _AUDIT_FILE_DEFAULT
    registro = {
        "timestamp": card.timestamp.isoformat(), "ticker": card.ticker, "decisao": card.decisao,
        "score": round(card.score, 2), "score_valuation": round(card.score_valuation, 2), "score_qualidade": round(card.score_qualidade, 2), "score_eficiencia": round(card.score_eficiencia, 2), "score_maturidade": round(card.score_maturidade, 2),
        "motivos": card.motivos, "flags_qualidade": card.flags_qualidade, "modo_conservador": card.modo_conservador,
        "metricas": {
            "total_12m": round(card.rendimento_12m.total_12m, 4) if card.rendimento_12m else None, "yoc_12m": round(card.rendimento_12m.yoc_12m, 4) if card.rendimento_12m else None, "yield_atual": round(card.rendimento_12m.yield_preco_atual_12m, 4) if card.rendimento_12m else None,
            "preco_teto": round(card.valuation.preco_teto, 4) if card.valuation else None, "preco_atual": round(card.valuation.preco_atual, 4) if card.valuation else None, "agio_pct": round(card.valuation.agio_pct, 2) if card.valuation else None, "zona": card.valuation.zona if card.valuation else None,
            "estabilidade": card.qualidade.estabilidade if card.qualidade else None, "tendencia_6m": card.qualidade.tendencia_6m if card.qualidade else None, "trend6": round(card.qualidade.trend6, 2) if card.qualidade else None,
            "capital_recuperado_pct": round(card.retorno.capital_recuperado_pct, 2) if card.retorno else None
        }
    }
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a", encoding="utf-8") as f: f.write(json.dumps(registro, ensure_ascii=False) + "\n")
        return True
    except Exception: return False

def carregar_historico_auditoria(ticker: Optional[str] = None, filepath: Optional[str] = None, ultimos_n: int = 50) -> List[dict]:
    path = filepath or _AUDIT_FILE_DEFAULT
    if not os.path.exists(path): return []
    registros = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for linha in f:
                if not linha.strip(): continue
                try:
                    r = json.loads(linha.strip())
                    if ticker is None or r.get("ticker") == str(ticker).upper().strip(): registros.append(r)
                except Exception: continue
    except Exception: return []
    return list(reversed(registros))[:ultimos_n]

def gerar_card(ticker: str, df_proventos: pd.DataFrame, df_movimentacoes: pd.DataFrame, df_cotacoes: Optional[pd.DataFrame] = None, df_ativos: Optional[pd.DataFrame] = None, renda_alvo_mensal: Optional[float] = None, hoje: Optional[date] = None) -> Optional[DecisaoCard]:
    motor = CardDecisaoMotor(ticker=ticker, df_proventos=df_proventos, df_movimentacoes=df_movimentacoes, df_cotacoes=df_cotacoes, df_ativos=df_ativos, renda_alvo_mensal_ativo=renda_alvo_mensal, hoje=hoje)
    return motor.calcular()
586271
