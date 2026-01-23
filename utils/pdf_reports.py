# utils/pdf_reports.py
# -*- coding: utf-8 -*-

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple
import io
import unicodedata

import pandas as pd
import requests

try:
    from fpdf import FPDF
    HAS_FPDF = True
except ImportError:
    HAS_FPDF = False


# =========================================================
# Result (para a página checar ok_exec/ok_aud)
# =========================================================
@dataclass
class PDFSendResult:
    ok_exec: bool
    ok_aud: bool
    err_exec: Optional[str] = None
    err_aud: Optional[str] = None


# =========================================================
# Helpers robustos (SEM Streamlit aqui!)
# =========================================================
def _safe_float(x) -> float:
    try:
        if x is None:
            return 0.0
        s = str(x).strip()
        if s == "" or s.lower() == "nan":
            return 0.0
        if "," in s:
            s = s.replace(".", "").replace(",", ".")
        return float(s)
    except Exception:
        return 0.0


def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame(df).copy()
    if df.empty:
        return df
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


def _fmt_brl(v: float) -> str:
    s = f"{float(v):,.2f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")


def _fmt_pct(v: float) -> str:
    return f"{float(v):.1f}%".replace(".", ",")


def _month_name(m: int) -> str:
    names = ["", "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
             "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]
    return names[m] if 1 <= m <= 12 else ""


def _pdf_text(s) -> str:
    """
    Sanitiza texto para não quebrar no FPDF (Helvetica/WinAnsi).
    Troca travessões e aspas “espertas”, remove caracteres fora de latin-1.
    """
    if s is None:
        return ""
    s = str(s)
    s = s.replace("-", "-").replace("–", "-").replace("−", "-")
    s = s.replace("“", '"').replace("”", '"').replace("’", "'").replace("‘", "'")
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("latin-1", "ignore").decode("latin-1")
    return s


# =========================================================
# Normalização / snapshots
# =========================================================
def prepare_proventos_df(df_proventos: pd.DataFrame, df_ativos: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    if df_proventos is None:
        return pd.DataFrame()

    df = pd.DataFrame(df_proventos).copy()
    if df.empty:
        return df

    df = _norm_cols(df)

    if "ticker" not in df.columns:
        df["ticker"] = ""
    df["ticker_norm"] = df["ticker"].astype(str).str.upper().str.strip()

    if "data" not in df.columns:
        df["data"] = None
    df["data_dt"] = pd.to_datetime(df["data"], dayfirst=True, errors="coerce")
    df["data_date"] = df["data_dt"].dt.date

    if "valor" not in df.columns:
        df["valor"] = 0
    df["valor_float"] = df["valor"].apply(_safe_float)

    if "tipo" not in df.columns:
        df["tipo"] = ""
    df["tipo_norm"] = df["tipo"].astype(str).str.upper().str.strip()

    if "valor_por_cota" not in df.columns:
        df["valor_por_cota"] = 0
    df["vpc_float"] = df["valor_por_cota"].apply(_safe_float)

    if "quantidade_na_data" not in df.columns:
        df["quantidade_na_data"] = 0
    df["qtd_float"] = df["quantidade_na_data"].apply(_safe_float)

    if "origem" not in df.columns:
        df["origem"] = ""

    if "id" not in df.columns:
        df["id"] = ""

    df["classe_norm"] = "OUTROS"
    if df_ativos is not None and not pd.DataFrame(df_ativos).empty:
        a = _norm_cols(df_ativos)
        if "ticker" in a.columns:
            a["ticker_norm"] = a["ticker"].astype(str).str.upper().str.strip()
            if "classe" in a.columns:
                m = dict(zip(a["ticker_norm"], a["classe"].astype(str).str.upper().str.strip()))
                df["classe_norm"] = df["ticker_norm"].map(m).fillna("OUTROS")

    return df


def compute_month_snapshot(df: pd.DataFrame, data_ref: date) -> Dict:
    if df is None or df.empty:
        return {
            "df_month": pd.DataFrame(),
            "total_mes": 0.0,
            "eventos_mes": 0,
            "ativos_pagantes": 0,
            "top5": pd.DataFrame(),
            "por_classe": pd.DataFrame(),
        }

    m, y = data_ref.month, data_ref.year
    dfm = df[(df["data_dt"].dt.month == m) & (df["data_dt"].dt.year == y)].copy()
    dfm = dfm[dfm["valor_float"] > 0].copy()

    total_mes = float(dfm["valor_float"].sum()) if not dfm.empty else 0.0
    eventos_mes = int(len(dfm))
    ativos_pagantes = int(dfm["ticker_norm"].nunique()) if not dfm.empty else 0

    top = pd.DataFrame()
    if not dfm.empty and total_mes > 0:
        g = dfm.groupby("ticker_norm", as_index=False).agg(
            total=("valor_float", "sum"),
            eventos=("ticker_norm", "count"),
        )
        g["pct_mes"] = (g["total"] / total_mes) * 100.0
        g = g.sort_values("total", ascending=False)
        top = g.head(5).reset_index(drop=True)

    cls = pd.DataFrame()
    if not dfm.empty and total_mes > 0:
        c = dfm.groupby("classe_norm", as_index=False).agg(total=("valor_float", "sum"))
        c["pct_mes"] = (c["total"] / total_mes) * 100.0
        cls = c.sort_values("total", ascending=False).reset_index(drop=True)

    return {
        "df_month": dfm,
        "total_mes": total_mes,
        "eventos_mes": eventos_mes,
        "ativos_pagantes": ativos_pagantes,
        "top5": top,
        "por_classe": cls,
    }


def compute_ltm_snapshot(df: pd.DataFrame, data_ref: date) -> Dict:
    if df is None or df.empty:
        return {"total_12m": 0.0, "media_12m": 0.0, "rank_mes": None, "df_12m": pd.DataFrame()}

    start = pd.Timestamp(data_ref).replace(day=1) - pd.DateOffset(months=11)
    df12 = df[df["data_dt"] >= start].copy()
    total_12m = float(df12["valor_float"].sum()) if not df12.empty else 0.0
    media_12m = float(total_12m / 12) if total_12m > 0 else 0.0

    rank_mes = None
    if not df12.empty:
        tmp = df12.copy()
        tmp["ym"] = tmp["data_dt"].dt.to_period("M").astype(str)
        g = tmp.groupby("ym", as_index=False).agg(total=("valor_float", "sum"))
        g = g.sort_values("total", ascending=False).reset_index(drop=True)
        g["rank"] = range(1, len(g) + 1)
        ym_ref = pd.Timestamp(data_ref).to_period("M").strftime("%Y-%m")
        hit = g[g["ym"] == ym_ref]
        if not hit.empty:
            rank_mes = int(hit.iloc[0]["rank"])

    return {"total_12m": total_12m, "media_12m": media_12m, "rank_mes": rank_mes, "df_12m": df12}


def prepare_pendentes(df_anunciados: Optional[pd.DataFrame], data_ref: date) -> pd.DataFrame:
    if df_anunciados is None:
        return pd.DataFrame()
    df = pd.DataFrame(df_anunciados).copy()
    if df.empty:
        return df

    df = _norm_cols(df)

    # ---- ALIASES (SEU CASO: data_pagamentc) ----
    rename_map = {}
    if "data_pagamentc" in df.columns and "data_pagamento" not in df.columns:
        rename_map["data_pagamentc"] = "data_pagamento"
    if "data_pagto" in df.columns and "data_pagamento" not in df.columns:
        rename_map["data_pagto"] = "data_pagamento"
    if "pagamento" in df.columns and "data_pagamento" not in df.columns:
        rename_map["pagamento"] = "data_pagamento"
    if "tipo_pagto" in df.columns and "tipo_pagamento" not in df.columns:
        rename_map["tipo_pagto"] = "tipo_pagamento"
    if rename_map:
        df = df.rename(columns=rename_map)

    for c in [
        "ticker", "tipo_ativo", "status", "tipo_pagamento",
        "data_com", "data_pagamento", "valor_por_cota",
        "quantidade_ref", "fonte_url"
    ]:
        if c not in df.columns:
            df[c] = ""

    df["ticker_norm"] = df["ticker"].astype(str).str.upper().str.strip()
    df["status_norm"] = df["status"].astype(str).str.upper().str.strip()

    df["dt_pag"] = pd.to_datetime(df["data_pagamento"], dayfirst=True, errors="coerce")
    # aceita yyyy-mm-dd também
    mask_nat = df["dt_pag"].isna() & df["data_pagamento"].astype(str).str.match(r"^\d{4}-\d{2}-\d{2}$", na=False)
    if mask_nat.any():
        df.loc[mask_nat, "dt_pag"] = pd.to_datetime(df.loc[mask_nat, "data_pagamento"], format="%Y-%m-%d", errors="coerce")

    df = df[df["status_norm"] == "ANUNCIADO"].copy()

    m, y = data_ref.month, data_ref.year
    df = df[(df["dt_pag"].dt.month == m) & (df["dt_pag"].dt.year == y)].copy()
    if df.empty:
        return df

    df["vpc_float"] = df["valor_por_cota"].apply(_safe_float)
    df["qtd_float"] = df["quantidade_ref"].apply(_safe_float)
    df["valor_estimado_float"] = df["qtd_float"] * df["vpc_float"]

    df["data_pag_br"] = df["dt_pag"].dt.strftime("%d/%m/%Y")
    df = df.sort_values(["dt_pag", "ticker_norm"]).reset_index(drop=True)
    return df


# =========================================================
# PDF base
# =========================================================
class _BasePDF(FPDF):
    def header(self):
        self.set_font("Arial", "B", 13)
        self.cell(0, 8, _pdf_text("Central de Investimentos"), 0, 1, "C")
        self.set_font("Arial", "", 10)
        self.cell(0, 6, _pdf_text("Relatorios de Proventos"), 0, 1, "C")
        self.ln(2)

    def footer(self):
        self.set_y(-14)
        self.set_font("Arial", "I", 8)
        self.cell(0, 8, _pdf_text(f"Pagina {self.page_no()}/{{nb}}"), 0, 0, "C")


def _add_section_title(pdf: _BasePDF, title: str):
    pdf.set_font("Arial", "B", 11)
    pdf.set_fill_color(230, 235, 245)
    pdf.cell(0, 8, _pdf_text(f"  {title}"), 0, 1, "L", True)
    pdf.ln(1)


def _add_kv(pdf: _BasePDF, k: str, v: str):
    pdf.set_font("Arial", "", 10)
    pdf.cell(55, 6, _pdf_text(k), 0, 0, "L")
    pdf.set_font("Arial", "B", 10)
    pdf.cell(0, 6, _pdf_text(v), 0, 1, "L")


def _add_table(pdf: _BasePDF, headers: List[str], rows: List[List[str]], col_widths: List[int]):
    pdf.set_font("Arial", "B", 9)
    pdf.set_fill_color(245, 245, 245)
    for h, w in zip(headers, col_widths):
        pdf.cell(w, 6, _pdf_text(h), 1, 0, "L", True)
    pdf.ln()

    pdf.set_font("Arial", "", 9)
    for r in rows:
        for val, w in zip(r, col_widths):
            s = _pdf_text(val)
            if len(s) > 38:
                s = s[:35] + "..."
            pdf.cell(w, 6, s, 1, 0, "L")
        pdf.ln()
    pdf.ln(1)


# =========================================================
# Builders
# =========================================================
def build_pdf_executivo(
    df_proventos_raw: pd.DataFrame,
    df_ativos: pd.DataFrame,
    data_ref: date,
    df_anunciados_raw: Optional[pd.DataFrame] = None,
) -> io.BytesIO:
    if not HAS_FPDF:
        raise RuntimeError("FPDF não instalado.")

    df = prepare_proventos_df(df_proventos_raw, df_ativos)
    snap_m = compute_month_snapshot(df, data_ref)
    snap_12 = compute_ltm_snapshot(df, data_ref)

    total_mes = snap_m["total_mes"]
    eventos_mes = snap_m["eventos_mes"]
    ativos_pagantes = snap_m["ativos_pagantes"]

    total_12m = snap_12["total_12m"]
    media_12m = snap_12["media_12m"]
    rank_mes = snap_12["rank_mes"]

    diff = total_mes - media_12m
    diff_pct = (diff / media_12m * 100.0) if media_12m > 0 else 0.0

    top5 = snap_m["top5"]
    cls = snap_m["por_classe"]

    pend = prepare_pendentes(df_anunciados_raw, data_ref) if df_anunciados_raw is not None else pd.DataFrame()
    pend_total = float(pend["valor_estimado_float"].sum()) if (pend is not None and not pend.empty and "valor_estimado_float" in pend.columns) else 0.0
    proj_total = total_mes + pend_total

    pdf = _BasePDF()
    pdf.alias_nb_pages()
    pdf.add_page()
    pdf.set_text_color(40, 40, 40)

    mes_nome = _month_name(data_ref.month)
    _add_section_title(pdf, f"Executivo - {mes_nome}/{data_ref.year}")

    _add_kv(pdf, "Total recebido (REAL):", f"R$ {_fmt_brl(total_mes)}")
    _add_kv(pdf, "Eventos no mês:", f"{eventos_mes}")
    _add_kv(pdf, "Ativos pagantes:", f"{ativos_pagantes}")
    _add_kv(pdf, "Média 12m:", f"R$ {_fmt_brl(media_12m)}")

    status = "Acima" if diff >= 0 else "Abaixo"
    _add_kv(pdf, "Status vs média 12m:", f"{status} (R$ {_fmt_brl(diff)} | {_fmt_pct(diff_pct)})")

    if rank_mes is not None:
        _add_kv(pdf, "Ranking do mês (12m):", f"{rank_mes}º melhor")

    pdf.ln(2)

    _add_section_title(pdf, "Top 5 pagadores do mês")
    rows = []
    top5_share = 0.0
    if not top5.empty and total_mes > 0:
        for _, r in top5.iterrows():
            rows.append([
                str(r["ticker_norm"]),
                f"R$ {_fmt_brl(r['total'])}",
                _fmt_pct(r["pct_mes"]),
                str(int(r["eventos"])),
            ])
            top5_share += float(r["pct_mes"])
    if rows:
        _add_table(pdf, ["Ativo", "Total", "% do mês", "Eventos"], rows, [32, 45, 30, 20])
        pdf.set_font("Arial", "I", 9)
        pdf.cell(0, 6, _pdf_text(f"Top 5 respondeu por {str(round(top5_share,1)).replace('.',',')}% do mês."), 0, 1, "L")
        pdf.ln(1)
    else:
        pdf.set_font("Arial", "", 9)
        pdf.cell(0, 6, _pdf_text("Sem dados de top pagadores para este mês."), 0, 1, "L")
        pdf.ln(1)

    _add_section_title(pdf, "Pendentes do mês (A RECEBER)")
    if pend is not None and not pend.empty:
        rows_p = []
        for _, r in pend.head(10).iterrows():
            rows_p.append([
                str(r["ticker_norm"]),
                str(r.get("data_pag_br", "")),
                f"R$ {_fmt_brl(r.get('valor_estimado_float', 0.0))}",
            ])
        _add_table(pdf, ["Ativo", "Pagamento", "Estimado"], rows_p, [32, 38, 40])
        _add_kv(pdf, "Total pendente:", f"R$ {_fmt_brl(pend_total)}")
        _add_kv(pdf, "Total projetado (REAL+pend.):", f"R$ {_fmt_brl(proj_total)}")
    else:
        pdf.set_font("Arial", "", 9)
        pdf.cell(0, 6, _pdf_text("Nenhum pendente no mês (ou base anunciada indisponível)."), 0, 1, "L")

    pdf.ln(1)

    _add_section_title(pdf, "Distribuição por classe (REAL no mês)")
    if cls is not None and not cls.empty and total_mes > 0:
        rows_c = []
        for _, r in cls.head(5).iterrows():
            rows_c.append([
                str(r["classe_norm"]),
                f"R$ {_fmt_brl(r['total'])}",
                _fmt_pct(r["pct_mes"]),
            ])
        _add_table(pdf, ["Classe", "Total", "%"], rows_c, [45, 45, 30])
    else:
        pdf.set_font("Arial", "", 9)
        pdf.cell(0, 6, _pdf_text("Sem dados por classe."), 0, 1, "L")

    pdf.ln(1)
    pdf.set_font("Arial", "I", 8)
    ts = datetime.now().strftime("%d/%m/%Y %H:%M")
    start_12m = (pd.Timestamp(data_ref).replace(day=1) - pd.DateOffset(months=11)).date().strftime("%d/%m/%Y")
    pdf.cell(0, 5, _pdf_text(f"Gerado em: {ts} | Janela 12m: desde {start_12m}"), 0, 1, "L")

    b = pdf.output(dest="S")
    if isinstance(b, str):
        b = b.encode("latin-1", errors="replace")
    buf = io.BytesIO(b)
    buf.name = f"Proventos_Executivo_{data_ref.strftime('%Y-%m')}.pdf"
    buf.seek(0)
    return buf


def build_pdf_auditoria(
    df_proventos_raw: pd.DataFrame,
    df_ativos: pd.DataFrame,
    data_ref: date,
    df_anunciados_raw: Optional[pd.DataFrame] = None,
) -> io.BytesIO:
    if not HAS_FPDF:
        raise RuntimeError("FPDF não instalado.")

    df = prepare_proventos_df(df_proventos_raw, df_ativos)
    snap_m = compute_month_snapshot(df, data_ref)
    snap_12 = compute_ltm_snapshot(df, data_ref)

    dfm = snap_m["df_month"]
    total_mes = snap_m["total_mes"]
    eventos_mes = snap_m["eventos_mes"]

    total_12m = snap_12["total_12m"]
    media_12m = snap_12["media_12m"]

    pend = prepare_pendentes(df_anunciados_raw, data_ref) if df_anunciados_raw is not None else pd.DataFrame()

    pdf = _BasePDF()
    pdf.alias_nb_pages()
    pdf.add_page()
    pdf.set_text_color(35, 35, 35)

    mes_nome = _month_name(data_ref.month)
    _add_section_title(pdf, f"Auditoria - {mes_nome}/{data_ref.year}")

    _add_kv(pdf, "Total mês (REAL):", f"R$ {_fmt_brl(total_mes)}")
    _add_kv(pdf, "Eventos mês:", str(eventos_mes))
    _add_kv(pdf, "Total 12m:", f"R$ {_fmt_brl(total_12m)}")
    _add_kv(pdf, "Média 12m:", f"R$ {_fmt_brl(media_12m)}")
    pdf.ln(2)

    _add_section_title(pdf, "Sumário técnico - contagem por tipo (mês)")
    if dfm is not None and not dfm.empty:
        g = dfm.groupby("tipo_norm", as_index=False).agg(
            eventos=("tipo_norm", "count"),
            total=("valor_float", "sum"),
        ).sort_values("total", ascending=False)
        rows = []
        for _, r in g.iterrows():
            rows.append([str(r["tipo_norm"] or "SEM_TIPO"), str(int(r["eventos"])), f"R$ {_fmt_brl(r['total'])}"])
        _add_table(pdf, ["Tipo", "Eventos", "Total"], rows, [55, 25, 55])
    else:
        pdf.set_font("Arial", "", 9)
        pdf.cell(0, 6, _pdf_text("Sem dados no mês."), 0, 1, "L")

    _add_section_title(pdf, "Tabela completa do mês (REAL)")
    if dfm is not None and not dfm.empty:
        dfm2 = dfm.copy()
        dfm2["data_iso"] = dfm2["data_dt"].dt.strftime("%Y-%m-%d")
        dfm2["data_br"] = dfm2["data_dt"].dt.strftime("%d/%m/%Y")
        dfm2 = dfm2.sort_values(["data_dt", "ticker_norm"]).reset_index(drop=True)

        headers = ["Data ISO", "Data BR", "Ativo", "Tipo", "Qtd", "VPC", "Total", "Origem"]
        colw = [22, 20, 16, 22, 12, 16, 22, 30]

        rows = []
        for _, r in dfm2.iterrows():
            qtd = float(r.get("qtd_float", 0))
            qtd_str = str(int(qtd)) if qtd >= 1 else str(round(qtd, 3)).replace(".", ",")
            rows.append([
                str(r.get("data_iso", "")),
                str(r.get("data_br", "")),
                str(r.get("ticker_norm", "")),
                str(r.get("tipo_norm", ""))[:12],
                qtd_str,
                _fmt_brl(r.get("vpc_float", 0.0)),
                _fmt_brl(r.get("valor_float", 0.0)),
                str(r.get("origem", ""))[:18],
            ])
            if len(rows) >= 25:
                _add_table(pdf, headers, rows, colw)
                rows = []
                if pdf.get_y() > 250:
                    pdf.add_page()

        if rows:
            _add_table(pdf, headers, rows, colw)
    else:
        pdf.set_font("Arial", "", 9)
        pdf.cell(0, 6, _pdf_text("Nenhum lançamento no mês."), 0, 1, "L")

    _add_section_title(pdf, "Pendentes detalhados (ANUNCIADOS) - mês")
    if pend is not None and not pend.empty:
        headers = ["Ativo", "Pagto", "COM", "VPC", "Qtd ref", "Estimado"]
        colw = [18, 22, 22, 16, 18, 30]
        rows = []
        for _, r in pend.iterrows():
            dcom = pd.to_datetime(r.get("data_com", None), dayfirst=True, errors="coerce")
            qtd = float(r.get("qtd_float", 0))
            qtd_str = str(int(qtd)) if qtd >= 1 else str(round(qtd, 3)).replace(".", ",")
            rows.append([
                str(r.get("ticker_norm", "")),
                str(r.get("data_pag_br", "")),
                dcom.strftime("%d/%m/%Y") if not pd.isna(dcom) else "",
                _fmt_brl(r.get("vpc_float", 0.0)),
                qtd_str,
                f"R$ {_fmt_brl(r.get('valor_estimado_float', 0.0))}",
            ])
            if len(rows) >= 25:
                _add_table(pdf, headers, rows, colw)
                rows = []
                if pdf.get_y() > 250:
                    pdf.add_page()
        if rows:
            _add_table(pdf, headers, rows, colw)
    else:
        pdf.set_font("Arial", "", 9)
        pdf.cell(0, 6, _pdf_text("Sem pendentes no mês (ou base anunciada indisponível)."), 0, 1, "L")

    pdf.ln(1)
    pdf.set_font("Arial", "I", 8)
    ts = datetime.now().strftime("%d/%m/%Y %H:%M")
    start_12m = (pd.Timestamp(data_ref).replace(day=1) - pd.DateOffset(months=11)).date().strftime("%d/%m/%Y")
    pdf.cell(0, 5, _pdf_text(f"Gerado em: {ts} | Janela 12m desde {start_12m}"), 0, 1, "L")

    b = pdf.output(dest="S")
    if isinstance(b, str):
        b = b.encode("latin-1", errors="replace")
    buf = io.BytesIO(b)
    buf.name = f"Proventos_Auditoria_{data_ref.strftime('%Y-%m')}.pdf"
    buf.seek(0)
    return buf


# =========================================================
# Telegram send + Orquestrador (motor único)
# =========================================================
def telegram_send_pdf(
    bot_token: str,
    chat_id: str,
    pdf_buf: io.BytesIO,
    caption: str,
    timeout: int = 45
) -> Tuple[bool, Optional[str]]:
    try:
        if not bot_token or not chat_id:
            return False, "BOT_TOKEN/CHAT_ID vazio."
        if pdf_buf is None:
            return False, "PDF buffer None."

        try:
            pdf_buf.seek(0)
        except Exception:
            pass

        filename = getattr(pdf_buf, "name", None) or "relatorio.pdf"
        url = f"https://api.telegram.org/bot{bot_token}/sendDocument"

        r = requests.post(
            url,
            data={"chat_id": chat_id, "caption": caption},
            files={"document": (filename, pdf_buf, "application/pdf")},
            timeout=timeout,
        )

        ok = False
        payload = None
        try:
            payload = r.json()
            ok = bool(payload.get("ok", False))
        except Exception:
            payload = {"raw_text": r.text}
            ok = False

        if r.status_code != 200 or not ok:
            return False, f"Telegram falhou ({r.status_code}): {payload}"

        return True, None

    except Exception as e:
        return False, str(e)


def gerar_e_enviar_pdfs(
    bot_token: str,
    chat_id: str,
    data_ref: Optional[date],
    df_proventos: pd.DataFrame,
    df_ativos: pd.DataFrame,
    df_anunciados: Optional[pd.DataFrame] = None,
) -> PDFSendResult:
    if data_ref is None:
        data_ref = date.today()

    if not HAS_FPDF:
        return PDFSendResult(False, False, err_exec="FPDF não instalado.", err_aud="FPDF não instalado.")

    dfp = _norm_cols(df_proventos) if df_proventos is not None else pd.DataFrame()
    dfa = _norm_cols(df_ativos) if df_ativos is not None else pd.DataFrame()
    dfn = _norm_cols(df_anunciados) if (df_anunciados is not None and not pd.DataFrame(df_anunciados).empty) else pd.DataFrame()

    try:
        pdf_exec = build_pdf_executivo(dfp, dfa, data_ref, dfn)
    except Exception as e:
        return PDFSendResult(False, False, err_exec=f"Falha gerar executivo: {e}", err_aud="Pulou auditoria por falha no executivo.")

    try:
        pdf_aud = build_pdf_auditoria(dfp, dfa, data_ref, dfn)
    except Exception as e:
        ok_exec, err_exec = telegram_send_pdf(bot_token, chat_id, pdf_exec, "📊 PDF Executivo - Proventos (Mês + 12m + Pendentes)")
        return PDFSendResult(ok_exec, False, err_exec=err_exec, err_aud=f"Falha gerar auditoria: {e}")

    ok_exec, err_exec = telegram_send_pdf(bot_token, chat_id, pdf_exec, "📊 PDF Executivo - Proventos (Mês + 12m + Pendentes)")
    ok_aud, err_aud = telegram_send_pdf(bot_token, chat_id, pdf_aud, "🧾 PDF Auditoria - Detalhado (linhas, filtros e totais)")
    return PDFSendResult(ok_exec, ok_aud, err_exec=err_exec, err_aud=err_aud)
