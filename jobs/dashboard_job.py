# jobs/dashboard_job.py
# -*- coding: utf-8 -*-
"""
Job diário: calcula o snapshot consolidado do dashboard.
Lê renda variável (carteira_snapshot) + renda fixa (renda_fixa)
e salva em 'dashboard_snapshot' no Sheets.

Executado pelo GitHub Actions junto com proventos_job.
Sem dependência de Streamlit.

Uso local:
    python jobs/dashboard_job.py
"""

from __future__ import annotations

import os, sys, json, math, time, requests
import pandas as pd
from datetime import date, datetime, timedelta
from typing import Optional

import gspread
from google.oauth2.service_account import Credentials

# ── Configuração ───────────────────────────────────────────────────────────────
SHEET_ID = (os.getenv("SHEET_ID_NOVO") or os.getenv("SHEET_ID") or "").strip()

ABA_RF          = "renda_fixa"
ABA_RV          = "carteira_snapshot"   # já gerado pelo proventos_job
ABA_PROVENTOS   = "proventos"
ABA_SNAPSHOT    = "dashboard_snapshot"
ABA_BCB_CACHE   = "bcb_cache"          # gerado pelo bcb_job

_BCB_CDI   = 12
_BCB_SELIC = 11
_BCB_IPCA  = 433
_BCB_URL   = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{serie}/dados"

_IR_TABELA = [(180, 0.225), (360, 0.200), (720, 0.175), (99999, 0.150)]
_IOF_TABELA = [
    0.96, 0.93, 0.90, 0.86, 0.83, 0.80, 0.76, 0.73, 0.70,
    0.66, 0.63, 0.60, 0.56, 0.53, 0.50, 0.46, 0.43, 0.40,
    0.36, 0.33, 0.30, 0.26, 0.23, 0.20, 0.16, 0.13, 0.10,
    0.06, 0.03,
]

HEADER_SNAPSHOT = [
    # Renda Fixa
    "rf_total_aplicado", "rf_saldo_bruto", "rf_saldo_liquido",
    "rf_rendimento_liquido", "rf_rendimento_pct", "rf_ir_estimado", "rf_qtd_titulos",
    # Renda Variável
    "rv_total_investido", "rv_valor_atual", "rv_lucro_total",
    "rv_lucro_pct", "rv_dividendos_12m",
    # Consolidado
    "total_investido", "total_atual", "total_lucro", "total_lucro_pct",
    # Mensal (últimos 12 meses) — JSON string
    "mensal_rf_juros",       # JSON: {"2025-03": 150.20, "2025-04": 163.10, ...}
    "mensal_rv_dividendos",  # JSON: {"2025-03": 320.00, "2025-04": 280.50, ...}
    "mensal_rf_ir",          # JSON: {"2025-03": 30.00, ...}
    # Meta
    "atualizado_em",
]


# ── Conexão Sheets ─────────────────────────────────────────────────────────────
def _gc() -> gspread.Client:
    sa_raw = (
        os.getenv("GCP_SERVICE_ACCOUNT")
        or os.getenv("GCP_SERVICE_ACCOUNT_JSON")
        or os.getenv("GOOGLE_SERVICE_ACCOUNT")
    )
    if not sa_raw:
        raise RuntimeError("GCP_SERVICE_ACCOUNT não encontrado no ambiente.")
    sa_info = json.loads(sa_raw) if isinstance(sa_raw, str) else sa_raw
    creds = Credentials.from_service_account_info(
        sa_info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)


def _ws(sh: gspread.Spreadsheet, nome: str, rows: int = 1000, cols: int = 30) -> gspread.Worksheet:
    titulos = [w.title for w in sh.worksheets()]
    if nome not in titulos:
        return sh.add_worksheet(title=nome, rows=rows, cols=cols)
    return sh.worksheet(nome)


def _read(sh: gspread.Spreadsheet, nome: str) -> pd.DataFrame:
    try:
        ws = sh.worksheet(nome)
        vals = ws.get_all_values()
        if not vals or len(vals) < 2:
            return pd.DataFrame()
        return pd.DataFrame(vals[1:], columns=[str(h).strip() for h in vals[0]])
    except Exception as e:
        print(f"  ⚠ Não consegui ler aba '{nome}': {e}")
        return pd.DataFrame()


def _to_float(v) -> float:
    if v is None or v == "":
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace("R$", "").replace(" ", "")
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def _parse_date(s) -> Optional[date]:
    if not s:
        return None
    s = str(s).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except ValueError:
            pass
    return None


# ── BCB: séries de taxa ────────────────────────────────────────────────────────
def _carregar_bcb_cache(sh: gspread.Spreadsheet) -> pd.DataFrame:
    """Lê bcb_cache do Sheets (gerado pelo bcb_job)."""
    df = _read(sh, ABA_BCB_CACHE)
    if df.empty or "serie" not in df.columns:
        return pd.DataFrame()
    df["serie"] = pd.to_numeric(df["serie"], errors="coerce")
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce").fillna(0.0)
    df["data"]  = pd.to_datetime(df["data"], errors="coerce")
    return df.dropna(subset=["serie", "data", "valor"]).sort_values("data").reset_index(drop=True)


def _serie(df_bcb: pd.DataFrame, serie: int, data_ini: date, data_fim: date) -> pd.DataFrame:
    if df_bcb.empty:
        return pd.DataFrame(columns=["data", "valor"])
    mask = (
        (df_bcb["serie"] == serie) &
        (df_bcb["data"] >= pd.Timestamp(data_ini)) &
        (df_bcb["data"] <= pd.Timestamp(data_fim))
    )
    return df_bcb.loc[mask, ["data", "valor"]].reset_index(drop=True)


def _buscar_bcb_api(serie: int, data_ini: date, data_fim: date) -> pd.DataFrame:
    """Fallback: API do BCB direto."""
    fmt = lambda d: d.strftime("%d/%m/%Y")
    try:
        r = requests.get(
            _BCB_URL.format(serie=serie),
            params={"formato": "json", "dataInicial": fmt(data_ini), "dataFinal": fmt(data_fim)},
            timeout=30,
        )
        r.raise_for_status()
        df = pd.DataFrame(r.json())
        df["data"]  = pd.to_datetime(df["data"], dayfirst=True)
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce").fillna(0.0)
        return df.sort_values("data").reset_index(drop=True)
    except Exception as e:
        print(f"  ⚠ BCB API série {serie}: {e}")
        return pd.DataFrame(columns=["data", "valor"])


def _get_serie(df_bcb: pd.DataFrame, serie: int, data_ini: date, data_fim: date) -> pd.DataFrame:
    df = _serie(df_bcb, serie, data_ini, data_fim)
    if not df.empty:
        return df
    print(f"  ↩ Fallback API BCB para série {serie}...")
    return _buscar_bcb_api(serie, data_ini, data_fim)


# ── Cálculo de rendimento RF ───────────────────────────────────────────────────
def _fator(df_bcb: pd.DataFrame, serie: int, data_ini: date, data_fim: date, pct: float) -> float:
    df = _get_serie(df_bcb, serie, data_ini, data_fim)
    if df.empty:
        return 1.0
    fator = 1.0
    for v in df["valor"]:
        fator *= (1 + (v / 100) * (pct / 100))
    return fator


def _fator_pre(data_ini: date, data_fim: date, taxa: float) -> float:
    dias = (data_fim - data_ini).days
    return (1 + taxa / 100) ** (dias / 365)


def _fator_ipca(df_bcb: pd.DataFrame, data_ini: date, data_fim: date, spread: float) -> float:
    hoje = date.today()
    limite = date(hoje.year, hoje.month, 1) - timedelta(days=1)
    limite = limite.replace(day=1)
    data_fim_ipca = min(data_fim.replace(day=1), limite)
    data_ini_ipca = data_ini.replace(day=1)
    fator_ipca = 1.0
    if data_fim_ipca > data_ini_ipca:
        df = _get_serie(df_bcb, _BCB_IPCA, data_ini_ipca, data_fim_ipca)
        for v in df["valor"]:
            fator_ipca *= (1 + v / 100)
    dias = (data_fim - data_ini).days
    return fator_ipca * (1 + spread / 100) ** (dias / 365)


def _calcular_titulo(t: dict, df_bcb: pd.DataFrame, ref: date) -> Optional[dict]:
    try:
        data_ap   = _parse_date(t.get("data_aplicacao"))
        data_vc   = _parse_date(t.get("data_vencimento"))
        valor_ap  = _to_float(t.get("valor_aplicado", 0))
        taxa      = _to_float(t.get("taxa", 0))
        indexador = str(t.get("indexador", "Pré")).strip().upper()
        isento    = str(t.get("isento_ir", "")).upper() in ("SIM", "TRUE", "1", "S")
        obs       = str(t.get("observacao", "")).strip()

        if obs == "__DELETED__" or not data_ap or valor_ap <= 0:
            return None

        data_fim = min(ref, data_vc) if data_vc else ref
        if data_fim <= data_ap:
            data_fim = data_ap + timedelta(days=1)

        if "CDI" in indexador:
            fator = _fator(df_bcb, _BCB_CDI, data_ap, data_fim, taxa)
        elif "SELIC" in indexador:
            fator = _fator(df_bcb, _BCB_SELIC, data_ap, data_fim, taxa)
        elif "IPCA" in indexador:
            fator = _fator_ipca(df_bcb, data_ap, data_fim, taxa)
        else:
            fator = _fator_pre(data_ap, data_fim, taxa)

        bruto = valor_ap * fator
        rend  = bruto - valor_ap
        dias  = (data_fim - data_ap).days

        iof = 0.0
        if dias < 30:
            iof = rend * _IOF_TABELA[max(0, min(dias - 1, 28))]

        ir = 0.0
        if not isento:
            for lim, aliq in _IR_TABELA:
                if dias <= lim:
                    ir = max(0, (rend - iof) * aliq)
                    break

        liquido = bruto - iof - ir
        return {
            "valor_aplicado": valor_ap,
            "valor_bruto":    bruto,
            "valor_liquido":  liquido,
            "ir_valor":       ir,
            "rendimento_liq": liquido - valor_ap,
            "vencido":        data_vc is not None and data_vc < ref,
        }
    except Exception as e:
        print(f"  ⚠ Título RF erro: {e}")
        return None


# ── Rendimento mensal RF (últimos 12 meses) ───────────────────────────────────
def _rendimento_mensal_rf(titulos: list, df_bcb: pd.DataFrame) -> tuple[dict, dict]:
    """
    Para cada mês dos últimos 12, calcula quanto a RF rendeu (líquido) e IR.
    Retorna (mensal_juros, mensal_ir) como dicts {YYYY-MM: valor}.
    """
    hoje  = date.today()
    meses = []
    for i in range(12):
        m = hoje.month - i
        y = hoje.year
        while m <= 0:
            m += 12
            y -= 1
        meses.append(date(y, m, 1))
    meses = sorted(meses)

    mensal_juros = {}
    mensal_ir    = {}

    for mes_ini in meses:
        # último dia do mês
        if mes_ini.month == 12:
            mes_fim = date(mes_ini.year + 1, 1, 1) - timedelta(days=1)
        else:
            mes_fim = date(mes_ini.year, mes_ini.month + 1, 1) - timedelta(days=1)

        chave = mes_ini.strftime("%Y-%m")
        juros_mes = 0.0
        ir_mes    = 0.0

        for t in titulos:
            data_ap = _parse_date(t.get("data_aplicacao"))
            data_vc = _parse_date(t.get("data_vencimento"))
            obs     = str(t.get("observacao", "")).strip()
            if obs == "__DELETED__" or not data_ap:
                continue
            # título ativo nesse mês?
            ativo_no_mes = data_ap <= mes_fim and (data_vc is None or data_vc >= mes_ini)
            if not ativo_no_mes:
                continue

            ini = _calcular_titulo(t, df_bcb, mes_ini)
            fim = _calcular_titulo(t, df_bcb, mes_fim)
            if ini and fim:
                delta_liq = fim["valor_liquido"] - ini["valor_liquido"]
                delta_ir  = fim["ir_valor"] - ini["ir_valor"]
                juros_mes += max(0.0, delta_liq)
                ir_mes    += max(0.0, delta_ir)

        mensal_juros[chave] = round(juros_mes, 2)
        mensal_ir[chave]    = round(ir_mes, 2)

    return mensal_juros, mensal_ir


# ── Dividendos mensais RV (últimos 12 meses) ──────────────────────────────────
def _dividendos_mensais(df_prov: pd.DataFrame) -> dict:
    if df_prov.empty:
        return {}
    df = df_prov.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]

    col_data = next((c for c in df.columns if "data" in c and "pag" in c), None) or \
               next((c for c in df.columns if c == "data"), None)
    col_val  = next((c for c in df.columns if "valor" in c and "cota" not in c), None) or \
               next((c for c in df.columns if "total" in c), None)

    if not col_data or not col_val:
        return {}

    df["_data"]  = df[col_data].apply(_parse_date)
    df["_valor"] = df[col_val].apply(_to_float)
    df = df.dropna(subset=["_data"])

    hoje    = date.today()
    doze_m  = hoje - timedelta(days=365)
    df      = df[df["_data"] >= doze_m]

    mensal = {}
    for _, row in df.iterrows():
        d = row["_data"]
        k = d.strftime("%Y-%m")
        mensal[k] = round(mensal.get(k, 0.0) + row["_valor"], 2)
    return mensal


# ── MAIN ───────────────────────────────────────────────────────────────────────


# =============================================================================
# 🔔 ALERTA DE VENCIMENTO — verifica títulos vencendo hoje ou em breve
# =============================================================================

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Avisa com quantos dias de antecedência (além do dia do vencimento)
_AVISOS_DIAS = [0, 1, 3, 7]   # hoje, amanhã, 3 dias, 7 dias

def _send_telegram_rf(msg: str) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("  ⚠ Telegram: TOKEN ou CHAT_ID não definidos")
        return
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=15,
        )
        if r.ok:
            print("  📨 Telegram enviado com sucesso")
        else:
            print(f"  ⚠ Telegram erro {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"  ⚠ Telegram exception: {e}")


def _card_vencimento(t: dict, calc: dict, dias_para_vencer: int) -> str:
    nome      = str(t.get("nome", "Título")).strip()
    inst      = str(t.get("instituicao", "—")).strip()
    tipo      = str(t.get("tipo", "—")).strip()
    indexador = str(t.get("indexador", "—")).strip()
    taxa      = _to_float(t.get("taxa", 0))
    def _fmt_br(s) -> str:
        """Converte qualquer formato de data para DD/MM/YYYY."""
        d = _parse_date(s)
        return d.strftime("%d/%m/%Y") if d else "—"
    data_vc = _fmt_br(t.get("data_vencimento"))
    data_ap = _fmt_br(t.get("data_aplicacao"))

    v_ap   = calc.get("valor_aplicado", 0.0)
    v_brut = calc.get("valor_bruto", 0.0)
    v_liq  = calc.get("valor_liquido", 0.0)
    v_ir   = calc.get("ir_valor", 0.0)
    r_pct  = calc.get("rendimento_liq", 0.0)
    r_liq  = v_liq - v_ap

    # Formatação da taxa
    idx_u = indexador.upper()
    if "CDI" in idx_u:
        taxa_str = f"{taxa:.0f}% CDI"
    elif "SELIC" in idx_u:
        taxa_str = f"{taxa:.0f}% Selic"
    elif "IPCA" in idx_u:
        taxa_str = f"IPCA+{taxa:.2f}%"
    else:
        taxa_str = f"{taxa:.2f}% a.a."

    def brl(v): return f"R$ {v:,.2f}".replace(",","X").replace(".",",").replace("X",".")
    def pct(v): return f"{v:+.2f}%"

    # Emoji e texto do prazo
    if dias_para_vencer == 0:
        prazo_emoji = "🔴"
        prazo_txt   = "VENCE HOJE"
    elif dias_para_vencer == 1:
        prazo_emoji = "🟠"
        prazo_txt   = "VENCE AMANHÃ"
    elif dias_para_vencer <= 3:
        prazo_emoji = "🟡"
        prazo_txt   = f"VENCE EM {dias_para_vencer} DIAS"
    else:
        prazo_emoji = "📅"
        prazo_txt   = f"VENCE EM {dias_para_vencer} DIAS"

    card = (
        f"{prazo_emoji} <b>VENCIMENTO RF — {prazo_txt}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"📄 <b>{nome}</b>\n"
        f"🏦 Instituição: {inst}\n"
        f"📂 Tipo: {tipo} · {taxa_str}\n"
        f"\n"
        f"📅 Aplicado em: {data_ap}\n"
        f"📅 Vencimento:  {data_vc}\n"
        f"\n"
        f"💰 Valor aplicado:  <b>{brl(v_ap)}</b>\n"
        f"📈 Saldo bruto:     <b>{brl(v_brut)}</b>\n"
        f"✅ Saldo líquido:   <b>{brl(v_liq)}</b>\n"
        f"🏛 IR estimado:     {brl(v_ir)}\n"
        f"📊 Rendimento líq.: {pct(r_liq / v_ap * 100) if v_ap else '—'} ({brl(r_liq)})\n"
        f"\n"
        f"📌 O valor retorna para a conta na {inst}."
    )
    return card


def verificar_vencimentos(titulos: list, df_bcb: pd.DataFrame) -> None:
    """
    Verifica títulos vencendo hoje ou nos próximos dias e envia card no Telegram.
    Roda dentro do dashboard_job — já tem acesso a titulos e df_bcb.
    """
    print("\n🔔 Verificando vencimentos...")
    hoje = date.today()
    enviados = 0

    for t in titulos:
        obs     = str(t.get("observacao", "")).strip()
        data_vc = _parse_date(t.get("data_vencimento"))

        if obs == "__DELETED__" or not data_vc:
            continue

        dias = (data_vc - hoje).days

        if dias not in _AVISOS_DIAS:
            continue

        # Calcula o saldo atual do título para exibir no card
        calc_raw = _calcular_titulo(t, df_bcb, hoje)
        if not calc_raw:
            continue

        card = _card_vencimento(t, calc_raw, dias)
        _send_telegram_rf(card)
        enviados += 1

        # Pequena pausa entre mensagens
        if enviados < len(_AVISOS_DIAS):
            time.sleep(1)

    if enviados == 0:
        print("  ✅ Nenhum título vencendo hoje ou nos próximos 7 dias.")
    else:
        print(f"  📨 {enviados} alerta(s) de vencimento enviado(s).")


def main() -> None:
    if not SHEET_ID:
        print("❌ SHEET_ID não encontrado.")
        sys.exit(1)

    print("🔌 Conectando ao Sheets...")
    gc = _gc()
    sh = gc.open_by_key(SHEET_ID)
    hoje = date.today()

    # ── 1. Carregar dados ──────────────────────────────────────────────────────
    print("📥 Lendo dados do Sheets...")
    df_rf   = _read(sh, ABA_RF)
    df_rv   = _read(sh, ABA_RV)
    df_prov = _read(sh, ABA_PROVENTOS)
    df_bcb  = _carregar_bcb_cache(sh)

    if df_bcb.empty:
        print("  ⚠ bcb_cache vazio — usando API do BCB direto (mais lento)")
    else:
        print(f"  ✅ bcb_cache: {len(df_bcb)} registros")

    # ── 2. Renda Fixa ──────────────────────────────────────────────────────────
    print("🏦 Calculando Renda Fixa...")
    titulos_rf = df_rf.to_dict("records") if not df_rf.empty else []
    rf_ap = rf_bruto = rf_liq = rf_ir = 0.0
    qtd_ativos = 0

    for t in titulos_rf:
        c = _calcular_titulo(t, df_bcb, hoje)
        if c and not c["vencido"]:
            rf_ap    += c["valor_aplicado"]
            rf_bruto += c["valor_bruto"]
            rf_liq   += c["valor_liquido"]
            rf_ir    += c["ir_valor"]
            qtd_ativos += 1

    rf_rend_liq = rf_liq - rf_ap
    rf_rend_pct = (rf_liq / rf_ap - 1) * 100 if rf_ap else 0.0

    print(f"  ✅ RF: {qtd_ativos} títulos | aplicado={rf_ap:.2f} | líquido={rf_liq:.2f}")

    # ── 3. Renda Variável (carteira_snapshot) ─────────────────────────────────
    print("📈 Calculando Renda Variável...")
    rv_investido = rv_atual = rv_div12m = 0.0

    if not df_rv.empty:
        df_rv.columns = [str(c).strip().lower() for c in df_rv.columns]

        # custo total
        col_pm  = next((c for c in df_rv.columns if "preco_medio" in c or "pm" in c), None)
        col_qtd = next((c for c in df_rv.columns if "quantidade" in c or "qtd" in c), None)
        col_cur = next((c for c in df_rv.columns if "preco_atual" in c or "cotacao" in c or "valor_atual" in c), None)
        col_div = next((c for c in df_rv.columns if "dividendo" in c or "proventos_12m" in c or "renda_12m" in c), None)

        for _, row in df_rv.iterrows():
            qtd = _to_float(row.get(col_qtd, 0)) if col_qtd else 0
            pm  = _to_float(row.get(col_pm, 0))  if col_pm  else 0
            cur = _to_float(row.get(col_cur, 0)) if col_cur else 0
            div = _to_float(row.get(col_div, 0)) if col_div else 0
            rv_investido += qtd * pm
            rv_atual     += qtd * cur if cur else qtd * pm
            rv_div12m    += div

    rv_lucro     = rv_atual - rv_investido
    rv_lucro_pct = (rv_atual / rv_investido - 1) * 100 if rv_investido else 0.0

    print(f"  ✅ RV: investido={rv_investido:.2f} | atual={rv_atual:.2f} | div12m={rv_div12m:.2f}")

    # ── 4. Consolidado ────────────────────────────────────────────────────────
    total_inv   = rf_ap + rv_investido
    total_atual = rf_liq + rv_atual
    total_lucro = total_atual - total_inv
    total_pct   = (total_atual / total_inv - 1) * 100 if total_inv else 0.0

    # ── 5. Séries mensais ─────────────────────────────────────────────────────
    print("📅 Calculando séries mensais (12 meses)...")
    mensal_juros, mensal_ir = _rendimento_mensal_rf(titulos_rf, df_bcb)
    mensal_div              = _dividendos_mensais(df_prov)

    # ── 6. Salvar snapshot ────────────────────────────────────────────────────
    print("💾 Salvando dashboard_snapshot...")
    agora = datetime.now().strftime("%Y-%m-%d %H:%M")
    linha = [
        round(rf_ap, 2), round(rf_bruto, 2), round(rf_liq, 2),
        round(rf_rend_liq, 2), round(rf_rend_pct, 4), round(rf_ir, 2), qtd_ativos,
        round(rv_investido, 2), round(rv_atual, 2), round(rv_lucro, 2),
        round(rv_lucro_pct, 4), round(rv_div12m, 2),
        round(total_inv, 2), round(total_atual, 2), round(total_lucro, 2), round(total_pct, 4),
        json.dumps(mensal_juros),
        json.dumps(mensal_div),
        json.dumps(mensal_ir),
        agora,
    ]

    ws = _ws(sh, ABA_SNAPSHOT, rows=10, cols=len(HEADER_SNAPSHOT))
    ws.clear()
    ws.update([HEADER_SNAPSHOT, linha], "A1", value_input_option="USER_ENTERED")

    print(f"\n✅ dashboard_snapshot salvo às {agora}")
    print(f"   RF  aplicado={rf_ap:,.2f} | líquido={rf_liq:,.2f} | IR={rf_ir:,.2f}")
    print(f"   RV  investido={rv_investido:,.2f} | atual={rv_atual:,.2f}")
    print(f"   TOT investido={total_inv:,.2f} | atual={total_atual:,.2f} | lucro={total_lucro:,.2f}")

    # ── 7. Alertas de vencimento RF ───────────────────────────────────────────
    verificar_vencimentos(titulos_rf, df_bcb)


if __name__ == "__main__":
    main()
