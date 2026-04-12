# jobs/bcb_job.py
# -*- coding: utf-8 -*-
"""
Job diário: busca séries CDI, Selic e IPCA do Banco Central
e salva na aba 'bcb_cache' do Google Sheets.

Executado pelo GitHub Actions todo dia às 6h (horário de Brasília).
Sem dependência de Streamlit — usa apenas gspread direto.

Uso local:
    python jobs/bcb_job.py
"""

from __future__ import annotations

import os
import json
import sys
import time
import requests
import pandas as pd
from datetime import date, datetime, timedelta

import gspread
from google.oauth2.service_account import Credentials

# ── Configuração ──────────────────────────────────────────────────────────────
_BCB_URL   = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{serie}/dados"
_SERIES    = {
    12:  "CDI",    # CDI diário (% ao dia)
    11:  "Selic",  # Selic diária (% ao dia)
    433: "IPCA",   # IPCA mensal (% ao mês)
}

ABA_CACHE  = "bcb_cache"
HEADER     = ["serie", "nome", "data", "valor", "atualizado_em"]

# Quantos dias de histórico manter (CDI e Selic são diários, IPCA mensal)
# 5 anos é mais que suficiente para qualquer título em carteira
_ANOS_HIST = 6


# ── Helpers ───────────────────────────────────────────────────────────────────
def _fmt(d: date) -> str:
    return d.strftime("%d/%m/%Y")


def _ini_hist() -> date:
    hoje = date.today()
    return date(hoje.year - _ANOS_HIST, hoje.month, 1)


def _buscar_serie(serie: int, data_ini: date, data_fim: date) -> pd.DataFrame:
    params = {
        "formato":      "json",
        "dataInicial":  _fmt(data_ini),
        "dataFinal":    _fmt(data_fim),
    }
    for tentativa in range(3):
        try:
            r = requests.get(
                _BCB_URL.format(serie=serie),
                params=params,
                timeout=60,
            )
            r.raise_for_status()
            df = pd.DataFrame(r.json())
            df["data"]  = pd.to_datetime(df["data"], dayfirst=True)
            df["valor"] = pd.to_numeric(df["valor"], errors="coerce").fillna(0.0)
            print(f"  ✅ Série {serie} ({_SERIES[serie]}): {len(df)} registros")
            return df.sort_values("data").reset_index(drop=True)
        except requests.exceptions.Timeout:
            print(f"  ⏱ Série {serie}: timeout (tentativa {tentativa+1}/3)")
            time.sleep(5 * (tentativa + 1))
        except Exception as e:
            print(f"  ❌ Série {serie}: {e}")
            return pd.DataFrame(columns=["data", "valor"])
    return pd.DataFrame(columns=["data", "valor"])


def _conectar_sheets(sheet_id: str) -> gspread.Spreadsheet:
    sa_raw = (
        os.environ.get("GCP_SERVICE_ACCOUNT_JSON")
        or os.environ.get("GCP_SERVICE_ACCOUNT")
        or os.environ.get("GOOGLE_SERVICE_ACCOUNT")
    )
    if not sa_raw:
        raise RuntimeError("Variável GCP_SERVICE_ACCOUNT_JSON não encontrada no ambiente.")

    sa_info = json.loads(sa_raw) if isinstance(sa_raw, str) else sa_raw
    creds   = Credentials.from_service_account_info(
        sa_info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    gc = gspread.authorize(creds)
    return gc.open_by_key(sheet_id)


def _garantir_aba(sh: gspread.Spreadsheet) -> gspread.Worksheet:
    titulos = [ws.title for ws in sh.worksheets()]
    if ABA_CACHE not in titulos:
        ws = sh.add_worksheet(title=ABA_CACHE, rows=100_000, cols=len(HEADER))
        ws.append_row(HEADER, value_input_option="USER_ENTERED")
        print(f"  📋 Aba '{ABA_CACHE}' criada.")
    else:
        ws = sh.worksheet(ABA_CACHE)
    return ws


def _salvar(ws: gspread.Worksheet, df_novo: pd.DataFrame) -> None:
    """
    Estratégia: limpa a aba e reescreve tudo.
    Simples, sem risco de duplicata, e o volume (≈6 anos CDI+Selic+IPCA)
    é de ~4.500 linhas — cabe fácil em uma chamada.
    """
    agora = datetime.now().strftime("%Y-%m-%d %H:%M")

    linhas = [HEADER]
    for _, row in df_novo.iterrows():
        linhas.append([
            int(row["serie"]),
            str(row["nome"]),
            row["data"].strftime("%Y-%m-%d"),   # ISO — sem ambiguidade de locale
            float(row["valor"]),
            agora,
        ])

    ws.clear()
    ws.update("A1", linhas, value_input_option="USER_ENTERED")
    print(f"  💾 {len(linhas)-1} linhas gravadas em '{ABA_CACHE}'.")


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    sheet_id = os.environ.get("SHEET_ID_NOVO") or os.environ.get("SHEET_ID")
    if not sheet_id:
        print("❌ SHEET_ID não encontrado. Defina SHEET_ID_NOVO ou SHEET_ID no ambiente.")
        sys.exit(1)

    data_ini = _ini_hist()
    data_fim = date.today()
    print(f"📅 Período: {_fmt(data_ini)} → {_fmt(data_fim)}")

    frames = []
    for serie, nome in _SERIES.items():
        print(f"🔄 Buscando série {serie} ({nome})...")
        df = _buscar_serie(serie, data_ini, data_fim)
        if not df.empty:
            df["serie"] = serie
            df["nome"]  = nome
            frames.append(df[["serie", "nome", "data", "valor"]])
        time.sleep(2)   # respeita rate limit do BCB

    if not frames:
        print("❌ Nenhuma série retornou dados. Abortando.")
        sys.exit(1)

    df_total = pd.concat(frames, ignore_index=True)
    print(f"\n📊 Total de registros a salvar: {len(df_total)}")

    print("🔌 Conectando ao Google Sheets...")
    sh = _conectar_sheets(sheet_id)
    ws = _garantir_aba(sh)

    print("💾 Salvando no Sheets...")
    _salvar(ws, df_total)

    print("\n✅ bcb_job concluído com sucesso!")


if __name__ == "__main__":
    main()
