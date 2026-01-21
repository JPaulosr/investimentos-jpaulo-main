# alerts/proventos_diarios.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import List, Dict, Any, Optional

import requests

try:
    import gspread
    from google.oauth2.service_account import Credentials
except Exception:
    gspread = None
    Credentials = None


# =========================
# Config / Utils
# =========================

def _env(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or "").strip()

def _env_int(name: str, default: int) -> int:
    v = _env(name, "")
    try:
        return int(v)
    except Exception:
        return default

def _to_date(x: Any) -> Optional[date]:
    if x is None:
        return None
    if isinstance(x, date) and not isinstance(x, datetime):
        return x
    s = str(x).strip()
    if not s:
        return None

    # tenta formatos comuns
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass

    # tenta datetime ISO
    try:
        return datetime.fromisoformat(s).date()
    except Exception:
        return None

def _br_money(v: Any) -> str:
    try:
        if v is None:
            return "R$ 0,00"
        s = str(v).strip()
        if not s:
            return "R$ 0,00"
        # aceita "0,4716" ou "0.4716"
        s = s.replace("R$", "").strip().replace(".", "").replace(",", ".") if ("," in s and s.count(",") == 1 and s.count(".") >= 1) else s
        val = float(s.replace(",", "."))
        return "R$ " + f"{val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return f"R$ {str(v)}"


# =========================
# Telegram
# =========================

def send_telegram(text: str) -> None:
    token = _env("TELEGRAM_BOT_TOKEN")
    chat_id = _env("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        raise RuntimeError("Faltou TELEGRAM_BOT_TOKEN ou TELEGRAM_CHAT_ID nos Secrets do GitHub.")

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    r = requests.post(url, json=payload, timeout=25)
    r.raise_for_status()


# =========================
# Google Sheets
# =========================

@dataclass
class ProventoAnunciado:
    ticker: str
    tipo_pagamento: str
    data_com: Optional[date]
    data_pagamento: Optional[date]
    valor_por_cota: Any
    fonte_url: str

def _gsheets_client():
    """
    Usa o Secret GCP_SERVICE_ACCOUNT_JSON (JSON inteiro do service account).
    """
    if gspread is None or Credentials is None:
        raise RuntimeError("Dependências do Google não instaladas. Confira requirements.txt (gspread/google-auth).")

    sa_json = _env("GCP_SERVICE_ACCOUNT_JSON")
    if not sa_json:
        raise RuntimeError("Faltou o secret GCP_SERVICE_ACCOUNT_JSON no GitHub.")

    info = json.loads(sa_json)
    
    # --- CORREÇÃO DE ERRO DE PEM / NEWLINE ---
    # Corrige a chave privada caso o GitHub tenha escapado as quebras de linha (\n virou \\n)
    if "private_key" in info:
        info["private_key"] = info["private_key"].replace("\\n", "\n")
    # -----------------------------------------

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def load_proventos_anunciados(sheet_id: str) -> List[ProventoAnunciado]:
    """
    Lê a aba 'proventos_anunciados' do Sheets (contrato).
    Espera headers:
    ticker, tipo_pagamento, data_com, data_pagamento, valor_por_cota, fonte_url
    (outros campos podem existir; a gente ignora)
    """
    gc = _gsheets_client()
    sh = gc.open_by_key(sheet_id)

    try:
        ws = sh.worksheet("proventos_anunciados")
    except Exception:
        # Se não existir, não quebra o robô — manda aviso
        return []

    rows = ws.get_all_records()  # usa primeira linha como header
    out: List[ProventoAnunciado] = []
    for r in rows:
        ticker = str(r.get("ticker", "")).strip().upper()
        if not ticker:
            continue
        out.append(
            ProventoAnunciado(
                ticker=ticker,
                tipo_pagamento=str(r.get("tipo_pagamento", "")).strip(),
                data_com=_to_date(r.get("data_com")),
                data_pagamento=_to_date(r.get("data_pagamento")),
                valor_por_cota=r.get("valor_por_cota"),
                fonte_url=str(r.get("fonte_url", "")).strip(),
            )
        )
    return out


# =========================
# Alerta de proventos futuros
# =========================

def build_future_alert(proventos: List[ProventoAnunciado], lookahead_days: int, tz_offset_hours: int) -> str:
    """
    Filtra pagamentos futuros até lookahead_days.
    Agrupa por data_pagamento e formata mensagem.
    """
    now_utc = datetime.utcnow()
    now_local = now_utc + timedelta(hours=tz_offset_hours)
    today = now_local.date()
    limit = today + timedelta(days=lookahead_days)

    futuros = [
        p for p in proventos
        if p.data_pagamento and (today <= p.data_pagamento <= limit)
    ]
    futuros.sort(key=lambda x: (x.data_pagamento or date.max, x.ticker))

    if not futuros:
        return (
            f"📌 <b>Proventos Futuros</b>\n"
            f"Janela: {today.strftime('%d/%m/%Y')} → {limit.strftime('%d/%m/%Y')}\n\n"
            f"✅ Nenhum provento futuro encontrado."
        )

    # agrupa por data
    by_date: Dict[date, List[ProventoAnunciado]] = {}
    for p in futuros:
        by_date.setdefault(p.data_pagamento, []).append(p)

    lines: List[str] = []
    lines.append("📌 <b>Proventos Futuros</b>")
    lines.append(f"Janela: {today.strftime('%d/%m/%Y')} → {limit.strftime('%d/%m/%Y')}")
    lines.append("")

    for d in sorted(by_date.keys()):
        lines.append(f"🗓️ <b>{d.strftime('%d/%m/%Y')}</b>")
        for p in by_date[d]:
            lines.append(
                f"• <b>{p.ticker}</b> ({p.tipo_pagamento or '—'}) — {_br_money(p.valor_por_cota)}"
                + (f" | COM: {p.data_com.strftime('%d/%m/%Y')}" if p.data_com else "")
            )
        lines.append("")

    return "\n".join(lines).strip()


def main():
    # config
    lookahead = _env_int("LOOKAHEAD_DAYS", 30)
    tz_offset = _env_int("TZ_OFFSET_HOURS", -3)

    # tenta ler do Sheets; se não tiver, manda "heartbeat" e falha explícita só se secrets do google estiverem setados?
    sheet_id = _env("SHEET_ID_NOVO")

    # Se você ainda não configurou Sheets, o robô ainda pode rodar e avisar isso no Telegram.
    if not sheet_id:
        msg = (
            "⚠️ <b>Proventos Diários</b>\n\n"
            "O workflow rodou, Telegram está OK.\n"
            "Mas não encontrei o secret <b>SHEET_ID_NOVO</b>.\n"
            "Configure para eu ler <b>proventos_anunciados</b>."
        )
        send_telegram(msg)
        return

    # carrega proventos anunciados
    try:
        prov = load_proventos_anunciados(sheet_id)
    except Exception as e:
        # se deu erro no google, manda erro útil no telegram
        msg = (
            "❌ <b>Proventos Diários</b>\n\n"
            "Falha ao acessar Google Sheets.\n"
            f"<b>Erro:</b> {str(e)}"
        )
        send_telegram(msg)
        raise

    # monta e envia alerta
    text = build_future_alert(prov, lookahead_days=lookahead, tz_offset_hours=tz_offset)
    send_telegram(text)


if __name__ == "__main__":
    main()