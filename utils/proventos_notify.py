# utils/proventos_notify.py
# -*- coding: utf-8 -*-

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional, Tuple
import requests

DEFAULT_LOGO = "https://cdn-icons-png.flaticon.com/512/2454/2454282.png"


def _fmt_money_br(v: Any) -> str:
    try:
        x = float(v or 0.0)
        s = f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"R$ {s}"
    except Exception:
        return "R$ 0,00"


def _fmt_date_br(iso: Any) -> str:
    try:
        if not iso:
            return "-"
        s = str(iso)[:10]
        return datetime.strptime(s, "%Y-%m-%d").strftime("%d/%m/%Y")
    except Exception:
        return str(iso or "-")


def _caption(
    ticker: str,
    evento: Dict[str, Any],
    meta: Dict[str, Any],
    posicao: Optional[Dict[str, Any]],
) -> str:
    tipo = str(evento.get("tipo_pagamento") or "PROVENTO").upper()
    valor = evento.get("valor_por_cota")

    msg = (
        f"🧾📌 <b>Provento anunciado</b>\n"
        f"<b>{ticker.upper()}</b> — {tipo}\n\n"
        f"📅 <b>Data com:</b> {_fmt_date_br(evento.get('data_com'))}\n"
        f"💰 <b>Pagamento:</b> {_fmt_date_br(evento.get('data_pagamento'))}\n"
        f"🧾 <b>Valor por cota:</b> {_fmt_money_br(valor)}\n"
    )

    if posicao:
        try:
            qtd = float(posicao.get("qtd") or 0)
        except Exception:
            qtd = 0.0

        if qtd > 0:
            credito = posicao.get("credito_estimado")
            if credito is None:
                try:
                    credito = qtd * float(valor or 0.0)
                except Exception:
                    credito = 0.0

            msg += (
                f"\n📦 <b>Impacto na sua posição</b>\n"
                f"• Quantidade: {int(qtd)} cotas\n"
                f"• Crédito estimado: <b>{_fmt_money_br(credito)}</b>\n"
            )

    # Contexto (opcional)
    tipo_ativo = (meta.get("tipo_ativo") or "").strip()
    classif = (meta.get("classificacao") or "").strip()
    acao = (meta.get("acao_sugerida") or "").strip()

    if tipo_ativo or classif or acao:
        msg += "\n📊 <b>Contexto rápido</b>\n"
        if tipo_ativo:
            msg += f"• Tipo: {tipo_ativo}\n"
        if classif:
            msg += f"• Classificação: {classif}\n"
        if acao:
            msg += f"• Ação sugerida: {acao}\n"

    return msg.strip()


def notify_provento(
    token: str,
    chat_id: str,
    ticker: str,
    evento: Dict[str, Any],
    meta: Optional[Dict[str, Any]] = None,
    posicao: Optional[Dict[str, Any]] = None,
    logo_url: Optional[str] = None,
    timeout_s: int = 15,
) -> Tuple[bool, str, int, str]:
    """
    Motor único:
    - tenta sendPhoto (logo + caption)
    - fallback para sendMessage (texto)
    Retorna: (ok, metodo, status_code, erro_ou_vazio)
    metodo: 'photo' | 'text_fallback' | 'no_token' | 'bad_input'
    """
    token = (token or "").strip()
    chat_id = (chat_id or "").strip()
    if not token or not chat_id:
        return False, "no_token", 0, "missing token/chat_id"

    meta = meta or {}
    ticker = (ticker or "").strip().upper()
    if not ticker:
        return False, "bad_input", 0, "missing ticker"

    caption = _caption(ticker, evento, meta, posicao)
    photo = (logo_url or meta.get("logo_url") or DEFAULT_LOGO).strip()

    # 1) sendPhoto
    photo_err = ""
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendPhoto",
            data={
                "chat_id": chat_id,
                "photo": photo,
                "caption": caption,
                "parse_mode": "HTML",
            },
            timeout=timeout_s,
        )
        if r.ok:
            return True, "photo", r.status_code, ""
        photo_err = (r.text or "")[:500]
    except Exception as e:
        photo_err = repr(e)

    # 2) fallback sendMessage
    try:
        r2 = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": caption,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=timeout_s,
        )
        if r2.ok:
            return True, "text_fallback", r2.status_code, ""
        return False, "text_fallback", r2.status_code, (r2.text or "")[:500]
    except Exception as e2:
        return False, "text_fallback", 0, f"photo_err={photo_err} | text_err={repr(e2)}"
