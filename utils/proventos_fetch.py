# utils/proventos_fetch.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, date
from typing import Optional, Dict, Any, List

import requests

try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:
    BeautifulSoup = None


_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}

def _safe_get(url: str, timeout: int = 15) -> str:
    try:
        r = requests.get(url, headers=_HEADERS, timeout=timeout)
        r.raise_for_status()
        return r.text
    except Exception:
        return ""

def _clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def _html_to_text(html: str) -> str:
    if not html:
        return ""
    if BeautifulSoup is not None:
        soup = BeautifulSoup(html, "html.parser")
        return _clean_text(soup.get_text(" "))

    txt = re.sub(r"<script.*?>.*?</script>", " ", html, flags=re.I | re.S)
    txt = re.sub(r"<style.*?>.*?</style>", " ", txt, flags=re.I | re.S)
    txt = re.sub(r"<[^>]+>", " ", txt)
    return _clean_text(txt)

def _parse_date_iso(s: str) -> Optional[str]:
    if not s:
        return None
    s = s.strip()

    m = re.search(r"(\d{2})/(\d{2})/(\d{4})", s)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"

    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return m.group(0)

    return None

def _get_date_obj(iso_date: str):
    try:
        return datetime.strptime(iso_date, "%Y-%m-%d").date()
    except:
        return None

def _parse_money_br(s: str) -> Optional[float]:
    if not s:
        return None
    # Remove tudo que não é número, vírgula ou ponto
    clean = re.sub(r"[^\d,.]", "", s)
    if not clean:
        return None
    
    # Se houver os dois, remove o ponto (milhar) e troca vírgula por ponto
    if "," in clean and "." in clean:
        clean = clean.replace(".", "").replace(",", ".")
    # Se houver apenas vírgula, troca por ponto
    elif "," in clean:
        clean = clean.replace(",", ".")
        
    try:
        val = float(clean)
        return val if val > 0 else None
    except:
        return None

def _valor_parece_valido(v: float) -> bool:
    if v <= 0:
        return False
    # Removido limite superior de 1000 — dividendos de ações podem ser altos
    return True


@dataclass
class ProventoAnunciado:
    ticker: str
    status: str = "ANUNCIADO"
    tipo_pagamento: str = "RENDIMENTO"
    data_com: Optional[str] = None
    data_pagamento: Optional[str] = None
    valor_por_cota: Optional[float] = None
    fonte_url: str = ""
    fonte_nome: str = ""
    capturado_em: str = ""

    def to_row(self) -> Dict[str, Any]:
        # ✅ FIX 2: exporta bruto_total e liq_total para o job calcular ir_por_cota
        _bruto_total = getattr(self, "_val_bruto_total", None)
        _liq_total   = getattr(self, "_val_liq_total", None)

        # Calcula IR total se temos os dois valores do site
        _ir_total = None
        if _bruto_total is not None and _liq_total is not None:
            _ir_total = round(float(_bruto_total) - float(_liq_total), 6)
            if _ir_total < 0:
                _ir_total = None  # sanidade

        # ✅ Cálculo automático de IR por cota quando o site não retornou líquido
        # JCP: IR fixo 15% PF | RENDIMENTO_TRIB: IR fixo 15% | RENDIMENTO/DIVIDENDO: isento
        vpc = self.valor_por_cota  # valor capturado = bruto por cota
        ir_por_cota_calc = None
        vpc_liq_calc = None

        if vpc is not None and vpc > 0:
            tp = (self.tipo_pagamento or "").upper()
            if tp in ("JCP", "RENDIMENTO_TRIB"):
                ir_por_cota_calc  = round(float(vpc) * 0.15, 8)
                vpc_liq_calc      = round(float(vpc) * 0.85, 8)
            else:
                # RENDIMENTO de FII / DIVIDENDO: isento
                ir_por_cota_calc = 0.0
                vpc_liq_calc     = float(vpc)

        return {
            "ticker": self.ticker,
            "status": self.status,
            "tipo_pagamento": self.tipo_pagamento,
            "data_com": self.data_com or "",
            "data_pagamento": self.data_pagamento or "",
            "valor_por_cota": vpc_liq_calc if vpc_liq_calc is not None else ("" if vpc is None else float(vpc)),
            "valor_bruto_por_cota": ("" if vpc is None else float(vpc)),   # ✅ bruto = o que o site capturou
            "ir_por_cota": ir_por_cota_calc if ir_por_cota_calc is not None else "",  # ✅ calculado
            "valor_bruto_total": _bruto_total,
            "ir_total": _ir_total,
            "valor_liq_total": _liq_total,
            "fonte_url": self.fonte_url,
            "fonte_nome": self.fonte_nome,
            "capturado_em": self.capturado_em or datetime.now().strftime("%Y-%m-%d %H:%M"),
        }


def fetch_investidor10(ticker: str) -> List[ProventoAnunciado]:
    t = (ticker or "").upper().strip()
    resultados: List[ProventoAnunciado] = []

    urls_to_try = [
        f"https://investidor10.com.br/fiis/{t.lower()}/",
        f"https://investidor10.com.br/fiagros/{t.lower()}/",
        f"https://investidor10.com.br/acoes/{t.lower()}/",
    ]

    # ✅ FIX 3: usa timezone de Brasília para evitar descartar eventos do dia em UTC-3
    try:
        from zoneinfo import ZoneInfo
        hoje = datetime.now(tz=ZoneInfo("America/Sao_Paulo")).date()
    except Exception:
        hoje = datetime.now().date()

    for url in urls_to_try:
        is_fundo = ("/fiis/" in url) or ("/fiagros/" in url)

        html = _safe_get(url)
        text = _html_to_text(html)

        # ✅ FIX 1: regex ampliado para capturar "Rend. Trib." e variantes
        # ✅ FIX 2: captura valor_bruto_por_cota (val_div) e total_liquido (2 valores opcionais após data_pag)
        # ✅ FIX 2b: \s* em vez de \s+ entre campos — BeautifulSoup pode gerar separadores variados
        pattern = re.compile(
            r"(Dividendos?|Rendimentos?|Rend\.?\s*Trib\.?|JSCP|JCP)\s*"
            r"(\d{2}/\d{2}/\d{4})\s*"        # data_com
            r"(\d{2}/\d{2}/\d{4})\s*"        # data_pagamento
            r"(\d+[.,]\d+)"                    # valor_div (bruto por cota)
            r"(?:\s+(\d+[.,]\d+))?"           # valor_total (opcional)
            r"(?:\s+(\d+[.,]\d+))?",          # total_liquido (opcional)
            re.IGNORECASE,
        )
        matches = pattern.findall(text)
        if not matches:
            continue

        for match in matches:
            tipo_raw  = match[0]
            dc_raw    = match[1]
            dp_raw    = match[2]
            val_bruto_raw   = match[3]          # valor_div = bruto por cota
            val_total_raw   = match[4] if len(match) > 4 else ""   # valor_total (qtd * bruto)
            val_liq_raw     = match[5] if len(match) > 5 else ""   # total_liquido

            val_bruto = _parse_money_br(val_bruto_raw)
            if val_bruto is None:
                continue

            # total_liquido é o líquido total — divide por quantidade para obter líquido/cota
            # Mas quantidade não está disponível aqui — guardamos os totais e calculamos depois
            val_liq_total = _parse_money_br(val_liq_raw) if val_liq_raw else None
            val_bruto_total = _parse_money_br(val_total_raw) if val_total_raw else None

            # valor_por_cota = bruto por cota (val_div)
            # valor_bruto_por_cota = igual ao valor_por_cota (é o que o site chama de bruto)
            # ir_por_cota = calculado se tivermos bruto_total e liq_total e quantidade
            # Como não temos quantidade aqui, guardamos bruto e liq totais para o job calcular

            dc = _parse_date_iso(dc_raw)
            dp = _parse_date_iso(dp_raw)

            dp_obj = _get_date_obj(dp) if dp else None
            if not (dp_obj and dp_obj >= hoje):
                continue

            tipo_upper = (tipo_raw or "").upper().strip()
            if "JSCP" in tipo_upper or "JCP" in tipo_upper:
                tipo_final = "JCP"
            elif "REND" in tipo_upper and "TRIB" in tipo_upper:
                tipo_final = "RENDIMENTO_TRIB"   # usuário ajusta ao salvar
            elif "DIVIDENDO" in tipo_upper:
                tipo_final = "RENDIMENTO" if is_fundo else "DIVIDENDO"
            elif "RENDIMENTO" in tipo_upper:
                tipo_final = "RENDIMENTO"
            else:
                tipo_final = "RENDIMENTO"

            prov = ProventoAnunciado(
                ticker=t,
                fonte_url=url,
                fonte_nome="INVESTIDOR10",
                data_com=dc,
                data_pagamento=dp,
                valor_por_cota=float(val_bruto),
                tipo_pagamento=tipo_final,
            )
            # ✅ FIX 2: guarda bruto total e líquido total para o job calcular IR/cota
            prov._val_bruto_total = val_bruto_total   # type: ignore[attr-defined]
            prov._val_liq_total   = val_liq_total     # type: ignore[attr-defined]

            # ✅ FIX: dedup por data_com + data_pag + tipo + valor
            # Sem data_com na chave, dividendos múltiplos do mesmo período (mesmo valor, mesmo dia de pag) eram descartados
            if not any(
                (r.data_com == prov.data_com and r.data_pagamento == prov.data_pagamento
                 and r.tipo_pagamento == prov.tipo_pagamento and r.valor_por_cota == prov.valor_por_cota)
                for r in resultados
            ):
                resultados.append(prov)

        if resultados:
            break

    resultados.sort(key=lambda x: x.data_pagamento or "9999-99-99")
    return resultados


def fetch_statusinvest(ticker: str) -> List[ProventoAnunciado]:
    t = (ticker or "").upper().strip()
    resultados: List[ProventoAnunciado] = []

    urls_to_try = [
        f"https://statusinvest.com.br/fundos-imobiliarios/{t.lower()}",
        f"https://statusinvest.com.br/fiagros/{t.lower()}",
        f"https://statusinvest.com.br/acoes/{t.lower()}",
    ]

    # ✅ FIX 3: usa timezone de Brasília para evitar descartar eventos do dia em UTC-3
    try:
        from zoneinfo import ZoneInfo
        hoje = datetime.now(tz=ZoneInfo("America/Sao_Paulo")).date()
    except Exception:
        hoje = datetime.now().date()

    for url in urls_to_try:

        html = _safe_get(url)
        if "Ops! Página não encontrada" in html:
            continue

        text = _html_to_text(html)

        found_idx = -1
        for key in ["PRÓXIMO RENDIMENTO", "PRÓXIMO PROVENTO", "PRÓXIMO JCP", "PRÓXIMO DIVIDENDO"]:
            idx = text.find(key)
            if idx != -1:
                found_idx = idx
                break

        if found_idx == -1:
            continue

        snippet = text[found_idx : found_idx + 500]

        val_match = re.search(r"R\$\s*(\d+,\d+)", snippet)
        val = _parse_money_br(val_match.group(1)) if val_match else None

        dates = re.findall(r"(\d{2}/\d{2}/\d{4})", snippet)

        data_com = None
        data_pag = None

        if len(dates) >= 2:
            d1 = _parse_date_iso(dates[0])
            d2 = _parse_date_iso(dates[1])
            if d1 and d2:
                if d1 < d2:
                    data_com, data_pag = d1, d2
                else:
                    data_com, data_pag = d2, d1

        tipo_final = "RENDIMENTO"
        snip_upper = snippet.upper()
        if "JCP" in snip_upper or "JUROS SOBRE" in snip_upper:
            tipo_final = "JCP"
        elif "DIVIDENDO" in snip_upper:
            tipo_final = "RENDIMENTO" if is_fundo else "DIVIDENDO"
        elif "RENDIMENTO" in snip_upper or "PROVENTO" in snip_upper:
            tipo_final = "RENDIMENTO"

        dp_obj = _get_date_obj(data_pag) if data_pag else None
        if dp_obj and dp_obj >= hoje:
            if val and _valor_parece_valido(val) and data_pag:
                prov = ProventoAnunciado(
                    ticker=t,
                    fonte_url=url,
                    fonte_nome="STATUSINVEST",
                    data_com=data_com,
                    data_pagamento=data_pag,
                    valor_por_cota=float(val),
                    tipo_pagamento=tipo_final,
                )
                resultados.append(prov)
                break

    resultados.sort(key=lambda x: x.data_pagamento or "9999-99-99")
    return resultados


def fetch_provento_anunciado(ticker: str, logs: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    def log(msg: str):
        if isinstance(logs, list):
            logs.append(msg)

    t = (ticker or "").strip().upper()
    if not t:
        return []

    sources = [
        ("INVESTIDOR10", fetch_investidor10),
        ("STATUSINVEST", fetch_statusinvest),
    ]

    for name, fn in sources:
        try:
            log(f"🔎 Consultando {name}...")
            lista_provs = fn(t)

            if lista_provs:
                output = [p.to_row() for p in lista_provs]
                output.sort(key=lambda x: x.get("data_pagamento", "9999-99-99"))
                log(f"✅ {name}: Encontrados {len(output)} anúncios futuros.")
                for item in output:
                    log(f"   -> Pag: {item['data_pagamento']} | Val: {item['valor_por_cota']} | Tipo: {item['tipo_pagamento']}")
                return output

        except Exception as e:
            log(f"❌ Erro em {name}: {e}")

    log("❌ Nenhuma previsão futura encontrada.")
    return []
