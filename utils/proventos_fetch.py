# utils/proventos_fetch.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import re
import json
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

def _safe_get_json(url: str, timeout: int = 15) -> Any:
    try:
        r = requests.get(url, headers=_HEADERS, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None

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
        # ══════════════════════════════════════════════════════════════════════
        # self.valor_por_cota = BRUTO por cota (conforme capturado pelo fetch)
        # _val_liq_por_cota   = líquido por cota (real do site, quando disponível)
        # _val_ir_por_cota    = IR por cota (real do site, quando disponível)
        #
        # Para JCP/RENDIMENTO_TRIB sem dados reais do site: calcula 17,5% (LC 224/2025)
        # Para DIVIDENDO/RENDIMENTO de FII: IR = 0 (isento)
        # ══════════════════════════════════════════════════════════════════════
        vpc_bruto = self.valor_por_cota  # sempre é o BRUTO
        _liq_site = getattr(self, "_val_liq_por_cota", None)   # líquido real do site
        _ir_site  = getattr(self, "_val_ir_por_cota",  None)   # IR real do site

        # Decide ir_por_cota e vpc_liq: prioriza dado real do site, fallback calcula
        tp = (self.tipo_pagamento or "").upper()
        if vpc_bruto is not None and vpc_bruto > 0:
            if _ir_site is not None and _ir_site > 0:
                # Site retornou IR real — usa direto
                ir_por_cota = round(float(_ir_site), 8)
                vpc_liq     = round(float(vpc_bruto) - ir_por_cota, 8)
            elif _liq_site is not None and _liq_site > 0 and _liq_site < vpc_bruto:
                # Site retornou líquido real — deriva o IR
                vpc_liq     = round(float(_liq_site), 8)
                ir_por_cota = round(float(vpc_bruto) - vpc_liq, 8)
            elif tp in ("JCP", "RENDIMENTO_TRIB"):
                # Sem dado real: alíquota por data (LC 224/2025: 17,5% a partir de 01/01/2026)
                _dp = self.data_pagamento or self.data_com or ""
                _aliq = 0.175 if _dp >= "2026-01-01" else 0.15
                ir_por_cota = round(float(vpc_bruto) * _aliq, 8)
                vpc_liq     = round(float(vpc_bruto) * (1 - _aliq), 8)
            else:
                # DIVIDENDO / RENDIMENTO FII: isento de IR
                ir_por_cota = 0.0
                vpc_liq     = float(vpc_bruto)
        else:
            ir_por_cota = None
            vpc_liq     = None

        return {
            "ticker":              self.ticker,
            "status":              self.status,
            "tipo_pagamento":      self.tipo_pagamento,
            "data_com":            self.data_com or "",
            "data_pagamento":      self.data_pagamento or "",
            "valor_por_cota":      round(float(vpc_bruto), 8) if vpc_bruto else "",   # BRUTO
            "valor_bruto_por_cota": round(float(vpc_bruto), 8) if vpc_bruto else "",  # redundante, mesma coisa
            "valor_liq_por_cota":  vpc_liq     if vpc_liq is not None else "",        # LÍQUIDO
            "ir_por_cota":         ir_por_cota if ir_por_cota is not None else "",    # IR por cota
            "fonte_url":           self.fonte_url,
            "fonte_nome":          self.fonte_nome,
            "capturado_em":        self.capturado_em or datetime.now().strftime("%Y-%m-%d %H:%M"),
        }


# =============================================================================
# ✅ NOVO: busca via API JSON do Investidor10 — retorna valor com 8 casas decimais
# O HTML público exibe valor arredondado (ex: 0,18), a API retorna 0,17518233
# Endpoint: /api/v2/acoes/{ticker}/dividendos/?page=1
# =============================================================================
def _fetch_investidor10_api(ticker: str, hoje) -> List[ProventoAnunciado]:
    """
    Tenta buscar proventos via API JSON do Investidor10.
    Retorna lista vazia se a API não responder ou não tiver dados futuros.
    """
    t = ticker.upper().strip()
    resultados: List[ProventoAnunciado] = []

    # A API do Investidor10 retorna JSON com valores completos (sem arredondamento)
    api_url = f"https://investidor10.com.br/api/v2/acoes/{t.lower()}/dividendos/?page=1"

    data = _safe_get_json(api_url)
    if not data:
        return []

    # Estrutura esperada: {"results": [...]} ou lista direta
    items = []
    if isinstance(data, dict):
        items = data.get("results", data.get("data", data.get("dividendos", [])))
    elif isinstance(data, list):
        items = data

    if not items:
        return []

    # Mapeamento de campos — o Investidor10 pode usar nomes variados
    _TIPO_MAP = {
        "jscp": "JCP",
        "jcp": "JCP",
        "juros sobre capital proprio": "JCP",
        "dividendo": "DIVIDENDO",
        "dividendos": "DIVIDENDO",
        "rendimento tributavel": "RENDIMENTO_TRIB",
        "rendimento trib": "RENDIMENTO_TRIB",
        "rend. trib.": "RENDIMENTO_TRIB",
        "rendimento": "RENDIMENTO",
    }

    for item in items:
        if not isinstance(item, dict):
            continue

        # Extrai data_com
        dc_raw = (
            item.get("data_com") or item.get("dataCom") or
            item.get("ex_date") or item.get("exDate") or ""
        )
        # Extrai data_pagamento
        dp_raw = (
            item.get("data_pagamento") or item.get("dataPagamento") or
            item.get("payment_date") or item.get("paymentDate") or
            item.get("data_pagto") or ""
        )
        # Extrai valor bruto — prioriza campos com nome "bruto"
        valor_bruto = (
            item.get("valor_bruto") or item.get("valorBruto") or
            item.get("value") or item.get("valor") or
            item.get("dividend_value") or item.get("dividendValue") or
            item.get("valor_por_cota") or item.get("valorPorCota") or 0
        )
        # Extrai valor líquido (quando disponível)
        valor_liq = (
            item.get("valor_liquido") or item.get("valorLiquido") or
            item.get("net_value") or item.get("netValue") or None
        )
        # Extrai IR (quando disponível)
        valor_ir = (
            item.get("ir") or item.get("imposto") or
            item.get("tax_value") or item.get("taxValue") or None
        )
        # Extrai tipo
        tipo_raw = str(
            item.get("tipo") or item.get("type") or
            item.get("tipo_pagamento") or item.get("tipoPagamento") or
            item.get("dividend_type") or "dividendo"
        ).strip().lower()

        dc = _parse_date_iso(str(dc_raw)) if dc_raw else None
        dp = _parse_date_iso(str(dp_raw)) if dp_raw else None

        # Só registros com data de pagamento futura (ou dentro da janela retroativa)
        # JANELA_RETROATIVA_DIAS=7 → aceita pagamentos dos últimos 7 dias (modo teste)
        # JANELA_RETROATIVA_DIAS=0 → comportamento padrão (só futuro)
        import os as _os_inner
        _retro_dias = 0
        try:
            _retro_dias = int(_os_inner.getenv("JANELA_RETROATIVA_DIAS", "0"))
        except (ValueError, TypeError):
            _retro_dias = 0
        from datetime import timedelta as _td
        _data_minima = hoje - _td(days=_retro_dias)
        dp_obj = _get_date_obj(dp) if dp else None
        if not (dp_obj and dp_obj >= _data_minima):
            continue

        # Converte valor para float (já vem com ponto decimal da API)
        try:
            vpc = float(str(valor_bruto).replace(",", "."))
        except Exception:
            continue
        if not vpc or vpc <= 0:
            continue

        # Converte liq/ir se disponíveis
        try:
            vpc_liq = float(str(valor_liq).replace(",", ".")) if valor_liq else None
        except Exception:
            vpc_liq = None
        try:
            vpc_ir = float(str(valor_ir).replace(",", ".")) if valor_ir else None
        except Exception:
            vpc_ir = None

        # Mapeia tipo
        tipo_final = _TIPO_MAP.get(tipo_raw, "DIVIDENDO")

        prov = ProventoAnunciado(
            ticker=t,
            fonte_url=api_url,
            fonte_nome="INVESTIDOR10_API",
            data_com=dc,
            data_pagamento=dp,
            valor_por_cota=vpc,
            tipo_pagamento=tipo_final,
        )
        prov._val_liq_por_cota = vpc_liq  # type: ignore[attr-defined]
        prov._val_ir_por_cota  = vpc_ir   # type: ignore[attr-defined]

        # Dedup por data_com + data_pag + tipo + valor (8 casas)
        chave = (prov.data_com, prov.data_pagamento, prov.tipo_pagamento, round(vpc, 8))
        if not any(
            (r.data_com, r.data_pagamento, r.tipo_pagamento, round(r.valor_por_cota or 0, 8)) == chave
            for r in resultados
        ):
            resultados.append(prov)

    return resultados


def fetch_investidor10(ticker: str, hoje: Optional[date] = None) -> List[ProventoAnunciado]:
    t = (ticker or "").upper().strip()
    resultados: List[ProventoAnunciado] = []

    # ✅ Timezone de Brasília — usa hoje passado externamente (ex: HOJE_OVERRIDE) ou data real
    if hoje is None:
        try:
            from zoneinfo import ZoneInfo
            hoje = datetime.now(tz=ZoneInfo("America/Sao_Paulo")).date()
        except Exception:
            hoje = datetime.now().date()

    # ══════════════════════════════════════════════════════════════════════════
    # PASSO 1: tenta API JSON (valores com 8 casas decimais, sem arredondamento)
    # Funciona para ações. Para FIIs a API pode não existir — cai no scraping.
    # ══════════════════════════════════════════════════════════════════════════
    try:
        api_results = _fetch_investidor10_api(t, hoje)
        if api_results:
            api_results.sort(key=lambda x: x.data_pagamento or "9999-99-99")
            return api_results
    except Exception:
        pass  # fallback para scraping HTML

    # ══════════════════════════════════════════════════════════════════════════
    # PASSO 2: fallback — scraping HTML (valor pode vir arredondado para ações)
    # ══════════════════════════════════════════════════════════════════════════
    urls_to_try = [
        f"https://investidor10.com.br/fiis/{t.lower()}/",
        f"https://investidor10.com.br/fiagros/{t.lower()}/",
        f"https://investidor10.com.br/acoes/{t.lower()}/",
    ]

    for url in urls_to_try:
        is_fundo = ("/fiis/" in url) or ("/fiagros/" in url)

        html = _safe_get(url)
        text = _html_to_text(html)

        pattern = re.compile(
            r"(Dividendos?|Rendimentos?|Rend\.?\s*Trib\.?|JSCP|JCP)\s*"
            r"(\d{2}/\d{2}/\d{4})\s*"        # data_com
            r"(\d{2}/\d{2}/\d{4})\s*"        # data_pagamento
            r"(\d+[.,]\d{1,8})"               # valor — até 8 casas decimais
            r"(?:\s+(\d+[.,]\d{1,8}))?"       # segundo valor (opcional)
            r"(?:\s+(\d+[.,]\d{1,8}))?",      # terceiro valor (opcional)
            re.IGNORECASE,
        )
        matches = pattern.findall(text)
        if not matches:
            continue

        for match in matches:
            tipo_raw = match[0]
            dc_raw   = match[1]
            dp_raw   = match[2]

            # Layout página pública Investidor10 (sem login):
            #   FII logado:  tipo | data_com | data_pag | LIQ/cota | BRUTO/cota | IR/cota
            #   Ação público: tipo | data_com | data_pag | VALOR (arredondado 2 casas)
            val_liq_raw   = match[3]
            val_bruto_raw = match[4] if len(match) > 4 and match[4] else ""
            val_ir_raw    = match[5] if len(match) > 5 and match[5] else ""

            val_liq_cota   = _parse_money_br(val_liq_raw)
            val_bruto_cota = _parse_money_br(val_bruto_raw) if val_bruto_raw else None
            val_ir_cota    = _parse_money_br(val_ir_raw)    if val_ir_raw    else None

            # Se só vier um valor, trata como bruto
            if val_bruto_cota is None and val_liq_cota is not None:
                val_bruto_cota = val_liq_cota
                val_liq_cota   = None
                val_ir_cota    = None

            if val_bruto_cota is None:
                continue

            dc = _parse_date_iso(dc_raw)
            dp = _parse_date_iso(dp_raw)

            dp_obj = _get_date_obj(dp) if dp else None
            # Janela retroativa: JANELA_RETROATIVA_DIAS=7 aceita pagamentos dos últimos 7 dias
            import os as _os_html
            _retro_html = 0
            try:
                _retro_html = int(_os_html.getenv("JANELA_RETROATIVA_DIAS", "0"))
            except (ValueError, TypeError):
                _retro_html = 0
            from datetime import timedelta as _td_html
            _data_min_html = hoje - _td_html(days=_retro_html)
            if not (dp_obj and dp_obj >= _data_min_html):
                continue

            tipo_upper = (tipo_raw or "").upper().strip()
            if "JSCP" in tipo_upper or "JCP" in tipo_upper:
                tipo_final = "JCP"
            elif "REND" in tipo_upper and "TRIB" in tipo_upper:
                tipo_final = "RENDIMENTO_TRIB"
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
                valor_por_cota=float(val_bruto_cota),
                tipo_pagamento=tipo_final,
            )
            prov._val_liq_por_cota = val_liq_cota   # type: ignore[attr-defined]
            prov._val_ir_por_cota  = val_ir_cota    # type: ignore[attr-defined]

            chave = (prov.data_com, prov.data_pagamento, prov.tipo_pagamento, round(val_bruto_cota, 8))
            if not any(
                (r.data_com, r.data_pagamento, r.tipo_pagamento, round(r.valor_por_cota or 0, 8)) == chave
                for r in resultados
            ):
                resultados.append(prov)

        if resultados:
            break

    resultados.sort(key=lambda x: x.data_pagamento or "9999-99-99")
    return resultados


def fetch_statusinvest(ticker: str, hoje: Optional[date] = None) -> List[ProventoAnunciado]:
    t = (ticker or "").upper().strip()
    resultados: List[ProventoAnunciado] = []

    urls_to_try = [
        f"https://statusinvest.com.br/fundos-imobiliarios/{t.lower()}",
        f"https://statusinvest.com.br/fiagros/{t.lower()}",
        f"https://statusinvest.com.br/acoes/{t.lower()}",
    ]

    # usa hoje passado externamente (ex: HOJE_OVERRIDE) ou data real
    if hoje is None:
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
            tipo_final = "DIVIDENDO"
        elif "RENDIMENTO" in snip_upper or "PROVENTO" in snip_upper:
            tipo_final = "RENDIMENTO"

        dp_obj = _get_date_obj(data_pag) if data_pag else None
        # Janela retroativa: JANELA_RETROATIVA_DIAS=7 aceita pagamentos dos últimos 7 dias
        import os as _os_si
        _retro_si = 0
        try:
            _retro_si = int(_os_si.getenv("JANELA_RETROATIVA_DIAS", "0"))
        except (ValueError, TypeError):
            _retro_si = 0
        from datetime import timedelta as _td_si
        _data_min_si = hoje - _td_si(days=_retro_si)
        if dp_obj and dp_obj >= _data_min_si:
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

    # ── MODO TESTE: respeita HOJE_OVERRIDE do job ────────────────────────────
    import os as _os
    _hoje_ref: Optional[date] = None
    _override = (_os.getenv("HOJE_OVERRIDE") or "").strip()
    if _override:
        try:
            _hoje_ref = datetime.strptime(_override, "%Y-%m-%d").date()
        except ValueError:
            pass
    # ────────────────────────────────────────────────────────────────────────

    sources = [
        ("INVESTIDOR10", fetch_investidor10),
        ("STATUSINVEST", fetch_statusinvest),
    ]

    for name, fn in sources:
        try:
            log(f"🔎 Consultando {name}...")
            lista_provs = fn(t, hoje=_hoje_ref) if _hoje_ref else fn(t)

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
