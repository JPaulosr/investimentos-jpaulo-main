import pandas as pd

def sync_ativos_from_compras(gc, sheet_id: str):
    sh = gc.open_by_key(sheet_id)

    ws_compras = sh.worksheet("compras")
    ws_ativos  = sh.worksheet("ativos")

    compras_values = ws_compras.get_all_values()
    if len(compras_values) < 2:
        return {"ok": False, "msg": "Aba compras vazia"}

    compras = pd.DataFrame(compras_values[1:], columns=compras_values[0])
    compras.columns = [c.strip().lower() for c in compras.columns]

    # sua coluna é "ticker" mesmo (pelo print)
    if "ticker" not in compras.columns:
        return {"ok": False, "msg": "Coluna 'ticker' não encontrada em compras"}

    tickers = (
        compras["ticker"].astype(str).str.strip().str.upper()
        .replace(["", "NONE", "NAN", "NULL"], pd.NA)
        .dropna()
        .unique()
        .tolist()
    )
    tickers = sorted(tickers)

    ativos_values = ws_ativos.get_all_values()
    if ativos_values:
        ativos = pd.DataFrame(ativos_values[1:], columns=ativos_values[0])
        ativos.columns = [c.strip().lower() for c in ativos.columns]
    else:
        ativos = pd.DataFrame()

    # garante cabeçalhos mínimos na aba ativos
    headers = ["ticker", "classe", "subtipo", "segmento", "moeda", "ativo", "criado_em"]
    if not ativos_values or [h.strip().lower() for h in ativos_values[0]] != headers:
        ws_ativos.clear()
        ws_ativos.append_row(headers)
        ativos = pd.DataFrame(columns=headers)

    existentes = set(ativos["ticker"].astype(str).str.strip().str.upper()) if not ativos.empty else set()
    novos = [t for t in tickers if t not in existentes]

    if not novos:
        return {"ok": True, "msg": "Nenhum ticker novo. Ativos já está sincronizada.", "novos": 0}

    now = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = [[t, "", "", "", "BRL", "1", now] for t in novos]
    ws_ativos.append_rows(rows, value_input_option="RAW")

    return {"ok": True, "msg": "Ativos sincronizada com sucesso.", "novos": len(novos)}
