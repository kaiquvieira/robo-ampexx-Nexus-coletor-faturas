import os
import argparse
import re
import hashlib
import json
import io
from datetime import datetime
from pathlib import Path

import pdfplumber
import requests
from dotenv import load_dotenv


# ===================== .env =====================
ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=ENV_PATH)


# ===================== Args =====================
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mes-ref", required=False, default=None, help="Mês no formato YYYY-MM (ex.: 2025-12). Se omitido, usa mês atual.")
    parser.add_argument("--bucket", required=False, default=None, help="Bucket do Supabase Storage (ex.: faturas-elektro). Se omitido, usa SUPABASE_STORAGE_BUCKET do .env.")
    parser.add_argument("--dry-run", action="store_true", help="Não grava no banco; só processa e mostra quantos registros seriam enviados.")
    parser.add_argument("--batch-size", type=int, default=500, help="Tamanho do batch de upsert no Supabase (default: 500).")
    return parser.parse_args()


def month_key_now():
    return datetime.now().strftime("%Y-%m")


# ===================== Regex / Utils =====================
NUM_RE = re.compile(r"-?\d{1,3}(?:\.\d{3})*(?:,\d{2,6})?|-?\d+(?:,\d{2,6})?")


def is_letter(ch: str) -> bool:
    return bool(ch) and ch.isalpha()


def sanitize_line_for_numbers(line: str) -> str:
    s = line
    s = re.sub(r"\b\d{1,2}/\d{1,2}/\d{4}\b", " ", s)
    s = re.sub(r"\b\d{1,2}/\d{4}\b", " ", s)
    s = re.sub(r"\b\d{1,2}/\d{1,2}\s*-\s*\d{1,2}/\d{1,2}\b", " ", s)
    s = re.sub(r"\b\d{1,2}/\d{1,2}\b", " ", s)
    s = " ".join(s.split())
    return s


def extract_numbers_ignoring_percent_and_codes(line: str):
    nums = []
    for m in NUM_RE.finditer(line):
        s = m.group(0)

        # ignora percentuais
        j = m.end()
        while j < len(line) and line[j].isspace():
            j += 1
        if j < len(line) and line[j] == "%":
            continue

        # ignora número colado em letra (VERM1, P1 etc)
        i = m.start()
        prev_ch = line[i - 1] if i - 1 >= 0 else ""
        next_ch = line[m.end()] if m.end() < len(line) else ""
        if is_letter(prev_ch) or is_letter(next_ch):
            continue

        nums.append(s)
    return nums


def split_bandeira_descricao(bandeira_raw: str):
    if not bandeira_raw:
        return ("", "")
    s = " ".join(str(bandeira_raw).split())
    pat = re.compile(r"\b(VERMELHA|AMARELA|VERDE)\b", re.IGNORECASE)
    matches = list(pat.finditer(s))
    if not matches:
        return (s.strip(), "")
    parts = []
    for idx, m in enumerate(matches):
        start = m.start()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(s)
        chunk = s[start:end].strip()
        if chunk:
            parts.append(chunk)
    desc1 = parts[0] if len(parts) >= 1 else ""
    desc2 = parts[1] if len(parts) >= 2 else ""
    return (desc1, desc2)


def infer_bandeira_raw_from_text(texto_completo: str):
    t = (texto_completo or "").upper()
    has_verm = "VERM" in t
    has_amar = "AMAR" in t
    has_verde = "VERDE" in t
    if has_verm and has_amar:
        return "Vermelha Amarela"
    if has_verm:
        return "Vermelha"
    if has_amar:
        return "Amarela"
    if has_verde:
        return "Verde"
    return "Verde"


def make_row_hash(d: dict) -> str:
    key_fields = [
        d.get("Mês Ref", ""),
        d.get("Cliente", ""),
        d.get("Cód Cliente", ""),
        d.get("Vencimento", ""),
        d.get("Itens da fatura", ""),
        d.get("Unid", ""),
        d.get("Quantidade", ""),
        d.get("Preço Unit com Trib(RS)", ""),
        d.get("Valor R$", ""),
        d.get("TARIFA UNIT.(R$)", ""),
    ]
    raw = "|".join([str(x).strip() for x in key_fields])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# ===================== Supabase Storage (Privado) =====================
def storage_headers(service_role_key: str):
    return {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
        "Content-Type": "application/json",
    }


def storage_list_all(url: str, service_role_key: str, bucket: str, prefix: str, limit: int = 1000):
    """
    Lista objetos no bucket via endpoint Storage:
      POST {SUPABASE_URL}/storage/v1/object/list/{bucket}
    Body: { prefix, limit, offset, sortBy }
    """
    endpoint = f"{url.rstrip('/')}/storage/v1/object/list/{bucket}"
    out = []
    offset = 0
    while True:
        payload = {
            "prefix": prefix,
            "limit": limit,
            "offset": offset,
            "sortBy": {"column": "name", "order": "asc"},
        }
        resp = requests.post(endpoint, headers=storage_headers(service_role_key), data=json.dumps(payload), timeout=120)
        if not (200 <= resp.status_code < 300):
            raise RuntimeError(f"Storage list falhou (HTTP {resp.status_code}): {resp.text[:500]}")
        data = resp.json() if resp.text else []
        if not data:
            break
        out.extend(data)
        if len(data) < limit:
            break
        offset += limit
    return out


def storage_download(url: str, service_role_key: str, bucket: str, object_key: str) -> bytes:
    """
    Download de objeto privado:
      GET {SUPABASE_URL}/storage/v1/object/authenticated/{bucket}/{object_key}
    """
    endpoint = f"{url.rstrip('/')}/storage/v1/object/authenticated/{bucket}/{object_key}"
    resp = requests.get(endpoint, headers=storage_headers(service_role_key), timeout=180)
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"Storage download falhou (HTTP {resp.status_code}): {resp.text[:300]}")
    return resp.content


def list_month_pdfs(url: str, service_role_key: str, bucket: str, mes_ref: str):
    """
    Retorna lista de tuples (cliente, cod_cliente, object_key) para o mês.
    Estrutura esperada: YYYY-MM/CLIENTE/UC.pdf
    """
    month_prefix = f"{mes_ref}/"

    # 1) lista "primeiro nível" do mês (prováveis pastas de cliente)
    top = storage_list_all(url, service_role_key, bucket, month_prefix, limit=1000)

    # candidatos a "pastas": no Storage list, pastas aparecem como name = "CLIENTE"
    client_names = []
    for it in top:
        nm = (it.get("name") or "").strip()
        if not nm:
            continue
        # se o item é pdf no root do mês, ignorar (não é esperado)
        if nm.lower().endswith(".pdf"):
            continue
        client_names.append(nm)

    client_names = sorted(set(client_names))

    pdfs = []
    for client in client_names:
        client_prefix = f"{mes_ref}/{client}/"
        items = storage_list_all(url, service_role_key, bucket, client_prefix, limit=1000)
        for it in items:
            name = (it.get("name") or "").strip()
            if not name.lower().endswith(".pdf"):
                continue
            cod = name[:-4]  # remove .pdf
            object_key = f"{mes_ref}/{client}/{name}"
            pdfs.append((client, cod, object_key))

    return pdfs


# ===================== Extractor (bytes) =====================
def extrair_dados_fatura_bytes(pdf_bytes: bytes, mes_ano: str, nome_cliente: str, cod_cliente_pasta: str):
    itens_lista = []
    vencimento = "N/A"
    total_fatura_pdf = "0,00"

    bandeira_raw = ""
    bandeira_desc_1 = ""
    bandeira_desc_2 = ""

    saldo_mes = "0"
    saldo_acumulado = "0"
    saldo_expirar = "0"

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            texto_completo = ""
            for pagina in pdf.pages:
                t = pagina.extract_text() or ""
                texto_completo += t + "\n"

            linhas = texto_completo.split("\n")

            # 1) vencimento e total
            for linha in linhas[:25]:
                if re.search(r"\d{2}/\d{2}/\d{4}", linha) and ("," in linha):
                    datas = re.findall(r"(\d{2}/\d{2}/\d{4})", linha)
                    valores = re.findall(r"(\d{1,3}(?:\.\d{3})*,\d{2})", linha)
                    if datas:
                        vencimento = datas[-1]
                    if valores:
                        total_fatura_pdf = valores[-1]
                    break

            # 2) bandeira
            band_match = re.search(r"Bandeira\s*Tarif[a-z]+ria\s+([^(\n\r]+)", texto_completo, re.IGNORECASE)
            if band_match:
                bandeira_raw = band_match.group(1).strip()
            else:
                bandeira_raw = infer_bandeira_raw_from_text(texto_completo)

            bandeira_desc_1, bandeira_desc_2 = split_bandeira_descricao(bandeira_raw)

            # 3) saldos GD
            s_mes = re.search(r"Saldo\s+Mes\s+no\s+\(TP\)\s+[\w\s]+\s+(\d+)", texto_completo, re.IGNORECASE)
            if s_mes:
                saldo_mes = s_mes.group(1)

            s_acum = re.search(r"Saldo\s+Acumulado\s+no\s+\(TP\)\s+[\w\s]+\s+(\d+)", texto_completo, re.IGNORECASE)
            if s_acum:
                saldo_acumulado = s_acum.group(1)

            s_exp = re.search(r"Saldo\s+a\s+Expirar\s+Proximo\s+Mes\s+no\s+\(TP\)\s+[\w\s]+\s+(\d+)", texto_completo, re.IGNORECASE)
            if s_exp:
                saldo_expirar = s_exp.group(1)

            # 4) itens
            for linha in linhas:
                linha_up = linha.upper()

                if any(x in linha_up for x in ["KWH", "WH ", "ENERGIA"]):
                    desc_match = re.search(r"^(.*?)(?:\s+k?Wh)", linha, re.IGNORECASE)
                    if not desc_match:
                        desc_match = re.search(r"^(.*?)(?=\s+-?\d)", linha)
                    desc = desc_match.group(1).strip() if desc_match else (linha.split()[0] if linha.split() else "Item")
                    desc_up = desc.upper()

                    linha_numeros = re.split(r"\bPIS\b|\bCOFINS\b|\bCONFINS\b", linha, flags=re.IGNORECASE)[0]
                    linha_numeros = sanitize_line_for_numbers(linha_numeros)
                    valores_linha = extract_numbers_ignoring_percent_and_codes(linha_numeros)

                    if len(valores_linha) >= 5:
                        try:
                            unid_item = "kWh" if "KWH" in linha_up else ("Wh" if "WH" in linha_up else "-")

                            qtd = valores_linha[0]
                            preco_unit_com_trib = valores_linha[1]
                            valor_rs = valores_linha[2]
                            pis_confins = valores_linha[3]
                            base_calc_icms = valores_linha[4]

                            aliq_match = re.search(r"(\d+)%", linha)
                            aliq = aliq_match.group(0) if aliq_match else "0%"

                            icms_valor = valores_linha[5] if len(valores_linha) > 5 else "0,00"
                            tarifa_unit = valores_linha[6] if len(valores_linha) > 6 else preco_unit_com_trib

                            # Regra AD.B: muda SOMENTE a TARIFA UNIT
                            if "AD.B.VERM1" in desc_up:
                                tarifa_unit = "0,044630"
                            elif "AD.B.AMAR" in desc_up:
                                tarifa_unit = "0,018850"

                            row = {
                                "Mês Ref": mes_ano,
                                "Cliente": nome_cliente,
                                "Cód Cliente": cod_cliente_pasta,
                                "Vencimento": vencimento,
                                "Total a Pagar (R$)": total_fatura_pdf,
                                "Itens da fatura": desc,
                                "Unid": unid_item,
                                "Quantidade": qtd,
                                "Preço Unit com Trib(RS)": preco_unit_com_trib,
                                "Valor R$": valor_rs,
                                "PIS/CONFINS(R$)": pis_confins,
                                "BASE CALC ICMS(R$)": base_calc_icms,
                                "Alíquota ICMS(%)": aliq,
                                "ICMS (R$)": icms_valor,
                                "TARIFA UNIT.(R$)": tarifa_unit,
                                "Bandeira Tarifária 1 (descrição)": bandeira_desc_1,
                                "Bandeira Tarifária 2 (descrição)": bandeira_desc_2,
                                "Saldo Mes (kWh)": saldo_mes,
                                "Saldo Acumulado (kWh)": saldo_acumulado,
                                "Saldo a Expirar (kWh)": saldo_expirar,
                            }
                            itens_lista.append(row)
                        except:
                            continue

                elif any(x in linha_up for x in ["ILUMINAÇÃO", "ILUM P", "MULTA", "JUROS", "DEVOL", "ILUM PUBLICA", "COBRANCA ILUM"]):
                    v_taxa = extract_numbers_ignoring_percent_and_codes(sanitize_line_for_numbers(linha))
                    if v_taxa:
                        row = {
                            "Mês Ref": mes_ano,
                            "Cliente": nome_cliente,
                            "Cód Cliente": cod_cliente_pasta,
                            "Vencimento": vencimento,
                            "Total a Pagar (R$)": total_fatura_pdf,
                            "Itens da fatura": linha[:60].split(",")[0].strip(),
                            "Unid": "-",
                            "Quantidade": "-",
                            "Preço Unit com Trib(RS)": "-",
                            "Valor R$": v_taxa[-1],
                            "PIS/CONFINS(R$)": "0,00",
                            "BASE CALC ICMS(R$)": "0,00",
                            "Alíquota ICMS(%)": "0%",
                            "ICMS (R$)": "0,00",
                            "TARIFA UNIT.(R$)": "-",
                            "Bandeira Tarifária 1 (descrição)": bandeira_desc_1,
                            "Bandeira Tarifária 2 (descrição)": bandeira_desc_2,
                            "Saldo Mes (kWh)": saldo_mes,
                            "Saldo Acumulado (kWh)": saldo_acumulado,
                            "Saldo a Expirar (kWh)": saldo_expirar,
                        }
                        itens_lista.append(row)

    except Exception as e:
        print(f"[ERRO] Falha ao ler PDF bytes (cliente={nome_cliente}, cod={cod_cliente_pasta}): {e}")

    return itens_lista


# ===================== Supabase DB upsert =====================
def to_supabase_rows(rows: list[dict]) -> list[dict]:
    out = []
    for r in rows:
        rh = make_row_hash(r)
        out.append({
            "mes_ref": r.get("Mês Ref"),
            "cliente": r.get("Cliente"),
            "cod_cliente": r.get("Cód Cliente"),
            "vencimento": r.get("Vencimento"),
            "total_pagar": r.get("Total a Pagar (R$)"),

            "item_fatura": r.get("Itens da fatura"),
            "unid": r.get("Unid"),
            "quantidade": r.get("Quantidade"),
            "preco_unit_com_trib": r.get("Preço Unit com Trib(RS)"),
            "valor_rs": r.get("Valor R$"),
            "pis_confins": r.get("PIS/CONFINS(R$)"),
            "base_calc_icms": r.get("BASE CALC ICMS(R$)"),
            "aliquota_icms": r.get("Alíquota ICMS(%)"),
            "icms_valor": r.get("ICMS (R$)"),
            "tarifa_unit": r.get("TARIFA UNIT.(R$)"),

            "bandeira_desc_1": r.get("Bandeira Tarifária 1 (descrição)"),
            "bandeira_desc_2": r.get("Bandeira Tarifária 2 (descrição)"),

            "saldo_mes": r.get("Saldo Mes (kWh)"),
            "saldo_acumulado": r.get("Saldo Acumulado (kWh)"),
            "saldo_expirar": r.get("Saldo a Expirar (kWh)"),

            "row_hash": rh,
        })
    return out


def supabase_upsert(table: str, rows: list[dict], url: str, key: str, batch_size: int = 500, verbose: bool = True):
    endpoint = f"{url.rstrip('/')}/rest/v1/{table}"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }

    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        params = {"on_conflict": "row_hash"}

        if verbose:
            print(f"[SUPA] POST {endpoint} batch={len(batch)} ({i+1}-{i+len(batch)} de {len(rows)})")

        resp = requests.post(endpoint, headers=headers, params=params, data=json.dumps(batch), timeout=120)

        if verbose:
            print(f"[SUPA] status={resp.status_code}")

        if not (200 <= resp.status_code < 300):
            body = (resp.text or "")[:800]
            raise RuntimeError(f"Falha no upsert (HTTP {resp.status_code}): {body}")

        total += len(batch)

    return total


# ===================== Main =====================
def main():
    args = parse_args()

    mes_ref = (args.mes_ref or "").strip() or month_key_now()

    supa_url = (os.getenv("SUPABASE_URL") or "").strip()
    supa_key = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    table = (os.getenv("SUPABASE_TABLE") or "").strip()

    bucket = (args.bucket or "").strip() or (os.getenv("SUPABASE_STORAGE_BUCKET") or "").strip()

    if not supa_url:
        raise SystemExit("Faltando SUPABASE_URL no .env.")
    if not supa_key:
        raise SystemExit("Faltando SUPABASE_SERVICE_ROLE_KEY no .env.")
    if not table:
        raise SystemExit("Faltando SUPABASE_TABLE no .env (ex.: faturas_elektro_itens).")
    if not bucket:
        raise SystemExit("Faltando bucket (use --bucket ou SUPABASE_STORAGE_BUCKET no .env).")

    print(f"[ENV OK] url={supa_url} table={table} key=SERVICE_ROLE bucket={bucket} mes_ref={mes_ref}")

    # 1) lista PDFs do mês no Storage
    pdfs = list_month_pdfs(supa_url, supa_key, bucket, mes_ref)
    if not pdfs:
        print(f"[INFO] Nenhum PDF encontrado no Storage para {mes_ref}.")
        return

    print(f"[INFO] PDFs encontrados no Storage: {len(pdfs)}")

    # 2) baixa e extrai
    consolidado = []
    for idx, (cliente, cod, object_key) in enumerate(pdfs, start=1):
        print(f"[{idx}/{len(pdfs)}] Baixando: {object_key}")
        try:
            pdf_bytes = storage_download(supa_url, supa_key, bucket, object_key)
            dados = extrair_dados_fatura_bytes(pdf_bytes, mes_ref, cliente, cod)
            consolidado.extend(dados)
        except Exception as e:
            print(f"[WARN] Falha ao processar {object_key}: {e}")
            continue

    if not consolidado:
        print("[INFO] Nenhum dado extraído dos PDFs.")
        return

    # 3) upsert
    rows = to_supabase_rows(consolidado)
    print(f"[INFO] Registros extraídos para banco: {len(rows)}")

    if args.dry_run:
        print("[DRY-RUN] Nenhum registro foi gravado.")
        return

    total = supabase_upsert(table, rows, supa_url, supa_key, batch_size=args.batch_size, verbose=True)
    print(f"[OK] Upsert concluído. Linhas enviadas: {total}. Tabela: {table}")


if __name__ == "__main__":
    main()
