#!/usr/bin/env python3
import json
import re
import sys
import urllib.request
from pathlib import Path

try:
    from pypdf import PdfReader
except Exception:
    print(json.dumps({"ok": False, "error": "pypdf_not_installed"}, ensure_ascii=False))
    sys.exit(0)


def fetch_json(url: str):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read().decode("utf-8", "ignore"))


def find_annual_report_art_code(stock_code: str, year: int):
    for p in range(1, 26):
        url = (
            "https://np-anotice-stock.eastmoney.com/api/security/ann"
            f"?sr=-1&page_size=100&page_index={p}&ann_type=A&stock_list={stock_code}"
        )
        data = fetch_json(url)
        rows = ((data.get("data") or {}).get("list") or [])
        if not rows:
            break
        for it in rows:
            title = str(it.get("title") or "")
            if f"{year}年年度报告" in title and "摘要" not in title:
                return it.get("art_code"), title, str(it.get("notice_date") or "")[:10]
    return None, None, None


def pdf_url_from_art_code(art_code: str):
    return f"https://pdf.dfcfw.com/pdf/H2_{art_code}_1.pdf"


def download_pdf(url: str, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.stat().st_size > 200_000:
        return
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=25) as r:
        path.write_bytes(r.read())


def read_pdf_text(path: Path):
    reader = PdfReader(str(path))
    texts = []
    for p in reader.pages:
        t = (p.extract_text() or "").replace("\n", " ")
        if t:
            texts.append(t)
    return " ".join(texts)


def clean_name(name: str):
    n = re.sub(r"\s+", "", str(name or "")).strip()
    n = re.sub(r"^[（(【\[]+", "", n)
    n = re.sub(r"[）)】\]]+$", "", n)
    n = re.sub(r"(（以下简称.*?$|\(以下简称.*?$)", "", n)
    return n


def valid_company_name(name: str):
    n = clean_name(name)
    if len(n) < 2 or len(n) > 40:
        return False
    if re.search(r"(客户|供应商|合计|占比|金额|比例|公司名称|单位|序号|前五|其中|其他|数据来源|披露)", n):
        return False
    if re.search(r"(有限公司|股份有限公司|集团|银行|证券|电网|电力|汽车|科技|电子|半导体|通信|能源|医院|大学|研究院)$", n):
        return True
    if re.fullmatch(r"[\u4e00-\u9fa5A-Za-z0-9]{2,12}", n):
        return True
    return False


def extract_table_items(seg: str, keyword: str, name_marker: str):
    # Match rows like:
    # 1 客户一 397,048,827.70 9.90%
    # 1 客户一 397,048,827.70
    rows = []
    patterns = [
        re.compile(r"\b([1-5])\s+([^\d\s][^\d]{1,50}?)\s+([\d,]+(?:\.\d+)?)\s+([\d.]+%)"),
        re.compile(r"\b([1-5])\s+([^\d\s][^\d]{1,50}?)\s+([\d,]+(?:\.\d+)?)\b"),
        re.compile(r"(?:客户|供应商)\s*([一二三四五1-5])\s*[：:]\s*([^\d\s][^\d]{1,50}?)\s+([\d,]+(?:\.\d+)?)\s*([%％]?)"),
    ]
    for pattern in patterns:
        for m in pattern.finditer(seg):
            rank_raw = m.group(1)
            if rank_raw in ["一", "二", "三", "四", "五"]:
                rank = "一二三四五".index(rank_raw) + 1
            else:
                rank = int(rank_raw)
            name = clean_name(m.group(2).strip())
            if not valid_company_name(name):
                continue
            amount = (m.group(3) or "").replace(",", "")
            ratio = (m.group(4) or "").replace("％", "%")
            if not amount:
                continue
            rows.append({
                "rank": rank,
                "name": name,
                "amount": float(amount) if amount else None,
                "ratio": ratio if ratio else "",
                "source": "annual_report_pdf",
                "reason": f"{keyword}披露",
                "confidence": 0.95,
            })
    # fallback phrase extraction: 前五大客户包括A、B、C...
    if not rows:
        phrase = re.search(r"(?:前\s*五\s*大?(?:客户|供应商)[^。；\n]{0,120})", seg)
        if phrase:
            tokens = re.split(r"[、,，；;]", phrase.group(1))
            rank = 1
            for tk in tokens:
                name = clean_name(tk)
                if not valid_company_name(name):
                    continue
                rows.append({
                    "rank": rank,
                    "name": name,
                    "amount": None,
                    "ratio": "",
                    "source": "annual_report_pdf",
                    "reason": f"{keyword}披露",
                    "confidence": 0.88,
                })
                rank += 1
                if rank > 5:
                    break
    # dedupe by rank
    dedup = {}
    for r in rows:
        if r["rank"] not in dedup:
            dedup[r["rank"]] = r
    out = [dedup[k] for k in sorted(dedup.keys())]
    return out[:5]


def extract_relations(full_text: str):
    t = re.sub(r"\s+", " ", full_text)
    c_start = -1
    for marker in ["公司前5大客户资料", "公司前5名客户资料", "前五大客户", "主要销售客户情况", "主要客户情况"]:
        c_start = t.find(marker)
        if c_start >= 0:
            break
    c_end = -1
    if c_start >= 0:
        for marker in ["主要客户其他情况说明", "前五大客户和供应商情况说明", "供应商情况", "主要供应商情况"]:
            c_end = t.find(marker, c_start + 10)
            if c_end > c_start:
                break
    c_seg = t[c_start : (c_end if c_end > c_start else c_start + 12000)] if c_start >= 0 else ""

    s_start = -1
    for marker in ["公司前5名供应商资料", "公司前5大供应商资料", "前五大供应商", "主要供应商情况", "采购情况"]:
        s_start = t.find(marker)
        if s_start >= 0:
            break
    s_end = -1
    if s_start >= 0:
        for marker in ["主要供应商其他情况说明", "客户情况", "主要客户情况", "研发投入"]:
            s_end = t.find(marker, s_start + 10)
            if s_end > s_start:
                break
    s_seg = t[s_start : (s_end if s_end > s_start else s_start + 12000)] if s_start >= 0 else ""

    customers = extract_table_items(c_seg, "主要销售客户", "客户") if c_seg else []
    suppliers = extract_table_items(s_seg, "主要供应商", "供应商") if s_seg else []

    return customers, suppliers


def main():
    if len(sys.argv) < 2:
        print(json.dumps({"ok": False, "error": "missing_stock_code"}, ensure_ascii=False))
        return
    stock_code = re.sub(r"\D", "", sys.argv[1])
    year = int(sys.argv[2]) if len(sys.argv) >= 3 and str(sys.argv[2]).isdigit() else 2024
    if len(stock_code) != 6:
        print(json.dumps({"ok": False, "error": "invalid_stock_code"}, ensure_ascii=False))
        return

    art_code, title, notice_date = find_annual_report_art_code(stock_code, year)
    if not art_code:
        print(json.dumps({"ok": True, "customers": [], "suppliers": [], "meta": {"found": False}}, ensure_ascii=False))
        return

    url = pdf_url_from_art_code(art_code)
    cache = Path(f"/tmp/annual_{art_code}.pdf")
    try:
        download_pdf(url, cache)
        txt = read_pdf_text(cache)
        customers, suppliers = extract_relations(txt)
        print(
            json.dumps(
                {
                    "ok": True,
                    "customers": customers,
                    "suppliers": suppliers,
                    "meta": {
                        "found": True,
                        "year": year,
                        "art_code": art_code,
                        "title": title,
                        "notice_date": notice_date,
                        "pdf_url": url,
                    },
                },
                ensure_ascii=False,
            )
        )
    except Exception as e:
        print(json.dumps({"ok": False, "error": str(e), "meta": {"pdf_url": url, "art_code": art_code}}, ensure_ascii=False))


if __name__ == "__main__":
    main()
