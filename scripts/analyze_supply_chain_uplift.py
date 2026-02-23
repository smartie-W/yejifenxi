#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
from pathlib import Path

BASE = Path('/Users/wang/Documents/codex/listed-supply-chain-mvp/data')
AB_PATH = BASE / 'semicon_fallback_ab_test.json'
MIX_PATH = BASE / 'semicon_source_mix_v2.json'
AUTO_PATH = BASE / 'auto_chain_name_industry_review.json'
OUT_PATH = BASE / 'supply_chain_uplift_report.json'


def pct(n, d):
    return round((n / d) * 100, 2) if d else 0.0


def main():
    ab = json.loads(AB_PATH.read_text(encoding='utf-8'))
    mix = json.loads(MIX_PATH.read_text(encoding='utf-8'))
    auto = json.loads(AUTO_PATH.read_text(encoding='utf-8'))

    on_s = ab.get('on', {}).get('semicon', {})
    off_s = ab.get('off', {}).get('semicon', {})
    on_count = on_s.get('count', 0)
    off_count = off_s.get('count', 0)
    on_both = on_s.get('both_nonempty', 0)
    off_both = off_s.get('both_nonempty', 0)
    on_avg = ab.get('on', {}).get('all', {}).get('avg_sec', 0)
    off_avg = ab.get('off', {}).get('all', {}).get('avg_sec', 0)

    supplier_sources = dict(mix.get('supplier_source_top', []))
    customer_sources = dict(mix.get('customer_source_top', []))
    semicon_supplier = supplier_sources.get('semiconductor_linkage_review', 0)
    semicon_customer = customer_sources.get('semiconductor_linkage_review', 0)
    supplier_total = sum(supplier_sources.values())
    customer_total = sum(customer_sources.values())

    rows = auto.get('rows', [])
    supplier_nonempty = sum(1 for r in rows if (r.get('suppliers') or 0) > 0)
    customer_nonempty = sum(1 for r in rows if (r.get('customers') or 0) > 0)
    avg_suppliers = round(sum((r.get('suppliers') or 0) for r in rows) / len(rows), 3) if rows else 0.0
    avg_customers = round(sum((r.get('customers') or 0) for r in rows) / len(rows), 3) if rows else 0.0

    result = {
        'ab_semicon_both_nonempty_on_pct': pct(on_both, on_count),
        'ab_semicon_both_nonempty_off_pct': pct(off_both, off_count),
        'ab_semicon_both_nonempty_delta_pct_point': round(pct(on_both, on_count) - pct(off_both, off_count), 2),
        'ab_avg_sec_on': on_avg,
        'ab_avg_sec_off': off_avg,
        'ab_avg_sec_delta_on_minus_off': round(on_avg - off_avg, 3),
        'source_semiconductor_linkage_supplier_share_pct': pct(semicon_supplier, supplier_total),
        'source_semiconductor_linkage_customer_share_pct': pct(semicon_customer, customer_total),
        'auto_chain_supplier_nonempty_pct': pct(supplier_nonempty, len(rows)),
        'auto_chain_customer_nonempty_pct': pct(customer_nonempty, len(rows)),
        'auto_chain_avg_suppliers': avg_suppliers,
        'auto_chain_avg_customers': avg_customers,
        'conclusion': (
            '半导体链路兜底在“来源占比”和“绝对覆盖”上明显发挥作用，'
            '但在当前 A/B 文件下未体现出相对命中率提升，且有额外耗时。'
        ),
    }

    OUT_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(result, ensure_ascii=False, indent=2))
    print(OUT_PATH)


if __name__ == '__main__':
    main()
