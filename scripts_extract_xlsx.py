import json
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

NS = {'a': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
CONTRACT_TYPES = {'SAAS', '私有部署订阅', '私有部署买断'}
PAYMENT_TYPES = {'新签', '增购', '续约', '维保', '升级费'}
CUSTOMER_TYPES = {'新客户', '老客户'}
INDICATOR_TYPES = {'新客户指标', '老客户指标'}
QUARTERS = {'Q1', 'Q2', 'Q3', 'Q4'}


def _shared_strings(z):
    if 'xl/sharedStrings.xml' not in z.namelist():
        return []
    sst = ET.fromstring(z.read('xl/sharedStrings.xml'))
    shared = []
    for si in sst.findall('.//a:si', NS):
        texts = [t.text or '' for t in si.findall('.//a:t', NS)]
        shared.append(''.join(texts))
    return shared


def _read_sheet(z, sheet_index):
    shared = _shared_strings(z)
    sheet_name = f'xl/worksheets/sheet{sheet_index + 1}.xml'
    sheet = ET.fromstring(z.read(sheet_name))
    rows = []
    for row in sheet.findall('.//a:row', NS):
        row_vals = []
        for c in row.findall('a:c', NS):
            t = c.get('t')
            v = c.find('a:v', NS)
            if v is None:
                val = ''
            else:
                if t == 's':
                    idx = int(v.text)
                    val = shared[idx] if idx < len(shared) else ''
                else:
                    val = v.text
            row_vals.append(val)
        rows.append(row_vals)
    return rows


def _sheet_names(z):
    wb = ET.fromstring(z.read('xl/workbook.xml'))
    return [s.get('name') for s in wb.findall('.//a:sheets/a:sheet', NS)]


def _num(value):
    text = str(value or '').replace(',', '').replace('￥', '').replace('¥', '').strip()
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return None


def _has_text(value):
    return bool(str(value or '').strip())


def is_valid_contract_row(row):
    return (
        _has_text(row.get('销售人员'))
        and _has_text(row.get('客户名称'))
        and str(row.get('合同类型') or '').strip() in CONTRACT_TYPES
        and str(row.get('签约季度') or '').strip() in QUARTERS
        and (_num(row.get('合同金额')) or 0) > 0
    )


def is_valid_payment_row(row):
    amount = _num(row.get('本次到款金额')) or 0
    total_cost = _num(row.get('本次到款-合计成本')) or 0
    actual = _num(row.get('实际计提金额')) or 0
    return (
        _has_text(row.get('销售人员'))
        and _has_text(row.get('客户名称'))
        and str(row.get('客户类型') or '').strip() in CUSTOMER_TYPES
        and str(row.get('合同类型') or '').strip() in PAYMENT_TYPES
        and str(row.get('款项归属指标类型') or '').strip() in INDICATOR_TYPES
        and str(row.get('回款季度') or '').strip() in QUARTERS
        and any(abs(v) > 0 for v in (amount, total_cost, actual))
    )


def is_valid_kpi_row(row):
    q1 = _num(row.get('第1季度净回款金额'))
    q2 = _num(row.get('第2季度净回款金额'))
    q3 = _num(row.get('第3季度净回款金额'))
    q4 = _num(row.get('第4季度净回款金额'))
    year = _num(row.get('全年净回款金额'))
    if None in (q1, q2, q3, q4, year):
        return False
    return (
        str(row.get('部门名称') or '').strip() == '东区一组'
        and _has_text(row.get('员工姓名'))
        and str(row.get('新老客户') or '').strip() in CUSTOMER_TYPES
        and year > 0
        and abs((q1 + q2 + q3 + q4) - year) <= 0.01
    )


VALIDATORS = {
    '合同管理-新签': is_valid_contract_row,
    '回款明细账': is_valid_payment_row,
    '目标数据': is_valid_kpi_row,
}


def rows_to_dicts(rows, validator=None):
    if not rows:
        return []
    header = rows[0]
    data = []
    for r in rows[1:]:
        if not any(str(x).strip() for x in r):
            continue
        item = {}
        for i, key in enumerate(header):
            if key is None:
                continue
            k = str(key).strip()
            if not k:
                continue
            item[k] = r[i] if i < len(r) else ''
        if validator and not validator(item):
            continue
        data.append(item)
    return data


def extract(path, out_dir, wanted):
    z = zipfile.ZipFile(path)
    names = _sheet_names(z)
    for idx, name in enumerate(names):
        if name not in wanted:
            continue
        rows = _read_sheet(z, idx)
        data = rows_to_dicts(rows, validator=VALIDATORS.get(name))
        out = Path(out_dir) / f'{name}.json'
        out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
        print('wrote', out, 'rows', len(data))


def resolve_input_path(*candidates):
    for candidate in candidates:
        path = Path(candidate)
        if path.exists():
            return path
    raise FileNotFoundError(f'No source file found in: {candidates}')


if __name__ == '__main__':
    base = Path('/Users/wang/Documents/codex/sales-performance-web/data')
    base.mkdir(parents=True, exist_ok=True)

    contract_xlsx = resolve_input_path(
        '/Users/wang/Downloads/销售合同及回款明细2026.xlsx',
        '/Users/wang/Library/Containers/com.kingsoft.wpsoffice.mac/Data/Library/Application Support/Kingsoft/WPS Cloud Files/userdata/qing/filecache/206442282/团队文档/2026年华东区销售工作/销售合同及回款明细2026.xlsx',
    )
    kpi_xlsx = resolve_input_path(
        '/Users/wang/Nutstore Files/.symlinks/坚果云/华东区管理工作/2026 年指标/2026 年东区一组王麒铭指标V3.xlsx',
        '/Users/wang/Library/Containers/com.tencent.WeWorkMac/Data/Documents/Profiles/41D14FCE88132F924052B032B1808C76/Caches/Files/2026-03/f7f91df218a7d949b065fb216544524c/2026 年东区一组王麒铭指标V3.xlsx',
    )

    extract(
        contract_xlsx,
        base,
        wanted={'合同管理-新签', '回款明细账'},
    )

    extract(
        kpi_xlsx,
        base,
        wanted={'目标数据'},
    )
