import json
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

NS = {'a': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}


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


def rows_to_dicts(rows):
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
        data.append(item)
    return data


def extract(path, out_dir, wanted):
    z = zipfile.ZipFile(path)
    names = _sheet_names(z)
    for idx, name in enumerate(names):
        if name not in wanted:
            continue
        rows = _read_sheet(z, idx)
        data = rows_to_dicts(rows)
        out = Path(out_dir) / f'{name}.json'
        out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
        print('wrote', out, 'rows', len(data))


if __name__ == '__main__':
    base = Path('/Users/wang/Documents/codex/sales-performance-web/data')
    base.mkdir(parents=True, exist_ok=True)

    extract(
        Path('/Users/wang/Downloads/销售合同及回款明细2026.xlsx'),
        base,
        wanted={'合同管理-新签', '回款明细账'},
    )

    extract(
        Path('/Users/wang/Nutstore Files/.symlinks/坚果云/华东区管理工作/2026 年指标/2026 年东区一组王麒铭指标V2.xlsx'),
        base,
        wanted={'目标数据'},
    )
