#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import csv
import json
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

BASE = 'http://127.0.0.1:8090'
OUT_JSON = Path('/Users/wang/Documents/codex/listed-supply-chain-mvp/data/auto_chain_name_industry_review.json')
OUT_CSV = Path('/Users/wang/Documents/codex/listed-supply-chain-mvp/data/auto_chain_name_industry_review.csv')

RAW_ITEMS = [
    ('德赛西威', '智能座舱/域控'),('华阳集团', '智能座舱/HUD/车载电子'),('均胜电子', '汽车电子/安全/座舱'),('航盛电子', '车载信息娱乐/座舱'),('继峰股份', '座椅/座舱部件与系统'),('中科创达', '智能座舱软件/OS/生态'),('东软集团', '汽车软件/座舱/车联网'),('博泰车联网', '车联网/座舱'),('亿咖通科技', '智能座舱平台'),('四维图新', '地图/定位/座舱生态'),('高德', '地图/定位/出行'),('百度 Apollo', '智能驾驶平台/定位/地图'),('华为智能汽车解决方案', '智能座舱/智驾/车控'),('中兴通讯车联网', 'V2X/车联网'),('星网宇达', '惯导/组合导航'),('北斗星通', '北斗/GNSS/定位'),('华测导航', 'GNSS/RTK/定位'),('千寻位置', '高精度定位服务'),('合众思壮', 'GNSS/北斗/定位'),('司南导航', 'GNSS/定位'),('华大北斗', '北斗芯片/定位'),('和芯星通', 'GNSS芯片/模块'),('移远通信', '车载模组/通信/定位'),('广和通', '车载通信模组'),('美格智能', '蜂窝/车载模组'),('中科星图', '时空大数据/定位相关'),('卡斯柯', '轨交/信号与定位相关技术'),('经纬恒润', '汽车电子/域控/软件'),('诚迈科技', '操作系统/座舱软件'),('普华基础软件', '车载基础软件/OS'),('中汽研', '测试认证/功能安全相关生态'),('地平线', '智驾芯片/域控生态'),('黑芝麻智能', '车规芯片/智驾'),('芯驰科技', '车规 SoC/座舱/网关'),('紫光展锐', '通信/车载连接相关'),('联发科', '车载座舱/连接 SoC'),('高通', '车载座舱/连接/平台'),('英伟达 NVIDIA', '智驾计算平台'),('Mobileye', 'ADAS/视觉方案'),('博世 Bosch', 'ADAS/底盘/传感器/ECU'),('大陆 Continental', 'ADAS/座舱/传感器'),('采埃孚 ZF', '底盘/智驾/转向'),('安波福 Aptiv', '车载架构/连接/域控'),('法雷奥 Valeo', 'ADAS 传感器/座舱'),('电装 DENSO', '汽车电子/控制器/传感器'),('松下车载', '车载电子/娱乐'),('哈曼 HARMAN', '车载娱乐/座舱'),('伟世通 Visteon', '座舱/域控'),('博泽 Brose', '车身电子/机电'),('海拉 HELLA', '照明/电子/传感器'),('纵目科技', 'ADAS/泊车'),('Momenta', '高阶辅助驾驶软件'),('小马智行', '自动驾驶'),('文远知行', '自动驾驶'),('轻舟智航', '自动驾驶'),('元戎启行', '自动驾驶'),('蘑菇车联', '车路协同/V2X'),('千方科技', '交通/车路协同/感知'),('赛目科技', '自动驾驶仿真/测试'),('禾多科技', '自动驾驶/智驾方案'),('佑驾创新', 'ADAS/智能驾驶'),('驭势科技', '自动驾驶/物流场景'),('仙途智能', '无人环卫/车路协同'),('图森未来', '干线自动驾驶'),('华域汽车', 'Tier1 集团，覆盖多类系统'),('延锋', '座舱系统/内饰与电子集成'),('联合汽车电子', '动力/电控/电子系统'),('伯特利', '制动/线控底盘与电子'),('拓普集团', '底盘/热管理/机电系统'),('保隆科技', 'TPMS/传感器/空气悬架等'),('德尔股份', '电泵/电控/机电系统'),('精进电动', '电驱/电控'),('汇川联合动力', '电驱/电控'),('英搏尔', '电机/电控'),('比亚迪半导体', '车规功率器件/芯片'),('斯达半导', '车规 IGBT/功率器件'),('士兰微', '功率器件/车规芯片'),('华润微', '功率器件/车规'),('中颖电子', 'MCU 等控制芯片相关'),('纳芯微', '车规模拟/隔离/传感接口'),('圣邦股份', '模拟芯片/车规方向'),('思特威', '图像传感器 CIS，车载方向'),('格科微', 'CIS/传感器'),('舜宇光学', '车载镜头/摄像头模组'),('联创电子', '车载镜头/光学'),('欧菲光', '摄像头模组/车载方向'),('大立科技', '红外热成像/感知'),('睿创微纳', '红外/热成像传感'),('速腾聚创', '激光雷达'),('禾赛科技', '激光雷达'),('图达通', '激光雷达'),('北醒科技', '激光雷达/测距'),('森思泰克', '毫米波雷达/传感'),('木牛科技', '定位/车载相关方向'),('中海达', 'GNSS/定位/测绘'),('北方导航', '惯导/导航相关'),('中国电科相关研究所体系', '雷达/导航/车载电子产业链'),('宁德时代', '智能电动核心：电池/系统集成'),('国轩高科', '动力电池/系统集成'),
]

KEYWORD_EXPECT = [
    (['半导体', '芯片', 'IGBT', 'CIS', 'SoC', 'MCU'], ['半导体', '芯片', '电子']),
    (['导航', 'GNSS', '北斗', '定位', '地图'], ['导航', '定位', '智能网联', '汽车供应链', '软件']),
    (['智驾', '自动驾驶', 'ADAS', '座舱', '车载', 'V2X'], ['汽车', '智能网联', '汽车供应链', '电子', '软件']),
    (['电池', '电驱', '电控', '功率器件'], ['新能源', '汽车', '电子', '半导体']),
]


def get_json(url, timeout=10):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read().decode('utf-8', 'ignore'))


def expected_tokens(tag: str):
    tag_l = tag.lower()
    out = set()
    for keys, mapped in KEYWORD_EXPECT:
        if any(k.lower() in tag_l for k in keys):
            out.update(mapped)
    if not out:
        out.update(['汽车', '电子', '软件'])
    return out


def verify_industry(industry_l1: str, industry_l2: str, tag: str):
    tks = expected_tokens(tag)
    text = f"{industry_l1} {industry_l2}".lower()
    hit = any(t.lower() in text for t in tks)
    return hit, sorted(tks)


def one(idx_alias_tag):
    idx, alias, tag = idx_alias_tag
    try:
        sug = get_json(f"{BASE}/api/suggest?q={urllib.parse.quote(alias)}", timeout=8)
        items = sug.get('items') or []
        full = (items[0].get('displayName') if items else alias) or alias
        enrich = get_json(f"{BASE}/api/enrich?q={urllib.parse.quote(full)}", timeout=14)
        c = enrich.get('company') or {}
        l1 = c.get('industryLevel1') or ''
        l2 = c.get('industryLevel2') or c.get('industryName') or ''
        hit, tks = verify_industry(l1, l2, tag)
        return {
            'idx': idx, 'alias': alias, 'input_tag': tag, 'full_name': full,
            'industry_l1': l1, 'industry_l2': l2, 'industry_match': 'YES' if hit else 'NO',
            'expected_tokens': '/'.join(tks), 'competitors': len(enrich.get('competitors') or []),
            'top5': len(enrich.get('top5') or []), 'suppliers': len(enrich.get('suppliers') or []),
            'customers': len(enrich.get('customers') or []),
            'suggest_preview': ' | '.join((x.get('displayName') or x.get('name') or '') for x in items[:3]),
        }
    except Exception as ex:
        return {
            'idx': idx, 'alias': alias, 'input_tag': tag, 'full_name': alias,
            'industry_l1': '', 'industry_l2': '', 'industry_match': 'ERROR', 'expected_tokens': '',
            'competitors': 0, 'top5': 0, 'suppliers': 0, 'customers': 0, 'suggest_preview': '', 'error': str(ex),
        }


def main():
    start = time.time()
    payload = [(i, a, t) for i, (a, t) in enumerate(RAW_ITEMS, start=1)]
    rows = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = [ex.submit(one, x) for x in payload]
        for i, f in enumerate(as_completed(futures), start=1):
            rows.append(f.result())
            if i % 20 == 0:
                print(f'done {i}')

    rows.sort(key=lambda x: x['idx'])
    summary = {
        'generatedAt': time.strftime('%Y-%m-%d %H:%M:%S'),
        'total': len(rows),
        'yes': sum(1 for r in rows if r['industry_match'] == 'YES'),
        'no': sum(1 for r in rows if r['industry_match'] == 'NO'),
        'error': sum(1 for r in rows if r['industry_match'] == 'ERROR'),
        'avg_competitors': round(sum(r['competitors'] for r in rows) / len(rows), 3) if rows else 0,
        'avg_top5': round(sum(r['top5'] for r in rows) / len(rows), 3) if rows else 0,
        'avg_suppliers': round(sum(r['suppliers'] for r in rows) / len(rows), 3) if rows else 0,
        'avg_customers': round(sum(r['customers'] for r in rows) / len(rows), 3) if rows else 0,
        'elapsedSec': round(time.time() - start, 3),
    }

    OUT_JSON.write_text(json.dumps({'summary': summary, 'rows': rows}, ensure_ascii=False, indent=2), encoding='utf-8')

    with OUT_CSV.open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                'idx','alias','input_tag','full_name','industry_l1','industry_l2','industry_match',
                'expected_tokens','competitors','top5','suppliers','customers','suggest_preview'
            ],
        )
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, '') for k in writer.fieldnames})

    print(json.dumps(summary, ensure_ascii=False))
    print(OUT_JSON)
    print(OUT_CSV)


if __name__ == '__main__':
    main()
