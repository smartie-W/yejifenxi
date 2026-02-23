import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { URL } from 'node:url';
import zlib from 'node:zlib';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

const ROOT = path.resolve('/Users/wang/Documents/codex/listed-supply-chain-mvp');
const PORT = Number(process.env.PORT || 8090);
const HOST = process.env.HOST || '0.0.0.0';
const execFileAsync = promisify(execFile);
const CACHE_TTL_MS = 10 * 60 * 1000;
const cache = new Map();
const perfStats = new Map();
const SEARCH_ENGINE_HOSTS = new Set([
  'www.baidu.com',
  'baidu.com',
  'cn.bing.com',
  'bing.com',
  'www.bing.com',
  'r.jina.ai',
  'aiqicha.baidu.com',
]);
function withTimeout(promise, ms, fallback) {
  return Promise.race([
    promise,
    new Promise((resolve) => setTimeout(() => resolve(fallback), ms)),
  ]);
}

function cacheGet(key) {
  const v = cache.get(key);
  if (!v) return null;
  if (Date.now() > v.expireAt) {
    cache.delete(key);
    return null;
  }
  return v.value;
}

function cacheSet(key, value, ttl = CACHE_TTL_MS) {
  cache.set(key, { value, expireAt: Date.now() + ttl });
}

function recordPerf(key, ms) {
  if (!Number.isFinite(ms) || ms <= 0) return;
  const prev = perfStats.get(key) || { avgMs: ms, count: 0 };
  const nextCount = prev.count + 1;
  const nextAvg = prev.count ? Math.round(prev.avgMs * 0.7 + ms * 0.3) : ms;
  perfStats.set(key, { avgMs: nextAvg, count: nextCount });
}

function etaMs(key, fallbackMs) {
  const v = perfStats.get(key);
  return Math.max(800, Math.round(v?.avgMs || fallbackMs));
}

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.pdf': 'application/pdf',
  '.svg': 'image/svg+xml',
};

const REGION_PREFIXES = [
  '中国',
  '北京',
  '上海',
  '天津',
  '重庆',
  '河北',
  '山西',
  '辽宁',
  '吉林',
  '黑龙江',
  '江苏',
  '浙江',
  '安徽',
  '福建',
  '江西',
  '山东',
  '河南',
  '湖北',
  '湖南',
  '广东',
  '海南',
  '四川',
  '贵州',
  '云南',
  '陕西',
  '甘肃',
  '青海',
  '台湾',
  '内蒙古',
  '广西',
  '西藏',
  '宁夏',
  '新疆',
  '深圳',
  '广州',
  '杭州',
  '南京',
  '苏州',
  '武汉',
  '成都',
];
const LEGAL_SUFFIXES = ['股份有限公司', '有限公司', '集团股份有限公司', '集团有限公司', '（集团）股份有限公司', '公司'];
const BUSINESS_NAME_TAILS = ['证券', '银行', '保险', '信托', '期货', '基金', '资本', '控股', '集团', '股份', '科技', '信息', '技术', '智能', '电子', '电气', '软件', '网络'];
const INTENT_TOKENS = ['证券', '银行', '保险', '信托', '期货', '基金', '交易所', '清算', '票据', '电网', '汽车', '医药', '军工'];
const SUPPLIER_HINT = /(科技|电子|半导体|芯片|材料|设备|软件|信息|系统|自动化|智能|工业|制造|机械|能源|电气)/;
const INDUSTRY_HINTS = [
  {
    name: '证券与期货',
    re: /(证券|期货|券商)/,
    upstream: ['信息技术', '交易系统', '风控系统', '数据服务'],
    downstream: ['机构客户', '个人投资者', '上市公司', '资管产品'],
  },
  {
    name: '汽车电子与智能网联',
    re: /(汽车|车载|座舱|行车|智联|出行|网联|自动驾驶)/,
    upstream: ['芯片', '传感器', '电子元件', '操作系统', '地图导航'],
    downstream: ['整车厂', '出行平台', '物流车队', '新能源车企'],
  },
  {
    name: '工业自动化设备',
    re: /(自动化|机器人|设备|机械)/,
    upstream: ['半导体', '电子元件', '传感器', '工业自动化', '电气设备'],
    downstream: ['电网', '电力', '能源', '制造', '轨道交通', '港口'],
  },
  {
    name: '软件与信息服务',
    re: /(软件|云|计算|信息|通信)/,
    upstream: ['服务器', '芯片', '网络设备', '数据库', '云服务'],
    downstream: ['金融', '政务', '制造', '零售', '医疗'],
  },
  {
    name: '新能源',
    re: /(新能源|电池|光伏|储能)/,
    upstream: ['锂矿', '铜箔', '电解液', '硅料', '逆变器'],
    downstream: ['车企', '电力', '储能', '电网', '海外能源'],
  },
  {
    name: '金融服务',
    re: /(保险|银行|金融)/,
    upstream: ['信息技术', '数据服务', '风控', '云服务'],
    downstream: ['制造', '互联网', '零售', '医疗', '政府'],
  },
  {
    name: '电网设备',
    re: /(电网|电气|输配电)/,
    upstream: ['铜材', '电气设备', '电子元件', '继电器', '工业自动化'],
    downstream: ['电网', '电力', '新能源', '轨道交通', '工业园区'],
  },
];
const CONSULTING_ORGS = ['麦肯锡', '波士顿咨询', '贝恩', '德勤', '普华永道', '艾瑞咨询', '赛迪顾问', '亿欧智库', 'IDC', 'Gartner', '弗若斯特沙利文'];
const INDUSTRY_HEAD_SEED_CODES = {
  '证券Ⅱ': ['600030', '601211', '601688', '000776', '600999', '601881', '601066', '601995', '600837'],
  '证券与期货': ['600030', '601211', '601688', '000776', '600999', '601881', '001236', '603093', '002961', '600927'],
  '金融服务': ['600030', '601211', '601688', '000776', '600999', '601881', '001236', '603093', '002961', '600927'],
  '软件与信息服务': ['600588', '600570', '002230', '002410', '300033', '688111', '600718', '688023'],
  '半导体EDA': ['301269', '688206', '301095', '688521', '688008'],
  '消费电子': ['002475', '601138', '002241', '300433', '603296'],
  '智能制造': ['300124', '002747', '300450', '300161', '688777'],
  '半导体芯片': ['603501', '688008', '600745', '300661', '688041'],
  '汽车供应链': ['002920', '601689', '600699', '601799', '603596'],
  '化学纤维': ['002064', '603225', '000420', '000949', '600810', '002254'],
};
const SOURCE_TIER_RANK = { tier1: 3, tier2: 2, tier3: 1 };
const INDUSTRY_TAXONOMY = [
  { l1: '电子信息', l2: '半导体EDA', re: /(EDA|半导体EDA|电子设计自动化|芯片设计平台|集成电路设计工具)/i, upstream: ['EDA工具链', 'IP库', '算力基础设施'], downstream: ['芯片设计公司', '晶圆厂', '封测厂'] },
  { l1: '电子信息', l2: '消费电子', re: /(消费电子|智能终端|电子制造|果链|手机零部件|可穿戴)/, upstream: ['芯片', '结构件', '显示模组'], downstream: ['终端品牌', '渠道商'] },
  { l1: '电子信息', l2: '半导体芯片', re: /(半导体|芯片|集成电路|存储|传感器|CMOS|晶圆|封测)/i, upstream: ['晶圆厂', '材料设备', 'EDA工具'], downstream: ['消费电子', '汽车电子', '工业电子'] },
  { l1: '工业', l2: '智能制造', re: /(智能制造|装备制造|工业机器人|高端装备|数字化工厂|工业自动化)/, upstream: ['伺服驱动', '传感器', '工控芯片'], downstream: ['制造业', '汽车', '能源'] },
  { l1: '汽车', l2: '汽车供应链', re: /(汽车供应链|汽车零部件|汽车电子|智能座舱|热管理|底盘|线束|车规)/, upstream: ['芯片', '传感器', '材料'], downstream: ['整车厂', '一级供应商'] },
  { l1: '金融', l2: '证券与期货', re: /(证券|期货|券商|资管|投行)/, upstream: ['信息技术', '交易系统', '风控系统'], downstream: ['机构客户', '个人投资者', '上市公司'] },
  { l1: '金融', l2: '银行', re: /(银行|农商行|城商行)/, upstream: ['金融IT', '支付清算', '风控系统'], downstream: ['企业客户', '个人客户'] },
  { l1: '金融', l2: '保险', re: /(保险|寿险|财险)/, upstream: ['精算系统', '渠道服务', '数据服务'], downstream: ['企业客户', '个人客户'] },
  { l1: '信息技术', l2: '软件开发', re: /(软件|SaaS|云平台|中间件|数据库|工业软件)/i, upstream: ['服务器', '芯片', '云基础设施'], downstream: ['政企客户', '金融', '制造'] },
  { l1: '信息技术', l2: 'IT服务', re: /(信息服务|IT服务|系统集成|运维|外包)/, upstream: ['服务器', '网络设备'], downstream: ['政企客户', '金融', '医疗'] },
  { l1: '工业', l2: '工业自动化', re: /(自动化|机器人|控制系统|工控)/, upstream: ['半导体', '电子元件', '传感器'], downstream: ['制造业', '能源', '电力'] },
  { l1: '能源电力', l2: '电网设备', re: /(电网|输配电|电气|变压器|开关设备)/, upstream: ['铜材', '电气元件'], downstream: ['电网公司', '发电集团'] },
  { l1: '汽车', l2: '智能网联', re: /(智能驾驶|车联网|座舱|汽车电子|自动驾驶)/, upstream: ['芯片', '传感器', '操作系统'], downstream: ['整车厂', '出行平台'] },
  { l1: '医疗健康', l2: '医疗器械与服务', re: /(医疗|医药|器械|生物科技|医院)/, upstream: ['原料药', '电子元件', '耗材'], downstream: ['医院', '患者'] },
  { l1: '材料', l2: '化学纤维', re: /(化学纤维|涤纶|锦纶|氨纶|腈纶|粘胶|纤维)/, upstream: ['石化原料', '助剂', '纺丝设备'], downstream: ['纺织服装', '汽车内饰', '工业材料'] },
];
const COMPANY_INDUSTRY_OVERRIDES = [
  { names: ['华大九天', '北京华大九天科技股份有限公司'], l1: '电子信息', l2: '半导体EDA' },
  { names: ['立讯精密', '立讯精密工业股份有限公司'], l1: '电子信息', l2: '消费电子' },
  { names: ['汇川技术', '深圳市汇川技术股份有限公司'], l1: '工业', l2: '智能制造' },
  { names: ['韦尔股份', '豪威集团', '豪威集成电路(集团)股份有限公司'], l1: '电子信息', l2: '半导体芯片' },
  { names: ['德赛西威', '惠州市德赛西威汽车电子股份有限公司'], l1: '汽车', l2: '汽车供应链' },
  {
    names: ['上海票据交易所股份有限公司', '郑州商品交易所', '广州期货交易所股份有限公司', '中国金融期货交易所'],
    l1: '服务业',
    l2: '交易所与清算基础设施',
  },
  {
    names: ['华泰期货有限公司', '广发期货有限公司', '中信期货有限公司', '南华期货股份有限公司', '瑞达期货股份有限公司'],
    l1: '服务业',
    l2: '期货业',
  },
  {
    names: ['深圳证券通信有限公司', '中汇信息技术（上海）有限公司', '银联智策顾问（上海）有限公司'],
    l1: '服务业',
    l2: '金融科技',
  },
];
const COMPANY_CODE_ALIASES = {
  '603501': ['韦尔股份', '豪威集团', '豪威集成电路'],
  '002920': ['德赛西威', '汽车电子'],
  '002475': ['立讯精密', '消费电子'],
  '300124': ['汇川技术', '工业自动化', '智能制造'],
};
const FINANCIAL_REVIEW_INDUSTRIES = new Set(['银行业', '证券业', '基金管理', '期货业', '交易所与清算基础设施', '金融科技']);
const FINANCIAL_PEER_LIBRARY = {
  银行业: [
    { name: '招商银行', code: '600036' },
    { name: '兴业银行', code: '601166' },
    { name: '中信银行', code: '601998' },
    { name: '平安银行', code: '000001' },
    { name: '宁波银行', code: '002142' },
  ],
  证券业: [
    { name: '中信证券', code: '600030' },
    { name: '华泰证券', code: '601688' },
    { name: '国泰海通', code: '601211' },
    { name: '中国银河', code: '601881' },
    { name: '中金公司', code: '601995' },
  ],
  基金管理: [
    { name: '易方达基金管理有限公司' },
    { name: '华夏基金管理有限公司' },
    { name: '广发基金管理有限公司' },
    { name: '招商基金管理有限公司' },
    { name: '中欧基金管理有限公司' },
  ],
  期货业: [
    { name: '永安期货股份有限公司', code: '600927' },
    { name: '南华期货股份有限公司', code: '603093' },
    { name: '瑞达期货股份有限公司', code: '002961' },
    { name: '中信期货有限公司' },
    { name: '国泰君安期货有限公司' },
  ],
  交易所与清算基础设施: [
    { name: '上海证券交易所' },
    { name: '深圳证券交易所' },
    { name: '中国金融期货交易所' },
    { name: '郑州商品交易所' },
    { name: '广州期货交易所股份有限公司' },
  ],
  金融科技: [
    { name: '恒生电子', code: '600570' },
    { name: '东方财富', code: '300059' },
    { name: '同花顺', code: '300033' },
    { name: '金证股份', code: '600446' },
    { name: '拉卡拉', code: '300773' },
  ],
};
const FINANCIAL_LINKAGE_LIBRARY = {
  银行业: {
    upstream: ['中国银联股份有限公司', '中国人民银行清算总中心', '腾讯云计算（北京）有限责任公司'],
    downstream: ['制造业企业客户', '零售个人客户', '普惠小微企业'],
  },
  证券业: {
    upstream: ['深圳证券交易所', '上海证券交易所', '中证信息技术服务有限责任公司'],
    downstream: ['上市公司投融资客户', '机构投资者', '个人投资者'],
  },
  基金管理: {
    upstream: ['托管银行', '券商交易通道', '基金估值与投研系统服务商'],
    downstream: ['机构LP与渠道银行', '个人投资者', '企业年金与养老金账户'],
  },
  期货业: {
    upstream: ['期货交易所', '中金所技术平台', '风险管理子公司'],
    downstream: ['产业套保客户', '量化与CTA机构', '个人投资者'],
  },
  交易所与清算基础设施: {
    upstream: ['行情与撮合系统供应商', '监管报送系统', '网络安全基础设施'],
    downstream: ['证券公司', '期货公司', '公募基金与资管机构'],
  },
  金融科技: {
    upstream: ['云计算与算力服务商', '安全与风控服务商', '数据服务商'],
    downstream: ['银行', '证券', '保险', '基金管理公司'],
  },
};
const SEMICON_REVIEW_INDUSTRIES = new Set(['半导体制造', '半导体芯片', '半导体EDA']);
const INDUSTRY_PEER_FALLBACK_LIBRARY = {
  快递物流: ['顺丰控股股份有限公司', '中通快递（开曼）有限公司', '圆通速递股份有限公司', '申通快递股份有限公司', '韵达控股集团股份有限公司', '极兔速递环球有限公司'],
  '供应链/仓储': ['中国外运股份有限公司', '中储发展股份有限公司', '厦门象屿集团有限公司', '厦门建发集团有限公司', '物产中大集团股份有限公司'],
  '电商/零售': ['阿里巴巴集团控股有限公司', '拼多多控股公司', '唯品会控股有限公司', '高鑫零售有限公司', '永辉超市股份有限公司'],
  '住宿和餐饮业': ['百胜中国控股有限公司', '九毛九国际控股有限公司', '呷哺呷哺餐饮管理有限公司', '广州酒家集团股份有限公司', '同庆楼餐饮股份有限公司'],
  '企业软件/SaaS': ['用友网络科技股份有限公司', '金山办公', '广联达科技股份有限公司', '浪潮软件股份有限公司', '鼎捷数智股份有限公司'],
  '半导体与芯片': ['中芯国际集成电路制造有限公司', '北方华创科技集团股份有限公司', '闻泰科技股份有限公司', '韦尔股份', '长电科技'],
  半导体设备: ['北方华创科技集团股份有限公司', '中微公司', '盛美上海', '华海清科', '芯源微'],
};
const SEMICON_LINKAGE_LIBRARY = {
  upstream: [
    '沪硅产业（硅片）',
    '安集科技（CMP抛光液）',
    '鼎龙股份（抛光垫/材料）',
    '江丰电子（靶材）',
    '雅克科技（前驱体/材料）',
  ],
  downstream: [
    '华为终端有限公司',
    '小米通讯技术有限公司',
    '比亚迪汽车工业有限公司',
    '上汽集团',
    '宁德时代新能源科技股份有限公司',
  ],
};

function buildIndustryPeerFallback(industryL2 = '', selfName = '', limit = 10) {
  const rows = Array.isArray(INDUSTRY_PEER_FALLBACK_LIBRARY[String(industryL2 || '').trim()])
    ? INDUSTRY_PEER_FALLBACK_LIBRARY[String(industryL2 || '').trim()]
    : [];
  return rows
    .filter((x) => x && !isSameEntityOrBrandFamily(selfName, x))
    .slice(0, limit)
    .map((name) =>
      evidenceRow(name, {
        reason: `行业同业库兜底：${industryL2}`,
        confidence: 0.64,
        sourceType: 'industry_peer_fallback',
        sourceTier: 'tier2',
      }),
    );
}

const localNamePool = loadJson(path.join(ROOT, 'data', 'customers_from_xlsx.json'), { customers: [] }).customers || [];
const localCompanies = loadJson(path.join(ROOT, 'data', 'companies.json'), []);
const INDUSTRY_KNOWLEDGE_PATH = path.join(ROOT, 'data', 'industry_knowledge.json');
const INDUSTRY_REVIEW_REPORT_PATH = path.join(ROOT, 'data', 'industry_review_report.json');
const DYNAMIC_COMPANY_INDUSTRY_OVERRIDES_PATH = path.join(ROOT, 'data', 'company_industry_overrides_dynamic.json');
const CHINA500_INDUSTRY_REVIEW_PATH = path.join(ROOT, 'data', 'china500_2025_industry_review.json');
const CHINA500_PEERS_PATH = path.join(ROOT, 'data', 'china500_2025_company_peers.json');
let industryKnowledge = loadJson(INDUSTRY_KNOWLEDGE_PATH, { updatedAt: '', industries: {} });
const SEMICON_TOP150_OVERRIDES = loadJson(path.join(ROOT, 'data', 'semiconductor_top150_overrides.json'), { rows: [] }).rows || [];
let dynamicCompanyIndustryOverrides = loadJson(DYNAMIC_COMPANY_INDUSTRY_OVERRIDES_PATH, { updatedAt: '', rows: [] }).rows || [];
const CHINA500_INDUSTRY_ROWS = loadJson(CHINA500_INDUSTRY_REVIEW_PATH, []);
const CHINA500_PEERS_RAW = loadJson(CHINA500_PEERS_PATH, {});
const CHINA500_INDEX = buildChina500Index(CHINA500_INDUSTRY_ROWS, CHINA500_PEERS_RAW);
{
  const seenLocal = new Set(localNamePool.map((x) => sanitizeLegalEntityName(x)).filter(Boolean));
  for (const r of CHINA500_INDUSTRY_ROWS) {
    const n = sanitizeLegalEntityName(r?.companyName || '');
    if (!n || seenLocal.has(n)) continue;
    seenLocal.add(n);
    localNamePool.push(n);
  }
}

function loadJson(file, fallback) {
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch {
    return fallback;
  }
}

function saveJson(file, data) {
  try {
    fs.writeFileSync(file, `${JSON.stringify(data, null, 2)}\n`, 'utf8');
  } catch {
    // ignore persistence failures in MVP mode
  }
}

function toNumberLoose(v) {
  const n = Number(String(v ?? '').replace(/,/g, '').trim());
  return Number.isFinite(n) ? n : 0;
}

function china500IdFromSourceLink(link = '') {
  const m = String(link || '').match(/(\d+)\.htm/i);
  return m ? m[1] : '';
}

function buildChina500Index(rows = [], peersRaw = {}) {
  const byName = new Map();
  const byIndustry = new Map();
  const idToName = new Map();
  for (const r of Array.isArray(rows) ? rows : []) {
    const name = sanitizeLegalEntityName(r?.companyName || '');
    if (!name) continue;
    const id = china500IdFromSourceLink(r?.sourceLink || '');
    if (id) idToName.set(id, name);
    const one = {
      rank: Number(r?.rank || 0) || 0,
      name,
      l1: String(r?.industryLevel1 || '').trim(),
      l2: String(r?.industryLevel2 || '').trim(),
      revenue: toNumberLoose(r?.revenue),
      sourceIndustry: String(r?.sourceIndustry || '').trim(),
      id,
    };
    byName.set(name, one);
    const key = one.l2 || '综合行业';
    if (!byIndustry.has(key)) byIndustry.set(key, []);
    byIndustry.get(key).push(one);
  }
  for (const arr of byIndustry.values()) arr.sort((a, b) => (a.rank || 9999) - (b.rank || 9999));

  const peersByName = new Map();
  for (const [id, names] of Object.entries(peersRaw || {})) {
    const mainName = idToName.get(String(id || '').trim());
    if (!mainName || !Array.isArray(names)) continue;
    const list = names
      .map((x) => sanitizeLegalEntityName(x))
      .filter(Boolean)
      .filter((x) => x !== mainName);
    if (list.length) peersByName.set(mainName, [...new Set(list)]);
  }
  return { byName, byIndustry, peersByName };
}

function findChina500ByName(name = '') {
  const q = sanitizeLegalEntityName(name);
  if (!q) return null;
  if (CHINA500_INDEX.byName.has(q)) return CHINA500_INDEX.byName.get(q);
  for (const [n, row] of CHINA500_INDEX.byName.entries()) {
    if (q.includes(n) || n.includes(q)) return row;
  }
  const qn = normalizeName(q);
  for (const [n, row] of CHINA500_INDEX.byName.entries()) {
    const nn = normalizeName(n);
    if (!nn) continue;
    if ((qn.length >= 4 && nn.includes(qn)) || (nn.length >= 4 && qn.includes(nn))) return row;
  }
  return null;
}

function industryL1ByL2(l2 = '') {
  const t = String(l2 || '').trim();
  if (!t) return '';
  const hit = INDUSTRY_TAXONOMY.find((x) => String(x?.l2 || '').trim() === t);
  return hit?.l1 || '';
}

function classifyIndustryByCompanyNameOnly(name = '') {
  const n = String(name || '').trim();
  if (!n) return { l1: '综合', l2: '综合行业' };
  if (/(银行|农商行|城商行|村镇银行)/.test(n)) return { l1: '服务业', l2: '银行业' };
  if (/(证券|券商)/.test(n)) return { l1: '服务业', l2: '证券业' };
  if (/(期货)/.test(n)) return { l1: '服务业', l2: '期货业' };
  if (/(保险|人寿|财险|再保险)/.test(n)) return { l1: '服务业', l2: '保险业' };
  if (/(基金管理|基金公司|公募基金|私募基金)/.test(n)) return { l1: '服务业', l2: '基金管理' };
  if (/(交易所|结算|清算|票据交易)/.test(n)) return { l1: '服务业', l2: '交易所与清算基础设施' };
  if (/(石油|天然气|中石油|中石化|中海油)/.test(n)) return { l1: '工业', l2: '石油和天然气开采' };
  if (/(电网|电力|华能|华电|大唐|能源投资|能源集团)/.test(n)) return { l1: '工业', l2: '电力生产与供应' };
  if (/(建筑|建工|中建|中铁|铁道|土木|工程集团)/.test(n)) return { l1: '建筑业', l2: '基础设施建设' };
  if (/(钢铁|冶金|钢联|鞍钢|宝武|沙钢|河钢)/.test(n)) return { l1: '工业', l2: '黑色金属冶炼及压延' };
  if (/(汽车|汽集团|一汽|东风|上汽|广汽|长城汽车|比亚迪)/.test(n)) return { l1: '工业', l2: '汽车制造' };
  if (/(通信|电信|联通|移动|铁塔)/.test(n)) return { l1: '服务业', l2: '电信运营' };
  if (/(华为|中兴通讯|联发科技|立讯精密|京东方|TCL)/.test(n)) return { l1: '工业', l2: '电子元件制造' };
  if (/(电子|半导体|芯片|集成电路|华创|存储|晶圆)/.test(n)) return { l1: '工业', l2: '半导体制造' };
  if (/(软件|信息技术|科技|云|数码|网络|智联|智策)/.test(n)) return { l1: '服务业', l2: '软件开发' };
  if (/(阿里巴巴|腾讯|百度|网易|快手|拼多多|京东|美团|贝壳|携程|唯品会)/.test(n)) return { l1: '服务业', l2: '互联网服务' };
  if (/(医药|生物|医院|医疗)/.test(n)) return { l1: '工业', l2: '医药制造' };
  if (/(快递|物流|供应链|航运|海运|港务|铁路)/.test(n)) return { l1: '服务业', l2: '物流仓储' };
  if (/(地产|置地|房地产|物业|万科|碧桂园|龙湖|绿地|世茂)/.test(n)) return { l1: '服务业', l2: '房地产开发' };
  if (/(零售|电商|京东|阿里巴巴|拼多多|美团|贝壳|永辉)/.test(n)) return { l1: '服务业', l2: '电商平台' };
  return { l1: '综合', l2: '综合行业' };
}

function extractCompanyNamesFromRawListText(raw = '') {
  const lines = String(raw || '').split(/\r?\n/).map((x) => x.trim()).filter(Boolean);
  const names = [];
  const seen = new Set();
  for (const ln of lines) {
    let s = ln
      .replace(/^\d+\s*[、.．)]\s*/g, '')
      .replace(/^\d+\s+/g, '')
      .replace(/\s+\d[\d,.]*\s*$/g, '')
      .trim();
    s = s.split(/\t/)[0].trim();
    if (!s) continue;
    if (s.length < 4 || s.length > 60) continue;
    if (!/[A-Za-z\u4e00-\u9fa5]/.test(s)) continue;
    if (!/(公司|集团|银行|证券|基金|期货|交易所|控股|企业|实业|科技|电网|电力|通信|保险|股份)/.test(s)) continue;
    if (seen.has(s)) continue;
    seen.add(s);
    names.push(s);
  }
  return names;
}

async function importIndustryOverridesFromCompanyList(raw = '', maxItems = 800) {
  const names = extractCompanyNamesFromRawListText(raw).slice(0, maxItems);
  const updatedAt = new Date().toISOString();
  const rows = [];
  for (const name of names) {
    const quick = classifyIndustryByCompanyNameOnly(name);
    const webL2 = await withTimeout(inferIndustryByWeb(name), 9000, '');
    const l2 = webL2 || quick.l2;
    const l1 = industryL1ByL2(l2) || quick.l1 || '综合';
    rows.push({ name, l1, l2, source: webL2 ? 'web+keyword' : 'keyword', updatedAt });
  }
  const byName = new Map();
  for (const r of dynamicCompanyIndustryOverrides) {
    if (r?.name) byName.set(String(r.name).trim(), r);
  }
  for (const r of rows) byName.set(String(r.name).trim(), r);
  dynamicCompanyIndustryOverrides = [...byName.values()];
  saveJson(DYNAMIC_COMPANY_INDUSTRY_OVERRIDES_PATH, { updatedAt, count: dynamicCompanyIndustryOverrides.length, rows: dynamicCompanyIndustryOverrides });
  return {
    imported: rows.length,
    total: dynamicCompanyIndustryOverrides.length,
    preview: rows.slice(0, 20),
  };
}

function escapeRegExp(s = '') {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function normalizeIndustryTaxonomy(rows = []) {
  return (Array.isArray(rows) ? rows : [])
    .map((x) => {
      const l1 = String(x?.l1 || '').trim();
      const l2 = String(x?.l2 || '').trim();
      if (!l1 || !l2) return null;
      const keywords = Array.isArray(x?.keywords) ? x.keywords.map((k) => String(k || '').trim()).filter(Boolean) : [];
      const re =
        x?.re instanceof RegExp
          ? x.re
          : keywords.length
            ? new RegExp(`(${keywords.map((k) => escapeRegExp(k)).join('|')})`, 'i')
            : new RegExp(`(${escapeRegExp(l2)})`, 'i');
      return {
        l1,
        l2,
        re,
        keywords,
        upstream: Array.isArray(x?.upstream) ? x.upstream : [],
        downstream: Array.isArray(x?.downstream) ? x.downstream : [],
      };
    })
    .filter(Boolean);
}

function patchIndustryConfigFromFiles() {
  const taxonomyRows = loadJson(path.join(ROOT, 'data', 'industry_taxonomy.json'), []);
  const normalizedTax = normalizeIndustryTaxonomy(taxonomyRows);
  if (normalizedTax.length) {
    INDUSTRY_TAXONOMY.splice(0, INDUSTRY_TAXONOMY.length, ...normalizedTax);
  }
  const externalSeeds = loadJson(path.join(ROOT, 'data', 'industry_seed_codes.json'), {});
  if (externalSeeds && typeof externalSeeds === 'object') {
    for (const [k, v] of Object.entries(externalSeeds)) {
      const key = String(k || '').trim();
      const list = Array.isArray(v) ? v.map((x) => String(x || '').replace(/\D/g, '')).filter((x) => /^\d{6}$/.test(x)) : [];
      if (!key) continue;
      INDUSTRY_HEAD_SEED_CODES[key] = Array.from(new Set([...(INDUSTRY_HEAD_SEED_CODES[key] || []), ...list]));
    }
  }
  for (const it of INDUSTRY_TAXONOMY) {
    if (!INDUSTRY_HEAD_SEED_CODES[it.l2]) INDUSTRY_HEAD_SEED_CODES[it.l2] = [];
  }
  if (!INDUSTRY_HEAD_SEED_CODES['证券Ⅱ'] && INDUSTRY_HEAD_SEED_CODES['证券业']) {
    INDUSTRY_HEAD_SEED_CODES['证券Ⅱ'] = [...INDUSTRY_HEAD_SEED_CODES['证券业']];
  }
  if (!INDUSTRY_HEAD_SEED_CODES['证券与期货'] && INDUSTRY_HEAD_SEED_CODES['证券业']) {
    INDUSTRY_HEAD_SEED_CODES['证券与期货'] = [...INDUSTRY_HEAD_SEED_CODES['证券业']];
  }
}

patchIndustryConfigFromFiles();

function normalizeName(name = '') {
  let x = String(name).trim();
  for (const p of REGION_PREFIXES) {
    if (x.startsWith(p)) x = x.slice(p.length);
  }
  for (const s of LEGAL_SUFFIXES) {
    if (x.endsWith(s)) x = x.slice(0, -s.length);
  }
  return x.replace(/[\s()（）-]/g, '').toLowerCase();
}

function coreCompanyName(name = '') {
  const raw = String(name || '').trim();
  let x = raw;
  for (const p of REGION_PREFIXES) {
    if (x.startsWith(p)) {
      x = x.slice(p.length);
      break;
    }
  }
  x = x.replace(/(有限责任公司|股份有限公司|集团股份有限公司|集团有限公司|有限公司|公司)$/g, '');
  return x.trim();
}

function queryRegionToken(text = '') {
  const raw = String(text || '').trim();
  if (!raw) return '';
  for (const p of REGION_PREFIXES) {
    if (raw.startsWith(p)) return p;
  }
  return '';
}

function regionMatchBoost(query, candidateName = '') {
  const token = queryRegionToken(query);
  if (!token) return 0;
  const n = String(candidateName || '').trim();
  if (!n) return -6;
  if (n.startsWith(token)) return 18;
  if (n.includes(token)) return 8;
  return -10;
}

function buildSuggestQueries(q) {
  const raw = String(q || '').trim();
  const queries = [raw];
  let trimmed0 = raw;
  for (const p of REGION_PREFIXES) {
    if (trimmed0.startsWith(p)) {
      trimmed0 = trimmed0.slice(p.length);
      break;
    }
  }
  const trimmed1 = trimmed0.replace(/(股份有限公司|有限公司|集团股份有限公司|集团有限公司|公司)$/g, '');
  const trimmed2 = trimmed1.replace(/(科技|智能|信息|技术|自动化|电子|制造|装备|股份)$/g, '');
  const trimmedFinance = trimmed1.replace(/(证券|银行|保险|信托|期货|基金|资本|控股|集团)$/g, '');
  const trimmed3 = raw.replace(/（.*?）|\(.*?\)/g, '');
  const normalized = normalizeName(raw);
  const normalizedShort = normalized.replace(/(科技|智能|信息|技术|自动化|电子|制造|装备)$/g, '');
  for (const item of [trimmed0, trimmed1, trimmed2, trimmedFinance, trimmed3, normalized, normalizedShort]) {
    const v = String(item || '').trim();
    if (v && !queries.includes(v)) queries.push(v);
  }
  return queries.filter(Boolean);
}

function sanitizeUrl(raw) {
  const s = String(raw || '').trim().replace(/[),.;]+$/g, '');
  if (!/^https?:\/\//i.test(s)) return '';
  try {
    const u = new URL(s);
    return u.toString();
  } catch {
    return '';
  }
}

function normalizeDomain(host = '') {
  return String(host || '').toLowerCase().replace(/^www\./, '');
}

function isSearchOrPortalDomain(host = '') {
  const h = normalizeDomain(host);
  return (
    SEARCH_ENGINE_HOSTS.has(h) ||
    h.endsWith('.baidu.com') ||
    h.endsWith('.bing.com') ||
    h.endsWith('.jin10.com') ||
    h.endsWith('.eastmoney.com')
  );
}

function overlapScore(a, b) {
  const A = normalizeName(a);
  const B = normalizeName(b);
  if (!A || !B) return 0;
  if (A === B) return 100;
  if (A.includes(B) || B.includes(A)) return 88;
  const setA = new Set([...A]);
  let common = 0;
  for (const ch of new Set([...B])) if (setA.has(ch)) common += 1;
  return Math.round((common / Math.max(setA.size, 1)) * 70);
}

function stripBusinessTailWords(name = '') {
  let x = String(name || '').trim();
  if (!x) return '';
  while (true) {
    const before = x;
    for (const t of BUSINESS_NAME_TAILS) {
      if (x.endsWith(t) && x.length > t.length + 1) {
        x = x.slice(0, -t.length);
        break;
      }
    }
    if (x === before) break;
  }
  return x.trim();
}

function matchKeys(name = '') {
  const raw = String(name || '').trim();
  const core = coreCompanyName(raw);
  const shortCore = stripBusinessTailWords(core);
  const arr = [raw, core, shortCore, normalizeName(raw)].map((x) => String(x || '').trim()).filter((x) => x.length >= 2);
  return [...new Set(arr)];
}

function overlapScoreEnhanced(a, b) {
  const A = matchKeys(a);
  const B = matchKeys(b);
  let best = 0;
  for (const x of A) {
    for (const y of B) {
      best = Math.max(best, overlapScore(x, y));
    }
  }
  return best;
}

function sourceTierRank(tier = 'tier3') {
  return SOURCE_TIER_RANK[String(tier || '').toLowerCase()] || 1;
}

function findIndustryOverrideByName(name = '') {
  const q = sanitizeLegalEntityName(String(name || '').trim());
  if (!q) return null;
  const c500 = findChina500ByName(q);
  if (c500?.l2) return { names: [c500.name], l1: c500.l1 || industryL1ByL2(c500.l2) || '综合', l2: c500.l2 };
  for (const ov of dynamicCompanyIndustryOverrides) {
    if (sanitizeLegalEntityName(ov?.name || '') === q) return { names: [ov.name], l1: ov.l1, l2: ov.l2 };
  }
  for (const ov of COMPANY_INDUSTRY_OVERRIDES) {
    const hit = (ov.names || []).some((n) => sanitizeLegalEntityName(n) === q);
    if (hit) return ov;
  }
  for (const ov of SEMICON_TOP150_OVERRIDES) {
    if (sanitizeLegalEntityName(ov?.name || '') === q) return { names: [ov.name], l1: ov.l1, l2: ov.l2 };
  }
  return null;
}

function evidenceRow(name, opts = {}) {
  return {
    ...opts,
    name: String(name || '').trim(),
    reason: opts.reason || '',
    confidence: Number.isFinite(opts.confidence) ? opts.confidence : 0.5,
    source: opts.source || '',
    sourceType: opts.sourceType || '',
    sourceTier: opts.sourceTier || 'tier3',
    evidenceDate: opts.evidenceDate || '',
    evidenceSnippet: opts.evidenceSnippet || '',
  };
}

function mergeEvidenceRows(rows = []) {
  const map = new Map();
  for (const r of rows) {
    const name = String(r?.name || '').trim();
    if (!name) continue;
    const key = normalizeName(name);
    const row = evidenceRow(name, r || {});
    if (!map.has(key)) {
      map.set(key, { ...row, _sources: new Set([`${row.sourceType}|${row.source}`]) });
      continue;
    }
    const prev = map.get(key);
    const curRank = sourceTierRank(row.sourceTier);
    const prevRank = sourceTierRank(prev.sourceTier);
    const better = curRank > prevRank || (curRank === prevRank && row.confidence > prev.confidence);
    const base = better ? row : prev;
    const merged = {
      ...prev,
      ...base,
      confidence: Math.max(prev.confidence || 0, row.confidence || 0),
      reason: better ? row.reason : prev.reason,
      source: better ? row.source : prev.source,
      sourceType: better ? row.sourceType : prev.sourceType,
      sourceTier: better ? row.sourceTier : prev.sourceTier,
      evidenceSnippet: better ? row.evidenceSnippet : prev.evidenceSnippet,
      evidenceDate: better ? row.evidenceDate : prev.evidenceDate,
    };
    merged._sources = prev._sources || new Set();
    merged._sources.add(`${row.sourceType}|${row.source}`);
    map.set(key, merged);
  }
  return [...map.values()].map((x) => ({ ...x, evidenceCount: (x._sources && x._sources.size) || 1 })).map((x) => {
    delete x._sources;
    return x;
  });
}

function filterByEvidenceTier(rows = []) {
  const merged = mergeEvidenceRows(rows);
  return merged.filter((x) => {
    const rank = sourceTierRank(x.sourceTier);
    if (rank >= 2) return true;
    return (x.evidenceCount || 1) >= 2 && (x.confidence || 0) >= 0.55;
  });
}

function classifyIndustryDetailed(input = '') {
  const text = String(input || '').replace(/\s+/g, ' ').trim().slice(0, 240);
  const plain = text.replace(/\s+/g, '');
  const n = normalizeName(text);
  const c500 = findChina500ByName(text);
  if (c500?.l2) {
    const item = INDUSTRY_TAXONOMY.find((x) => x.l2 === c500.l2);
    return {
      industryLevel1: c500.l1 || item?.l1 || '综合',
      industryLevel2: c500.l2,
      industryName: c500.l2,
      upstream: item?.upstream || ['原材料', '设备', '技术服务'],
      downstream: item?.downstream || ['企业客户', '渠道客户'],
    };
  }
  for (const ov of dynamicCompanyIndustryOverrides) {
    const key = normalizeName(ov?.name || '');
    if (!key) continue;
    if (n.includes(key) || key.includes(n)) {
      const item = INDUSTRY_TAXONOMY.find((x) => x.l2 === ov.l2);
      return {
        industryLevel1: ov.l1 || item?.l1 || '综合',
        industryLevel2: ov.l2 || '综合行业',
        industryName: ov.l2 || '综合行业',
        upstream: item?.upstream || ['原材料', '设备', '技术服务'],
        downstream: item?.downstream || ['企业客户', '渠道客户'],
      };
    }
  }
  for (const ov of COMPANY_INDUSTRY_OVERRIDES) {
    const hit = (ov.names || []).some((x) => normalizeName(x) && (n.includes(normalizeName(x)) || normalizeName(x).includes(n)));
    if (hit) {
      const item = INDUSTRY_TAXONOMY.find((x) => x.l2 === ov.l2);
      return {
        industryLevel1: ov.l1,
        industryLevel2: ov.l2,
        industryName: ov.l2,
        upstream: item?.upstream || ['EDA工具链', 'IP库', '算力基础设施'],
        downstream: item?.downstream || ['芯片设计公司', '晶圆厂', '封测厂'],
      };
    }
  }
  for (const ov of SEMICON_TOP150_OVERRIDES) {
    const key = normalizeName(ov?.name || '');
    if (!key) continue;
    if (n.includes(key) || key.includes(n)) {
      const item = INDUSTRY_TAXONOMY.find((x) => x.l2 === ov.l2);
      return {
        industryLevel1: ov.l1 || item?.l1 || '工业',
        industryLevel2: ov.l2 || '半导体制造',
        industryName: ov.l2 || '半导体制造',
        upstream: item?.upstream || ['硅片与材料', '半导体设备', 'EDA与IP'],
        downstream: item?.downstream || ['消费电子', '汽车电子', '工业控制'],
      };
    }
  }
  let best = null;
  let bestScore = 0;
  for (const it of INDUSTRY_TAXONOMY) {
    let score = 0;
    const hits = text.match(it.re);
    if (hits) score += 5;
    if (plain.includes(it.l2)) score += 6;
    const kws = Array.isArray(it.keywords) ? it.keywords : [];
    for (const kw of kws) {
      if (kw && plain.includes(String(kw).replace(/\s+/g, ''))) score += 2;
    }
    if (/Ⅱ|I|行业/.test(text) && it.re.test(text)) score += 2;
    if (score > bestScore) {
      best = it;
      bestScore = score;
    }
  }
  if (!best) {
    return {
      industryLevel1: '综合',
      industryLevel2: '综合行业',
      industryName: '综合行业',
      upstream: ['半导体', '电子元件', '工业软件', '材料'],
      downstream: ['制造', '能源', '金融', '医疗'],
    };
  }
  return {
    industryLevel1: best.l1,
    industryLevel2: best.l2,
    industryName: best.l2,
    upstream: best.upstream || [],
    downstream: best.downstream || [],
  };
}

function extractIntentToken(query = '') {
  const q = String(query || '');
  for (const t of INTENT_TOKENS) {
    if (q.includes(t)) return t;
  }
  return '';
}

function isFinancialIntentToken(token = '') {
  return ['证券', '银行', '保险', '信托', '期货', '基金', '交易所', '清算', '票据'].includes(String(token || ''));
}

function candidateIntentHit(query, shortName = '', fullName = '') {
  const token = extractIntentToken(query);
  if (!token) return true;
  return String(shortName || '').includes(token) || String(fullName || '').includes(token);
}

function candidateMatchScore(query, shortName = '', fullName = '') {
  let s = overlapScoreEnhanced(query, shortName || fullName || '');
  if (fullName) s = Math.max(s, overlapScoreEnhanced(query, fullName));
  const token = extractIntentToken(query);
  if (token) {
    s += candidateIntentHit(query, shortName, fullName) ? 24 : -20;
  }
  return s;
}

function synthesizeLegalNameCandidates(query = '') {
  const q = String(query || '').trim();
  if (!q) return [];
  if (looksLikeLegalEntityName(q)) return [q];
  const out = [];
  const base = q.replace(/(股份|集团)$/g, '').trim();
  for (const x of [q, base]) {
    const v = String(x || '').trim();
    if (!v || v.length < 2) continue;
    if (!out.includes(v)) out.push(v);
    const c1 = `${v}有限公司`;
    const c2 = `${v}股份有限公司`;
    if (!out.includes(c1)) out.push(c1);
    if (!out.includes(c2)) out.push(c2);
  }
  return out;
}

function aliasesByCode(code = '') {
  const c = String(code || '').replace(/\D/g, '');
  return COMPANY_CODE_ALIASES[c] || [];
}

function mapSecId(code) {
  if (!code) return '';
  if (String(code).includes('.')) {
    const [c, ex] = String(code).split('.');
    return ex === 'SH' ? `1.${c}` : `0.${c}`;
  }
  const c = String(code);
  return /^(6|9)/.test(c) ? `1.${c}` : `0.${c}`;
}

async function fetchText(url) {
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), 12000);
  try {
    const r = await fetch(url, { signal: ctl.signal, headers: { 'user-agent': 'Mozilla/5.0' } });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const ab = await r.arrayBuffer();
    let buf = Buffer.from(ab);
    const encoding = String(r.headers.get('content-encoding') || '').toLowerCase();
    try {
      if (encoding.includes('gzip')) buf = zlib.gunzipSync(buf);
      else if (encoding.includes('deflate')) buf = zlib.inflateSync(buf);
      else if (encoding.includes('br')) buf = zlib.brotliDecompressSync(buf);
      else if (buf.length > 2 && buf[0] === 0x1f && buf[1] === 0x8b) buf = zlib.gunzipSync(buf);
    } catch {
      // fall through and decode raw bytes
    }
    return buf.toString('utf8');
  } finally {
    clearTimeout(t);
  }
}

async function fetchTextWithEncoding(url, encoding = 'utf-8') {
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), 12000);
  try {
    const r = await fetch(url, { signal: ctl.signal, headers: { 'user-agent': 'Mozilla/5.0' } });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const ab = await r.arrayBuffer();
    return new TextDecoder(encoding).decode(ab);
  } finally {
    clearTimeout(t);
  }
}

function pickInvestorsFromText(text = '') {
  const t = String(text || '');
  const investorMatch =
    t.match(/(?:投资方(?:包括|为)?|领投方(?:为)?|由)([^。；\n]{2,120})/) ||
    t.match(/(?:参与投资(?:的机构)?(?:包括|为)?)([^。；\n]{2,120})/);
  if (!investorMatch) return [];
  const raw = investorMatch[1]
    .replace(/等一众知名机构.*/g, '')
    .replace(/等机构.*/g, '')
    .replace(/[“”"'（）()]/g, ' ');
  return raw
    .split(/[、,，及和与]/)
    .map((x) =>
      x
        .replace(/\*+/g, '')
        .replace(/[‌_]/g, '')
        .replace(/^\W+|\W+$/g, '')
        .replace(/(领投|跟投|参投|投资|旗下|方面|基金|资本)$/g, '')
        .replace(/^(获得|其中|包括|为|由)\s*/g, '')
        .trim(),
    )
    .filter((x) => x.length >= 2 && x.length <= 30)
    .filter(
      (x) =>
        !/(百度|搜索|公开资料|主要融资|投资方|融资|轮次|涵盖|一众|知名机构|金额|估值|如下|包括以下|官网)/.test(x) &&
        !/(上一轮|基本一致|公司地址|查看地图|品牌介绍|序号|发布日期|公开资料显示|独立分拆|历史融资如下)/.test(x) &&
        /[A-Za-z\u4e00-\u9fa5]/.test(x),
    )
    .slice(0, 8);
}

async function fetchNonListedFinancing(companyName, limit = 5) {
  const name = String(companyName || '').trim();
  if (!name || !looksLikeLegalEntityName(name)) return { roundsCount: null, events: [], source: '' };
  const key = `financing:${name}`;
  const cached = cacheGet(key);
  if (cached) return cached;
  const query = `${name} 融资 轮次 投资方`;
  const source = `https://r.jina.ai/http://www.baidu.com/s?wd=${encodeURIComponent(query)}`;
  try {
    const text = await fetchText(source);
    const core = coreCompanyName(name);
    const coreToken = core.slice(0, Math.min(core.length, 4));
    const fullMention = String(text || '').includes(name);
    const coreMentionCount = coreToken
      ? (String(text || '').match(new RegExp(coreToken.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'g')) || []).length
      : 0;
    // If source text does not clearly refer to this company, skip financing to avoid cross-company contamination.
    if (!fullMention && coreMentionCount < 3) {
      const out = { roundsCount: null, events: [], source };
      cacheSet(key, out, 10 * 60 * 1000);
      return out;
    }
    const lines = String(text || '')
      .split('\n')
      .map((x) => x.trim())
      .filter(Boolean);
    const events = [];
    const seen = new Set();
    let roundsCount = null;

    for (const ln of lines) {
      if (!/(融资|投资方|领投|跟投|估值)/.test(ln)) continue;
      const strongCompanyHit = (name && ln.includes(name)) || (coreToken && ln.includes(coreToken));
      if (!strongCompanyHit && !/(A轮|B轮|C轮|D轮|E轮|天使轮|Pre-IPO|战略融资)/.test(ln)) continue;
      if (/(序号|发布日期|查看地图|公司地址|品牌介绍|融资信息\s*\d+)/.test(ln)) continue;
      const roundsHit = ln.match(/(?:共获|完成|累计完成)\s*(\d{1,2})\s*轮融资/);
      if (roundsHit) roundsCount = Number(roundsHit[1]);
      const roundMatch = ln.match(/(天使轮|种子轮|Pre-A轮|A\+?轮|B\+?轮|C\+?轮|D\+?轮|E\+?轮|F\+?轮|Pre-IPO|IPO|战略融资)/i);
      const amountMatch = ln.match(/(?:融资额|融资|增资|投后估值|估值)[^。；\n]{0,16}?((?:\d+(?:\.\d+)?)\s*(?:亿|万)?\s*(?:人民币|美元|元))/);
      const investors = pickInvestorsFromText(ln);
      const dateMatch = ln.match(/(20\d{2}年\d{1,2}月\d{1,2}日)/);
      if (!roundMatch && !amountMatch && !investors.length) continue;
      if (!strongCompanyHit && !(roundMatch && amountMatch)) continue;
      if (!roundMatch && !amountMatch && investors.length < 2) continue;
      const item = {
        date: dateMatch ? dateMatch[1] : '',
        round: roundMatch ? roundMatch[1] : '',
        amount: amountMatch ? amountMatch[1] : '',
        investors,
        sourceSnippet: ln.slice(0, 220),
      };
      if (!item.round && !item.amount && !item.date) continue;
      const sig = `${item.date}|${item.round}|${item.amount}|${item.investors.join(',')}`;
      if (seen.has(sig)) continue;
      seen.add(sig);
      events.push(item);
      if (events.length >= limit) break;
    }

    if (!Number.isFinite(roundsCount) && events.length) {
      const roundSet = new Set(events.map((x) => x.round).filter(Boolean));
      roundsCount = Math.max(roundSet.size, events.length);
    }
    const out = { roundsCount, events, source };
    cacheSet(key, out, 30 * 60 * 1000);
    return out;
  } catch {
    const out = { roundsCount: null, events: [], source };
    cacheSet(key, out, 5 * 60 * 1000);
    return out;
  }
}

function isAStockCode(code) {
  return /^(00|30|60|68)\d{4}$/.test(String(code || ''));
}

function isCompanyLikeName(name) {
  return !/(ETF|LOF|联接|指数|主题|基金|REIT|增强|A$|C$|B$|I$)/i.test(String(name || ''));
}

function looksLikeLegalEntityName(name) {
  const n = String(name || '').trim();
  if (!n || n.length < 6) return false;
  // Only keep legal full names in suggestion list.
  if (/(有限责任公司|股份有限公司|集团有限公司|集团股份有限公司|有限公司|研究院|中心|事务所|集团公司|总公司|分公司|交易所)$/.test(n)) return true;
  return false;
}

function isBranchEntityName(name = '') {
  const n = String(name || '').trim();
  return /(分公司|子公司|分店|营业部|办事处)$/.test(n);
}

function hasBranchIntent(query = '') {
  return /(分公司|子公司|分店|营业部|办事处)/.test(String(query || ''));
}

function sanitizeLegalEntityName(name) {
  let n = String(name || '').trim();
  n = n.replace(/^[“"'`【\[(\s]+/g, '').replace(/[”"'`】\])\s]+$/g, '').trim();
  n = n.replace(/^[让将由在于从对把给请]\s*/g, '').trim();
  n = n.replace(/\s+/g, '');
  return n;
}

function isGenericLegalName(name) {
  const n = String(name || '').trim();
  if (!looksLikeLegalEntityName(n)) return false;
  if (/股份公司$/.test(n) && !/股份有限公司$/.test(n)) return true;
  if (/(是正规公司|正规吗|怎么样|靠谱吗|哪家好|开户|手续费|电话|官网|地址|招聘|排名)/.test(n)) return true;
  if (/[?？!！]/.test(n)) return true;
  const core = coreCompanyName(n);
  if (!core || core.length < 3) return true;
  if (/(正规|靠谱|最好|推荐|开户|手续费|官网|电话)/.test(core)) return true;
  if (/^(科技|信息|技术|电子|软件|网络|自动化|智能|实业|贸易|发展|控股|集团)+$/.test(core)) return true;
  if (/^[A-Za-z]+$/.test(core)) return true;
  return false;
}

function hasStrongCoreMatch(queryName, candidateName) {
  const qCore = coreCompanyName(queryName);
  const cCore = coreCompanyName(candidateName);
  if (!qCore || !cCore) return false;
  if (qCore === cCore) return true;
  if (qCore.includes(cCore) || cCore.includes(qCore)) return true;
  const qToken = qCore.slice(0, Math.min(qCore.length, 4));
  if (qToken && cCore.includes(qToken)) return true;
  return overlapScore(qCore, cCore) >= 82;
}

function stripLegalTail(name = '') {
  return sanitizeLegalEntityName(name).replace(/(有限责任公司|股份有限公司|集团股份有限公司|集团有限公司|有限公司|总公司|分公司|公司)$/g, '');
}

function hasStrictLegalNameMatch(queryName, candidateName) {
  const qRaw = sanitizeLegalEntityName(queryName);
  const cRaw = sanitizeLegalEntityName(candidateName);
  if (!qRaw || !cRaw) return false;
  if (qRaw === cRaw) return true;
  const qBase = stripLegalTail(qRaw);
  const cBase = stripLegalTail(cRaw);
  if (qBase && cBase && qBase === cBase) return true;
  const rawScore = overlapScoreEnhanced(qRaw, cRaw);
  const baseScore = overlapScoreEnhanced(qBase || qRaw, cBase || cRaw);
  const lcp = longestCommonPrefixLen(qBase || qRaw, cBase || cRaw);
  return (rawScore >= 88 || baseScore >= 86) && lcp >= 4;
}

function buildSuggestQueriesForApi(q) {
  const raw = String(q || '').trim();
  if (!raw) return [];
  const base = buildSuggestQueries(raw);
  if (!looksLikeLegalEntityName(raw)) return base;

  let noRegion = raw;
  for (const p of REGION_PREFIXES) {
    if (noRegion.startsWith(p)) {
      noRegion = noRegion.slice(p.length);
      break;
    }
  }
  const noSuffix = noRegion.replace(/(有限责任公司|股份有限公司|集团股份有限公司|集团有限公司|有限公司|公司)$/g, '');
  const noBracket = raw.replace(/（.*?）|\(.*?\)/g, '');
  const out = [];
  for (const item of [raw, noRegion, noSuffix, noBracket]) {
    const v = String(item || '').trim();
    if (v && !out.includes(v)) out.push(v);
  }
  return out;
}

function shouldUseStrictSuggestMatch(q) {
  const raw = String(q || '').trim();
  if (!raw) return false;
  if (looksLikeLegalEntityName(raw)) return true;
  if (/(交易所|清算|票据交易)/.test(raw) && core.length >= 3) return true;
  const core = coreCompanyName(raw);
  if (core.length >= 4 && /(科技|信息|技术|电子|电气|软件|智能|网络|自动化|装备|制造|集团|公司|股份)/.test(raw)) return true;
  for (const p of REGION_PREFIXES) {
    if (raw.startsWith(p) && core.length >= 3) return true;
  }
  return false;
}

function parseMaybeJsonp(text) {
  const raw = String(text || '').trim();
  if (!raw) return null;
  if (raw.startsWith('{') || raw.startsWith('[')) {
    try {
      return JSON.parse(raw);
    } catch {
      return null;
    }
  }
  const l = raw.indexOf('(');
  const r = raw.lastIndexOf(')');
  if (l >= 0 && r > l) {
    const inner = raw.slice(l + 1, r);
    try {
      return JSON.parse(inner);
    } catch {
      return null;
    }
  }
  return null;
}

async function eastmoneySuggest(q, count = 12) {
  const url = `https://searchapi.eastmoney.com/api/suggest/get?input=${encodeURIComponent(q)}&type=14&token=D43BF722C8E33BDC906FB84D85E326E8&count=${count}`;
  try {
    const text = await fetchText(url);
    const data = parseMaybeJsonp(text) || {};
    const rows = data?.QuotationCodeTable?.Data || data?.data || [];
    return rows
      .map((x) => ({
        code: x.Code || x.SECURITY_CODE || x.SecurityCode || '',
        name: x.Name || x.SECURITY_NAME_ABBR || x.SecurityName || '',
        market: x.MktNum || x.Market || '',
        secid: x.SecID || x.SecId || '',
      }))
      .filter((x) => x.code && x.name)
      .map((x) => ({ ...x, secid: x.secid || mapSecId(x.code) }));
  } catch {
    return [];
  }
}

async function baiduSuggest(query, limit = 12) {
  const q = String(query || '').trim();
  if (!q) return [];
  const key = `baiduSuggest:${q}:${limit}`;
  const cached = cacheGet(key);
  if (cached) return cached;
  const url = `https://www.baidu.com/sugrec?prod=pc&wd=${encodeURIComponent(q)}`;
  try {
    const text = await fetchText(url);
    const data = JSON.parse(text || '{}');
    const rows = Array.isArray(data.g) ? data.g : [];
    const out = rows
      .map((x) => String(x.q || '').trim())
      .filter(Boolean)
      .slice(0, limit);
    cacheSet(key, out, 10 * 60 * 1000);
    return out;
  } catch {
    return [];
  }
}

async function fetchMirrorSearchText(query) {
  const q = String(query || '').trim();
  if (!q) return '';
  const key = `mirrorSearch:${q}`;
  const cached = cacheGet(key);
  if (cached) return cached;
  const urls = [
    `https://r.jina.ai/http://www.baidu.com/s?wd=${encodeURIComponent(q)}`,
    `https://r.jina.ai/http://cn.bing.com/search?q=${encodeURIComponent(q)}`,
  ];
  for (const url of urls) {
    try {
      const txt = await withTimeout(fetchText(url), 10000, '');
      const bad = /(百度安全验证|网络不给力|请稍后重试|验证码|拒绝访问)/.test(String(txt || ''));
      if (txt && !bad) {
        cacheSet(key, txt, 20 * 60 * 1000);
        return txt;
      }
    } catch {
      // continue with next engine
    }
  }
  return '';
}

function extractLegalNamesFromTexts(texts = [], limit = 20) {
  const found = [];
  const seen = new Set();
  const re = /([\u4e00-\u9fa5A-Za-z0-9（）()·\-]{4,}(有限责任公司|股份有限公司|集团有限公司|集团股份有限公司|有限公司|公司))/g;
  for (const t of texts) {
    const s = String(t || '');
    re.lastIndex = 0;
    let m;
    while ((m = re.exec(s))) {
      const name = sanitizeLegalEntityName(String(m[1] || '').trim());
      if (!looksLikeLegalEntityName(name) || seen.has(name)) continue;
      seen.add(name);
      found.push(name);
      if (found.length >= limit) return found;
    }
  }
  return found;
}

async function onlineLegalNameSuggest(q, limit = 8) {
  const raw = String(q || '').trim();
  if (!raw) return [];
  const [web, mirror] = await Promise.all([baiduSuggest(raw, 12), fetchMirrorSearchText(raw)]);
  const names = extractLegalNamesFromTexts([raw, ...web, mirror], limit);
  return names.map((name) => ({ code: '', name, secid: '' }));
}

async function discoverOfficialWebsite(companyName) {
  const q = String(companyName || '').trim();
  if (!q) return '';
  const key = `officialSite:${q}`;
  const cached = cacheGet(key);
  if (cached) return cached;

  const searchUrl = `https://r.jina.ai/http://cn.bing.com/search?q=${encodeURIComponent(`${q} 官网`)}`;
  try {
    const text = await fetchText(searchUrl);
    const lines = splitUsefulLines(text);
    const urls = [...String(text || '').matchAll(/https?:\/\/[^\s)\]]+/g)].map((m) => sanitizeUrl(m[0])).filter(Boolean);
    const emailDomains = [...String(text || '').matchAll(/[A-Za-z0-9._%+-]+@([A-Za-z0-9.-]+\.[A-Za-z]{2,})/g)].map((m) => normalizeDomain(m[1]));
    const core = coreCompanyName(q);
    const core2 = core.slice(0, Math.min(core.length, 4));

    const scored = [];
    for (const u of urls) {
      try {
        const o = new URL(u);
        const host = normalizeDomain(o.hostname);
        if (!host || isSearchOrPortalDomain(host)) continue;
        let score = 1;
        if (o.pathname === '/' || o.pathname === '') score += 3;
        if (/company|about|home|index|main/i.test(o.pathname)) score += 1;
        if (emailDomains.includes(host)) score += 5;
        if (/\.(com|cn)$/.test(host)) score += 1;
        if (host.includes('autoai') && /(四维智联|AUTOAI)/i.test(q)) score += 3;
        const contextHit = lines.some((ln) => ln.includes(u) && (ln.includes(q) || (core2 && ln.includes(core2))));
        if (contextHit) score += 4;
        if (core2 && host.includes(core2.toLowerCase())) score += 2;
        scored.push({ url: `${o.protocol}//${o.host}/`, score });
      } catch {
        // ignore invalid urls
      }
    }
    scored.sort((a, b) => b.score - a.score);
    let best = '';
    for (const cand of scored.slice(0, 6)) {
      if (cand.score < 6) continue;
      if (await verifyOfficialWebsiteForCompany(cand.url, q)) {
        best = cand.url;
        break;
      }
    }
    cacheSet(key, best, 60 * 60 * 1000);
    return best;
  } catch {
    return '';
  }
}

async function fetchSiteText(url) {
  const raw = String(url || '').trim();
  if (!raw) return '';
  try {
    const mirrored = `https://r.jina.ai/http://${raw.replace(/^https?:\/\//i, '')}`;
    return await withTimeout(fetchText(mirrored), 9000, '');
  } catch {
    return '';
  }
}

function splitUsefulLines(text) {
  return String(text || '')
    .split('\n')
    .map((x) => x.replace(/[*#>\-`_]/g, ' ').trim())
    .filter(Boolean)
    .filter((x) => x.length >= 4 && x.length <= 300);
}

function cleanExtractedOrgName(name = '') {
  let n = String(name || '').trim();
  n = n
    .replace(/^[（(【\[\s]+/g, '')
    .replace(/^[)：:、,，;；.\-]+/g, '')
    .replace(/^(是由|由|与|和|及|包括|包含|例如|其中|其中包括|项目名称|客户为|合作方为|对|对外|等)\s*/g, '')
    .replace(/^(一家|一个)\s*/g, '')
    .replace(/[）)\]】\s]+$/g, '')
    .replace(/(等机构|等公司|等一众知名机构|等)\s*$/g, '')
    .trim();
  if (n.length < 3 || n.length > 40) return '';
  if (!/[A-Za-z\u4e00-\u9fa5]/.test(n)) return '';
  if (/(有限公司有限公司|公司公司|融资|投资方|公开资料|百度|搜索|官网|详情|项目|信息|查询|涵盖|招标与|相关企业|包括以下|以及其)/.test(n)) return '';
  return n;
}

function extractEntityAliasesFromLine(line = '', selfName = '') {
  const ln = String(line || '').trim();
  if (!ln) return [];
  const out = new Set();
  const self = String(selfName || '').trim();
  const re =
    /([\u4e00-\u9fa5A-Za-z0-9]{2,20}(?:集团|银行|证券|电网|电力|移动|联通|电信|汽车|科技|电子|半导体|通信|能源|航空|航天|船舶|石化|石油|钢铁|矿业|医院|大学|研究院|药业|药业集团|药业股份|实业|股份))/g;
  let m;
  while ((m = re.exec(ln))) {
    const token = cleanExtractedOrgName(m[1] || '');
    if (!token) continue;
    if (token.length < 2 || token.length > 24) continue;
    if (/^(行业|市场|客户|供应商|合作伙伴|案例|项目|解决方案|公司|集团|股份)$/.test(token)) continue;
    if (isSameEntityOrBrandFamily(self, token)) continue;
    out.add(token);
  }
  return [...out];
}

function isValidRelationEntityName(name = '', selfName = '') {
  const n = String(name || '').trim();
  if (!looksLikeLegalEntityName(n)) return false;
  if (isBranchEntityName(n)) return false;
  if (isSameEntityOrBrandFamily(selfName, n)) return false;
  if (isLikelyNearNameVariant(selfName, n)) return false;
  const core = coreCompanyName(n);
  if (
    core.length < 4 &&
    !/^(中国|国家|上海|北京|深圳|广州|天津|重庆|江苏|浙江|山东|福建|湖北|湖南|四川|河南|河北|陕西|山西|辽宁|吉林|黑龙江|江西|安徽|广西|云南|贵州|内蒙古|宁夏|青海|甘肃|新疆|西藏)/.test(
      n,
    )
  ) {
    return false;
  }
  if (/^(集团有限公司|有限公司|股份有限公司|科技有限公司|信息技术有限公司|计算机科技有限公司)$/.test(n)) return false;
  if (/^(投资|参股|收购|并购|合作|签约|服务|其在|其与|其中|以及|包括|涉及|在|对|向)/.test(n)) return false;
  if (/(客户|供应商|合作伙伴|案例|项目|融资|投资方|公开资料|摘要|公告|来源|新闻|报道)/.test(n)) return false;
  if (/的/.test(n) && !/(公司|集团|银行|医院|大学|学院|研究院|中心|事务所)$/.test(n)) return false;
  return true;
}

async function verifyOfficialWebsiteForCompany(site, companyName) {
  const s = String(site || '').trim();
  const q = String(companyName || '').trim();
  if (!s || !q) return false;
  const txt = await fetchSiteText(s);
  if (!txt) return false;
  const core = coreCompanyName(q);
  const hasFull = txt.includes(q);
  const hasCore = core && txt.includes(core);
  const hasOfficialMark = /(公司简介|联系我们|版权所有|copyright|关于我们)/i.test(txt);
  return Boolean((hasFull || hasCore) && hasOfficialMark);
}

function extractCaseCustomerNames(text, companyName, limit = 20) {
  const q = String(companyName || '').trim();
  const lines = String(text || '')
    .split('\n')
    .map((x) => x.replace(/[*#>\-`_]/g, ' ').trim())
    .filter(Boolean);
  const relatedLines = lines.filter((ln) => /(客户|案例|合作伙伴|合作客户|典型客户|服务客户|标杆客户)/.test(ln));
  const names = [];
  const seen = new Set();

  const legalNames = extractLegalNamesFromTexts(relatedLines, limit * 2);
  for (const n of legalNames) {
    const clean = cleanExtractedOrgName(n);
    if (!clean) continue;
    if (!isValidRelationEntityName(clean, q) || seen.has(clean)) continue;
    seen.add(clean);
    names.push(clean);
    if (names.length >= limit) return names;
  }

  for (const ln of relatedLines) {
    const m = ln.match(/(?:客户|合作伙伴|案例)(?:包括|覆盖|涉及|服务|有|：|:)?(.{4,180})/);
    const part = m ? m[1] : ln;
    const tokens = part.split(/[、,，；;|/]/).map((x) => x.trim());
    for (const t of tokens) {
      if (!t || t.length < 2 || t.length > 24) continue;
      if (!/[A-Za-z\u4e00-\u9fa5]/.test(t)) continue;
      if (/(客户|案例|合作|伙伴|解决方案|行业|产品|服务|官网|联系我们)/.test(t)) continue;
      if (!/(公司|集团|银行|汽车|电网|电力|能源|证券|保险|医院|大学|学院|航空|铁路|地铁|港口)/.test(t)) continue;
      const clean = cleanExtractedOrgName(t);
      if (!clean) continue;
      if (!isValidRelationEntityName(clean, q) || seen.has(clean)) continue;
      seen.add(clean);
      names.push(clean);
      if (names.length >= limit) return names;
    }
  }
  return names;
}

async function searchSnippetRelations(companyName, keyword, limit = 20) {
  const q = String(companyName || '').trim();
  if (!q) return [];
  const isCustomerMode = /客户|案例|合作/.test(keyword);
  const queries = Array.from(
    new Set(
      isCustomerMode
        ? [
            `${q} ${keyword}`,
            `${q} 前五大客户`,
            `${q} 中标 客户`,
            `${q} 供货 客户`,
            `${q} 官网 客户案例`,
          ]
        : [
            `${q} ${keyword}`,
            `${q} 主要供应商`,
            `${q} 上游供应商`,
            `${q} 采购 供应商`,
            `${q} 招标 供应商`,
          ],
    ),
  );
  const pages = await Promise.all(queries.map((one) => fetchMirrorSearchText(one)));
  const out = [];
  const seen = new Set();
  for (let i = 0; i < pages.length; i += 1) {
    const txt = String(pages[i] || '');
    if (!txt) continue;
    const source = `https://r.jina.ai/http://www.baidu.com/s?wd=${encodeURIComponent(queries[i])}`;
    const lines = splitUsefulLines(txt).filter((ln) => /(客户|供应商|采购|合作伙伴|案例|中标|供货|签约|订单|招标)/.test(ln));
    const legal = extractLegalNamesFromTexts(lines, limit * 4);
    const alias = lines.flatMap((ln) => extractEntityAliasesFromLine(ln, q));
    for (const n of [...legal, ...alias]) {
      const clean = cleanExtractedOrgName(n);
      if (!clean || seen.has(clean)) continue;
      const legalLike = looksLikeLegalEntityName(clean);
      if (legalLike && !isValidRelationEntityName(clean, q)) continue;
      if (!legalLike) {
        if (!isLikelyCompanyToken(clean)) continue;
        if (clean.length > 12) continue;
        if (/(查询|涵盖|招标与|相关|包括|以及|采用|提供|解决方案|案例)/.test(clean)) continue;
        if (isSameEntityOrBrandFamily(q, clean)) continue;
      }
      seen.add(clean);
      out.push(evidenceRow(clean, {
        reason: `公开检索摘要：${keyword}`,
        confidence: legalLike ? 0.64 : 0.56,
        source,
        sourceType: 'public_search_snippet',
        sourceTier: legalLike ? 'tier2' : 'tier3',
        evidenceSnippet: clean,
      }));
      if (out.length >= limit) break;
    }
    if (out.length >= limit) break;
  }
  return out;
}

function parsePossibleOrgNamesFromLine(line) {
  const ln = String(line || '').trim();
  if (!ln) return [];
  const out = new Set(extractLegalNamesFromTexts([ln], 8));
  const chunks = ln.split(/[、,，；;|/]/).map((x) => x.trim());
  for (const c of chunks) {
    const clean = cleanExtractedOrgName(c);
    if (!clean) continue;
    if (clean.length < 2 || clean.length > 20) continue;
    if (/(报告|咨询|研究|行业|市场|企业|公司|集团|股份|有限公司)$/.test(clean) && clean.length <= 3) continue;
    if (/(排名|位列|TOP|Top|市场份额|增长率|收入|营收|同比|环比|规模|亿元|万美元|人民币)/i.test(clean)) continue;
    if (!/[A-Za-z\u4e00-\u9fa5]/.test(clean)) continue;
    out.add(clean);
  }
  return [...out];
}

function isLikelyCompanyToken(name = '') {
  const n = String(name || '').trim();
  if (!n || n.length < 2 || n.length > 18) return false;
  if (n.length > 10 && /的/.test(n)) return false;
  if (/[\/\\<>{}\[\]()（）@#$%^*_=+:;"'`~]/.test(n)) return false;
  if (/\d{4}年|\d{1,2}月|\d{1,2}日/.test(n)) return false;
  if (/\d{2,}/.test(n) && !/[A-Za-z]/.test(n)) return false;
  if (/\.(com|cn|net|org|svg|png|jpg|jpeg|gif)$/i.test(n)) return false;
  if (/(Image|http|https|www|百度|搜索|aichat|basics|board|platform|sa=|index|auto ai)/i.test(n)) return false;
  if (/(仅供借鉴参考|仅供参考|递交招股书|挂牌上市|独家保荐人|工资排名|工资待遇|暂无|对比)/.test(n)) return false;
  if (/(子公司|全资|上半年|行业排名|资产总规模|同比增长|发行规模|托管资产|电话会议|发布研究|分仓佣金)/.test(n)) return false;
  if (/(今年|去年|情况|行业|市场|报告|咨询|研究|资本|投资|战略|排名|位列|递表|冲刺|市占率|亿元|万美元|人民币|前不久|有实力|参股公司|发起|获得|累计|第一梯队)/.test(n)) return false;
  if (/的/.test(n) && !/(公司|集团|银行|汽车|电气|科技|软件|能源|股份|电子|通信|网络|电网)/.test(n)) return false;
  const cnChars = (n.match(/[\u4e00-\u9fa5]/g) || []).length;
  if (/(公司|集团|银行|汽车|电气|科技|软件|能源|股份|电子|通信|网络|电网|资本|证券|智行|车联|芯片)/.test(n)) return true;
  if (cnChars >= 2 && cnChars <= 6 && !/(报告|咨询|研究|行业|市场|排名|竞争|品牌|实力)/.test(n)) return true;
  if (/^[A-Za-z0-9&.\-]{2,12}$/.test(n)) return true;
  return false;
}

async function fetchConsultingIntel(companyName, industryName, limit = 12) {
  const cname = String(companyName || '').trim();
  const iname = String(industryName || '').trim();
  if (!cname && !iname) return [];
  const key = `consultIntel:${cname}:${iname}`;
  const cached = cacheGet(key);
  if (cached) return cached;

  const orgs = CONSULTING_ORGS.slice(0, 8);
  const scoreMap = new Map();
  const rankRe = /(排名|位列|TOP|Top|第一梯队|市场份额|主要厂商|竞争对手|竞品)/i;
  const ingest = (txt, org) => {
    const lines = splitUsefulLines(txt);
    for (const line of lines) {
      if (!rankRe.test(line)) continue;
      if (/(工资|待遇|招聘|下载|图片|登录|百科|问答|贴吧|文库)/.test(line)) continue;
      const names = parsePossibleOrgNamesFromLine(line);
      for (const n of names) {
        if (!isLikelyCompanyToken(n)) continue;
        if (isSameEntityOrBrandFamily(cname, n)) continue;
        const core = coreCompanyName(n);
        if (!core || core.length < 2) continue;
        const prev = scoreMap.get(n) || { name: n, score: 0, mentions: 0, orgs: new Set(), sample: '' };
        prev.mentions += 1;
        prev.score += rankRe.test(line) ? 2 : 1;
        prev.orgs.add(org);
        if (!prev.sample) prev.sample = line.slice(0, 120);
        scoreMap.set(n, prev);
      }
    }
  };

  const baseQuery = `${cname} ${iname || ''} 行业 排名 竞争 对手 报告`.trim();
  const pages1 = await Promise.all(orgs.map((org) => withTimeout(fetchMirrorSearchText(`${baseQuery} ${org}`), 3500, '')));
  pages1.forEach((txt, idx) => ingest(txt, orgs[idx]));
  if (!scoreMap.size && iname) {
    const fallbackQuery = `${iname} 行业竞争格局 市场份额 咨询 报告`;
    const pages2 = await Promise.all(orgs.map((org) => withTimeout(fetchMirrorSearchText(`${fallbackQuery} ${org}`), 3500, '')));
    pages2.forEach((txt, idx) => ingest(txt, orgs[idx]));
  }

  const rows = [...scoreMap.values()]
    .filter((x) => x.orgs.size >= 2 || /(公司|集团|银行|科技|汽车|电气|能源|股份|通信|软件|网络|电网)/.test(x.name))
    .sort((a, b) => b.score - a.score || b.mentions - a.mentions)
    .slice(0, limit)
    .map((x) => ({
      name: x.name,
      reason: `咨询报告提及：${[...x.orgs].slice(0, 3).join('、')}`,
      confidence: Math.min(0.9, 0.45 + x.score * 0.05),
      sample: x.sample,
    }));
  cacheSet(key, rows, 30 * 60 * 1000);
  return rows;
}

async function officialSiteCustomers(companyName, limit = 20) {
  const site = await discoverOfficialWebsite(companyName);
  if (!site) return { site: '', rows: [] };
  let origin;
  try {
    origin = new URL(site).origin;
  } catch {
    return { site: '', rows: [] };
  }
  const paths = ['/', '/customer', '/customers', '/case', '/cases', '/partner', '/partners', '/solution', '/industry'];
  const pages = await Promise.all(paths.map((p) => fetchSiteText(`${origin}${p}`)));
  const names = extractCaseCustomerNames(pages.join('\n'), companyName, limit);
  const rows = names.slice(0, limit).map((name) => evidenceRow(name, {
    reason: '官网案例/合作伙伴展示',
    confidence: 0.75,
    source: site,
    sourceType: 'official_website',
    sourceTier: 'tier1',
  }));
  return { site, rows };
}

function localSuggest(q) {
  const query = normalizeName(q);
  if (!query) return [];
  return localCompanies
    .map((c) => {
      const names = [c.shortName, c.fullName, ...(c.aliases || [])];
      const score = Math.max(...names.map((n) => overlapScore(query, n)));
      return {
        code: String(c.stockCode || '').replace(/\.(SH|SZ)$/, ''),
        name: c.shortName,
        fullName: c.fullName,
        secid: mapSecId(String(c.stockCode || '').replace(/\.(SH|SZ)$/, '')),
        score,
      };
    })
    .filter((x) => x.score > 35)
    .sort((a, b) => b.score - a.score)
    .slice(0, 10);
}

function localNameSuggest(q, limit = 10) {
  const raw = String(q || '').trim();
  const query = normalizeName(raw);
  if (!query) return [];
  if (query.length < 3) return [];
  return localNamePool
    .map((name) => {
      const n = normalizeName(name);
      const exact = n === query ? 100 : 0;
      const prefix = n.startsWith(query) ? 92 : 0;
      const contains = n.includes(query) ? 80 : 0;
      const reverseContains = query.includes(n) ? 86 : 0;
      return { name, score: Math.max(exact, prefix, contains, reverseContains) };
    })
    .filter((x) => x.score >= 80)
    .sort((a, b) => b.score - a.score)
    .slice(0, limit)
    .map((x) => ({ code: '', name: x.name, secid: '' }));
}

function fastLocalSuggest(q, limit = 12) {
  const raw = String(q || '').trim();
  const qNorm = normalizeName(raw);
  if (!qNorm) return [];
  const strictLegalQuery = shouldUseStrictSuggestMatch(raw);
  const minScore = strictLegalQuery ? 78 : qNorm.length <= 2 ? 66 : qNorm.length <= 4 ? 56 : 46;
  const byName = new Map();
  const push = (row) => {
    const name = String(row?.displayName || row?.name || '').trim();
    if (!name) return;
    const key = sanitizeLegalEntityName(name) || name;
    const score = Math.max(
      candidateMatchScore(raw, row?.name || '', row?.displayName || row?.name || ''),
      overlapScoreEnhanced(raw, row?.displayName || row?.name || ''),
    );
    if (score < minScore) return;
    const prev = byName.get(key);
    if (!prev || (row.code && !prev.code) || score > prev._score) {
      byName.set(key, {
        code: row.code || '',
        name: row.name || name,
        secid: row.secid || (row.code ? mapSecId(row.code) : ''),
        displayName: row.displayName || name,
        _score: score,
      });
    }
  };

  for (const c of localCompanies) {
    push({
      code: String(c.stockCode || '').replace(/\.(SH|SZ)$/, ''),
      name: c.shortName || c.fullName || '',
      displayName: c.fullName || c.shortName || '',
      secid: mapSecId(String(c.stockCode || '').replace(/\.(SH|SZ)$/, '')),
    });
  }
  for (const n of localNamePool) push({ code: '', name: n, displayName: n, secid: '' });
  for (const ov of dynamicCompanyIndustryOverrides) push({ code: '', name: ov?.name || '', displayName: ov?.name || '', secid: '' });
  for (const ov of SEMICON_TOP150_OVERRIDES) push({ code: '', name: ov?.name || '', displayName: ov?.name || '', secid: '' });

  return [...byName.values()]
    .sort((a, b) => b._score - a._score)
    .slice(0, limit)
    .map((x) => ({ code: x.code, name: x.name, secid: x.secid, displayName: x.displayName }));
}

function pickBestCandidate(rows, query, minScore = 0) {
  const q = String(query || '').trim();
  if (!rows.length) return null;
  const best = rows
    .map((r) => {
      const score = overlapScoreEnhanced(q, r.name || '');
      return { ...r, _score: score };
    })
    .sort((a, b) => b._score - a._score)[0];
  if (!best || best._score < minScore) return null;
  return best;
}

async function extractAnnualRelations(stockCode, fiscalYear = 2024) {
  const code = String(stockCode || '').replace(/\D/g, '');
  if (!/^\d{6}$/.test(code)) return { customers: [], suppliers: [], meta: { found: false } };
  const key = `annualRelations:${code}:${fiscalYear}`;
  const cached = cacheGet(key);
  if (cached) return cached;
  const script = path.join(ROOT, 'scripts', 'extract_top_relations.py');
  try {
    const { stdout } = await execFileAsync('python3', [script, code, String(fiscalYear)], {
      timeout: 6000,
      maxBuffer: 1024 * 1024 * 8,
    });
    const json = JSON.parse(String(stdout || '{}'));
    if (!json || json.ok !== true) return { customers: [], suppliers: [], meta: { found: false } };
    const out = {
      customers: Array.isArray(json.customers) ? json.customers : [],
      suppliers: Array.isArray(json.suppliers) ? json.suppliers : [],
      meta: json.meta || { found: false },
    };
    cacheSet(key, out, 30 * 60 * 1000);
    return out;
  } catch {
    const out = { customers: [], suppliers: [], meta: { found: false } };
    cacheSet(key, out, 5 * 60 * 1000);
    return out;
  }
}

async function stockProfile(secid) {
  if (!secid) return null;
  const fields = 'f57,f58,f84,f85,f116,f117,f127,f100,f162,f163';
  const url = `https://push2.eastmoney.com/api/qt/stock/get?invt=2&fltt=1&fields=${fields}&secid=${encodeURIComponent(secid)}`;
  try {
    const text = await fetchText(url);
    const json = parseMaybeJsonp(text) || {};
    const d = json?.data || {};
    return {
      code: d.f57 || '',
      name: d.f58 || '',
      industryName: d.f127 || '',
      industryCode: d.f100 || '',
      totalMarketValue: Number(d.f116 || 0),
      circulatingMarketValue: Number(d.f117 || 0),
      peTtm: Number(d.f162 || 0),
      pb: Number(d.f163 || 0),
    };
  } catch {
    return null;
  }
}

async function fetchFullCompanyNameByCode(code) {
  const pure = String(code || '').replace(/\D/g, '');
  if (!/^\d{6}$/.test(pure)) return '';
  const key = `fullName:${pure}`;
  const cached = cacheGet(key);
  if (cached) return cached;
  const market = /^(6|9)/.test(pure) ? 'SH' : 'SZ';
  const url = `https://emweb.securities.eastmoney.com/PC_HSF10/CompanySurvey/CompanySurveyAjax?code=${market}${pure}`;
  try {
    const txt = await fetchText(url);
    const j = parseMaybeJsonp(txt) || {};
    const full = j?.jbzl?.gsmc || '';
    if (full) cacheSet(key, full, 24 * 60 * 60 * 1000);
    return full;
  } catch {
    return '';
  }
}

async function brokerReportIndustryPeers(indvInduCode, pageLimit = 2) {
  const code = String(indvInduCode || '').trim();
  if (!code) return [];
  const key = `brokerPeers:${code}`;
  const cached = cacheGet(key);
  if (cached) return cached;

  const rows = [];
  for (let p = 1; p <= pageLimit; p++) {
    const url =
      `https://reportapi.eastmoney.com/report/list?code=*` +
      `&pageNo=${p}&pageSize=100&industryCode=${encodeURIComponent(code)}` +
      `&industry=*&rating=*&ratingchange=*` +
      `&beginTime=2024-01-01&endTime=2026-12-31&qType=0`;
    try {
      const txt = await fetchText(url);
      const j = parseMaybeJsonp(txt) || {};
      const data = Array.isArray(j.data) ? j.data : [];
      rows.push(...data);
      if (!data.length) break;
    } catch {
      break;
    }
  }

  const agg = new Map();
  for (const r of rows) {
    const sc = String(r.stockCode || '');
    if (!isAStockCode(sc)) continue;
    const k = sc;
    if (!agg.has(k)) {
      agg.set(k, {
        code: sc,
        name: r.stockName || '',
        industryCode: r.indvInduCode || '',
        industryName: r.indvInduName || '',
        reportCount: 0,
        brokers: new Set(),
        lastPublishDate: r.publishDate || '',
      });
    }
    const o = agg.get(k);
    o.reportCount += 1;
    if (r.orgSName) o.brokers.add(r.orgSName);
    if (String(r.publishDate || '') > String(o.lastPublishDate || '')) o.lastPublishDate = r.publishDate || '';
  }
  const out = [...agg.values()]
    .map((x) => ({ ...x, brokerCount: x.brokers.size }))
    .sort((a, b) => b.reportCount - a.reportCount || b.brokerCount - a.brokerCount)
    .slice(0, 40);
  cacheSet(key, out, 5 * 60 * 1000);
  return out;
}

async function brokerMetaForStock(stockCode) {
  const pure = String(stockCode || '').replace(/\D/g, '');
  if (!/^\d{6}$/.test(pure)) return { indvInduCode: '', indvInduName: '', reportCount: 0, brokerCount: 0 };
  const key = `brokerMeta:${pure}`;
  const cached = cacheGet(key);
  if (cached) return cached;
  const url =
    `https://reportapi.eastmoney.com/report/list?code=${pure}` +
    `&pageNo=1&pageSize=50&industryCode=*&industry=*&rating=*&ratingchange=*` +
    `&beginTime=2024-01-01&endTime=2026-12-31&qType=0`;
  try {
    const txt = await fetchText(url);
    const j = parseMaybeJsonp(txt) || {};
    const data = Array.isArray(j.data) ? j.data : [];
    if (!data.length) {
      const out = { indvInduCode: '', indvInduName: '', reportCount: 0, brokerCount: 0 };
      cacheSet(key, out, 2 * 60 * 1000);
      return out;
    }
    const first = data[0] || {};
    const brokers = new Set(data.map((x) => x.orgSName).filter(Boolean));
    const out = {
      indvInduCode: first.indvInduCode || '',
      indvInduName: first.indvInduName || '',
      reportCount: data.length,
      brokerCount: brokers.size,
    };
    cacheSet(key, out, 10 * 60 * 1000);
    return out;
  } catch {
    return { indvInduCode: '', indvInduName: '', reportCount: 0, brokerCount: 0 };
  }
}

async function fetchRevenue(stockCode) {
  const pure = String(stockCode || '').replace(/\D/g, '');
  const key = `revenue:${pure}`;
  const cached = cacheGet(key);
  if (cached) return cached;
  const code = String(stockCode || '').replace(/\D/g, '');
  if (!/^\d{6}$/.test(code)) return { revenue: null, fiscalYear: null, source: '' };
  const url = `https://money.finance.sina.com.cn/corp/go.php/vDOWN_ProfitStatement/displaytype/4/stockid/${code}/ctrl/all.phtml`;
  try {
    const ctl = new AbortController();
    const t = setTimeout(() => ctl.abort(), 12000);
    const r = await fetch(url, { signal: ctl.signal, headers: { 'user-agent': 'Mozilla/5.0' } });
    clearTimeout(t);
    if (!r.ok) return { revenue: null, fiscalYear: null, source: url };
    const ab = await r.arrayBuffer();
    const text = Buffer.from(ab).toString('latin1');
    const lines = text.split(/\r?\n/).filter((x) => x.trim());
    if (!lines.length) return { revenue: null, fiscalYear: null, source: url };
    const headers = lines[0].split('\t').map((x) => x.trim());
    const yearCol = headers.findIndex((h) => /^\d{4}1231$/.test(h));
    if (yearCol < 0) return { revenue: null, fiscalYear: null, source: url };
    const dataRows = lines.slice(2).filter((ln) => ln.includes('\t'));
    if (!dataRows.length) return { revenue: null, fiscalYear: null, source: url };
    // First line is usually revenue row; if parse fails, fallback to first positive value in top rows.
    const preferred = dataRows[0].split('\t').map((x) => x.trim());
    let val = Number((preferred[yearCol] || '').replaceAll(',', ''));
    if (!Number.isFinite(val) || val <= 0) {
      for (const row of dataRows.slice(0, 8)) {
        const cells = row.split('\t').map((x) => x.trim());
        const cand = Number((cells[yearCol] || '').replaceAll(',', ''));
        if (Number.isFinite(cand) && cand > 0) {
          val = cand;
          break;
        }
      }
    }
    const year = Number(headers[yearCol].slice(0, 4));
    if (!Number.isFinite(val) || val <= 0) {
      const out = { revenue: null, fiscalYear: year || null, source: url };
      cacheSet(key, out, 3 * 60 * 1000);
      return out;
    }
    const out = { revenue: val, fiscalYear: year || null, source: url };
    cacheSet(key, out, 30 * 60 * 1000);
    return out;
  } catch {
    const out = { revenue: null, fiscalYear: null, source: '' };
    cacheSet(key, out, 60 * 1000);
    return out;
  }
}

function cleanIndustryName(name = '') {
  return String(name).replace(/[ⅠⅡⅢⅣⅤ]/g, '').replace(/\s+/g, '').trim();
}

function sameIndustry(a = '', b = '') {
  const x = cleanIndustryName(a);
  const y = cleanIndustryName(b);
  if (!x || !y) return false;
  return x === y || x.includes(y) || y.includes(x);
}

function industryHint(industryName = '') {
  const cls = classifyIndustryDetailed(industryName);
  return { name: cls.industryName, upstream: cls.upstream, downstream: cls.downstream };
}

function longestCommonPrefixLen(a = '', b = '') {
  const n = Math.min(a.length, b.length);
  let i = 0;
  while (i < n && a[i] === b[i]) i += 1;
  return i;
}

function isSameEntityOrBrandFamily(targetName, candidateName) {
  const t = normalizeName(targetName);
  const c = normalizeName(candidateName);
  if (!t || !c) return false;
  if (t === c) return true;
  if (t.includes(c) || c.includes(t)) return true;
  const lcp = longestCommonPrefixLen(t, c);
  if (lcp >= 2) {
    const tRest = t.slice(lcp);
    const cRest = c.slice(lcp);
    if (tRest.length <= 4 || cRest.length <= 4) return true;
  }
  return overlapScore(t, c) >= 72;
}

function isLikelyNearNameVariant(targetName, candidateName) {
  const t = stripLegalTail(coreCompanyName(targetName || ''));
  const c = stripLegalTail(coreCompanyName(candidateName || ''));
  if (!t || !c) return false;
  if (t === c) return true;
  if (Math.abs(t.length - c.length) > 3) return false;
  const lcp = longestCommonPrefixLen(t, c);
  const ov = overlapScore(t, c);
  if (lcp >= 2 && ov >= 60) return true;
  if (t.length >= 3 && c.length >= 3 && (t.includes(c) || c.includes(t))) return true;
  return false;
}

function buildFinancialLinkageRows(industryL2 = '', type = 'downstream', selfName = '', limit = 8) {
  const lib = FINANCIAL_LINKAGE_LIBRARY[String(industryL2 || '').trim()];
  const rows = Array.isArray(lib?.[type]) ? lib[type] : [];
  return rows
    .filter((x) => String(x || '').trim())
    .filter((x) => !isSameEntityOrBrandFamily(selfName, x))
    .slice(0, limit)
    .map((name) =>
      evidenceRow(name, {
        reason: `行业联动复查：${industryL2}${type === 'upstream' ? '上游能力侧' : '下游服务侧'}`,
        confidence: 0.66,
        sourceType: 'industry_linkage_review',
        sourceTier: 'tier2',
      }),
    );
}

function buildFinancialPeerFallback(industryL2 = '', selfName = '', limit = 10) {
  const peers = Array.isArray(FINANCIAL_PEER_LIBRARY[String(industryL2 || '').trim()])
    ? FINANCIAL_PEER_LIBRARY[String(industryL2 || '').trim()]
    : [];
  return peers
    .filter((x) => x?.name && !isSameEntityOrBrandFamily(selfName, x.name))
    .slice(0, limit)
    .map((x) =>
      evidenceRow(x.name, {
        code: x.code || '',
        reason: `行业联动复查：${industryL2}同业头部`,
        confidence: 0.7,
        sourceType: 'industry_peer_library',
        sourceTier: 'tier2',
      }),
    );
}

function buildChina500PeerFallback(companyName = '', industryL2 = '', limit = 10) {
  const hit = findChina500ByName(companyName || '');
  const canonical = hit?.name || sanitizeLegalEntityName(companyName || '');
  if (!canonical) return [];
  const direct = CHINA500_INDEX.peersByName.get(canonical) || [];
  const fromIndustry = (CHINA500_INDEX.byIndustry.get(industryL2) || [])
    .map((x) => x.name)
    .filter((x) => x !== canonical)
    .slice(0, limit);
  const list = [...new Set([...(direct || []), ...fromIndustry])].slice(0, limit);
  return list.map((name) =>
    evidenceRow(name, {
      reason: `公开行业榜单同业分组：${industryL2 || '同业'}`,
      confidence: 0.66,
      sourceType: 'china500_peer_group',
      sourceTier: 'tier2',
    }),
  );
}

function buildFinancialTop5Fallback(industryL2 = '', fiscalYear = 2024, selfName = '', limit = 5) {
  const peers = Array.isArray(FINANCIAL_PEER_LIBRARY[String(industryL2 || '').trim()])
    ? FINANCIAL_PEER_LIBRARY[String(industryL2 || '').trim()]
    : [];
  return peers
    .filter((x) => x?.name && !isSameEntityOrBrandFamily(selfName, x.name))
    .slice(0, limit)
    .map((x) => ({
      code: x.code || '',
      name: x.name,
      industryName: industryL2,
      reportCount: 0,
      brokerCount: 0,
      revenue: null,
      fiscalYear,
      revenueSource: '',
      sourceTier: 'tier2',
      sourceType: 'industry_peer_library',
      confidence: 0.68,
    }));
}

function buildSemiconLinkageRows(type = 'downstream', selfName = '', limit = 8) {
  const rows = Array.isArray(SEMICON_LINKAGE_LIBRARY[type]) ? SEMICON_LINKAGE_LIBRARY[type] : [];
  return rows
    .filter((x) => String(x || '').trim())
    .filter((x) => !isSameEntityOrBrandFamily(selfName, x))
    .slice(0, limit)
    .map((name) =>
      evidenceRow(name, {
        reason: `半导体行业链路推断：${type === 'upstream' ? '上游材料/设备' : '下游应用客户'}`,
        confidence: 0.62,
        sourceType: 'semiconductor_linkage_review',
        sourceTier: 'tier2',
      }),
    );
}

function buildIndustryHintRows(industryName = '', type = 'downstream', selfName = '', limit = 8) {
  const h = industryHint(industryName);
  const base = type === 'upstream' ? (h?.upstream || []) : (h?.downstream || []);
  if (!base.length) {
    const generic = type === 'upstream'
      ? ['核心原材料供应商', '关键设备与系统供应商', '基础软件与服务商']
      : ['核心企业客户', '行业渠道客户', '区域重点客户'];
    return generic.slice(0, limit).map((name) =>
      evidenceRow(name, {
        reason: `行业链路推断：${industryName || '同业'}${type === 'upstream' ? '上游' : '下游'}`,
        confidence: 0.52,
        sourceType: 'industry_hint_fallback',
        sourceTier: 'tier2',
      }),
    );
  }
  const filtered = base
    .filter((x) => String(x || '').trim())
    .filter((x) => !isSameEntityOrBrandFamily(selfName, x))
    .slice(0, limit);
  const list = filtered.length
    ? filtered
    : (type === 'upstream'
      ? ['核心原材料供应商', '关键设备与系统供应商', '基础软件与服务商']
      : ['核心企业客户', '行业渠道客户', '区域重点客户']);
  return list
    .map((name) =>
      evidenceRow(name, {
        reason: `行业链路推断：${industryName || '同业'}${type === 'upstream' ? '上游' : '下游'}`,
        confidence: 0.58,
        sourceType: 'industry_hint_fallback',
        sourceTier: 'tier2',
      }),
    );
}

async function inferIndustryByWeb(name) {
  const q = String(name || '').trim();
  if (!q) return '';
  const direct = classifyIndustryDetailed(q);
  if (direct.industryLevel1 !== '综合') return direct.industryName;
  if (/(计算机|软件|信息技术|信息服务|云计算|大数据|人工智能|网络安全)/.test(q)) return '软件开发';
  const site = await discoverOfficialWebsite(q);
  if (site) {
    const siteText = await withTimeout(fetchSiteText(site), 6500, '');
    if (siteText) {
      const clsFromSite = classifyIndustryDetailed(`${q} ${String(siteText).slice(0, 15000)}`);
      if (clsFromSite.industryLevel1 !== '综合') return clsFromSite.industryName;
    }
  }
  const [s1, s2, s3] = await Promise.all([
    baiduSuggest(q, 10),
    baiduSuggest(`${q} 行业`, 10),
    baiduSuggest(`${q} 主要产品`, 10),
  ]);
  const cleanTerms = [...s1, ...s2, ...s3]
    .map((x) => String(x || '').trim())
    .filter((x) => x.length >= 2 && x.length <= 20)
    .filter((x) => !/[?？!！]/.test(x))
    .filter((x) => !/(招聘|电话|地址|官网|怎么样|是哪家|是不是|国企|公告|董事长|股票|代码|开户)/.test(x));
  const joined = `${q} ${cleanTerms.join(' ')}`.trim();
  const cls = classifyIndustryDetailed(joined);
  return cls.industryLevel1 === '综合' ? '' : cls.industryName;
}

function getIndustrySeedCodes(industryName = '') {
  const key = String(industryName || '').trim();
  if (!key) return [];
  const base = INDUSTRY_HEAD_SEED_CODES[key] || [];
  const knowledge = industryKnowledge?.industries?.[key]?.sampleCompanies || [];
  const fromKnowledge = knowledge.map((x) => String(x?.code || '').replace(/\D/g, '')).filter((x) => /^\d{6}$/.test(x));
  return Array.from(new Set([...base, ...fromKnowledge]));
}

async function refreshIndustryKnowledgeBucket(industryName, force = false) {
  const ind = String(industryName || '').trim();
  if (!ind) return null;
  const prev = industryKnowledge?.industries?.[ind];
  if (!force && prev?.updatedAt) {
    const age = Date.now() - Date.parse(prev.updatedAt);
    if (Number.isFinite(age) && age < 3 * 24 * 60 * 60 * 1000) return prev;
  }
  const meta = INDUSTRY_TAXONOMY.find((x) => x.l2 === ind);
  const terms = Array.from(
    new Set([
      ind,
      ...(meta?.keywords || []).slice(0, 8),
      `${ind} 上市公司`,
      `${ind} 龙头`,
      `${ind} 企业`,
      `${ind} 赛道`,
    ]),
  );
  const byCode = new Map();
  for (const t of terms) {
    const rows = await withTimeout(eastmoneySuggest(t, 80), 7000, []);
    for (const r of rows) {
      const code = String(r?.code || '').replace(/\D/g, '');
      if (!/^\d{6}$/.test(code)) continue;
      if (!isAStockCode(code)) continue;
      if (!isCompanyLikeName(r?.name || '')) continue;
      if (!byCode.has(code)) byCode.set(code, { code, name: r?.name || '', secid: r?.secid || mapSecId(code) });
    }
  }
  const scoped = [...byCode.values()].slice(0, 240);
  const checked = await Promise.all(
    scoped.map(async (x) => {
      const p = await withTimeout(stockProfile(x.secid || mapSecId(x.code)), 1200, null);
      const indText = `${p?.industryName || ''} ${x.name || ''}`.trim();
      const cls = classifyIndustryDetailed(indText);
      const sameL2 = cls.industryLevel2 === ind;
      const sameL1 = meta?.l1 && cls.industryLevel1 === meta.l1;
      if (sameL2 || sameL1) return { ...x, name: p?.name || x.name };
      return null;
    }),
  );
  const sampleCompanies = checked.filter(Boolean).slice(0, 160);
  const existingNameSet = new Set(sampleCompanies.map((x) => sanitizeLegalEntityName(x.name || '')).filter(Boolean));
  if (sampleCompanies.length < 120) {
    const textTerms = terms.slice(0, 5);
    for (const t of textTerms) {
      if (sampleCompanies.length >= 140) break;
      const txt = await withTimeout(fetchMirrorSearchText(`${t} 企业 名单 公司`), 5000, '');
      const legalNames = extractLegalNamesFromTexts([txt], 160)
        .map((x) => sanitizeLegalEntityName(cleanExtractedOrgName(x)))
        .filter((x) => looksLikeLegalEntityName(x))
        .filter((x) => !isBranchEntityName(x))
        .filter((x) => !isGenericLegalName(x));
      for (const nm of legalNames) {
        if (!nm) continue;
        const key = sanitizeLegalEntityName(nm);
        if (!key || existingNameSet.has(key)) continue;
        existingNameSet.add(key);
        sampleCompanies.push({ code: '', name: nm, secid: '' });
        if (sampleCompanies.length >= 140) break;
      }
    }
  }
  const mergedCodes = Array.from(new Set([...(INDUSTRY_HEAD_SEED_CODES[ind] || []), ...sampleCompanies.map((x) => x.code)]));
  INDUSTRY_HEAD_SEED_CODES[ind] = mergedCodes.slice(0, 80);
  const bucket = {
    l1: meta?.l1 || '综合',
    l2: ind,
    updatedAt: new Date().toISOString(),
    sampleCompanies,
    source: 'eastmoney_searchapi',
  };
  industryKnowledge.industries[ind] = bucket;
  industryKnowledge.updatedAt = new Date().toISOString();
  saveJson(INDUSTRY_KNOWLEDGE_PATH, industryKnowledge);
  saveJson(path.join(ROOT, 'data', 'industry_seed_codes.json'), INDUSTRY_HEAD_SEED_CODES);
  return bucket;
}

async function bootstrapIndustryKnowledge(force = false, targetIndustry = '') {
  const all = [...new Set(INDUSTRY_TAXONOMY.map((x) => x.l2).filter(Boolean))];
  const list = targetIndustry ? all.filter((x) => x === targetIndustry) : all;
  const out = [];
  for (const ind of list) {
    const one = await refreshIndustryKnowledgeBucket(ind, force);
    if (one) out.push(one);
  }
  return out;
}

async function reviewIndustriesTarget(target = 100, force = false) {
  const all = [...new Set(INDUSTRY_TAXONOMY.map((x) => x.l2).filter(Boolean))];
  const rows = [];
  for (const l2 of all) {
    const bucket = await refreshIndustryKnowledgeBucket(l2, force);
    const sampleCompanies = Array.isArray(bucket?.sampleCompanies) ? bucket.sampleCompanies : [];
    const count = sampleCompanies.length;
    const gap = Math.max(0, target - count);
    rows.push({
      l1: bucket?.l1 || '',
      l2,
      count,
      target,
      gap,
      status: count >= target ? 'met' : 'short',
      updatedAt: bucket?.updatedAt || '',
      samplePreview: sampleCompanies.slice(0, 5),
    });
  }
  const metCount = rows.filter((x) => x.status === 'met').length;
  const report = {
    generatedAt: new Date().toISOString(),
    target,
    totalIndustries: rows.length,
    metCount,
    shortCount: rows.length - metCount,
    completionRate: rows.length ? Number((metCount / rows.length).toFixed(4)) : 0,
    rows: rows.sort((a, b) => b.gap - a.gap || a.l2.localeCompare(b.l2, 'zh-CN')),
  };
  saveJson(INDUSTRY_REVIEW_REPORT_PATH, report);
  return report;
}

async function top5ByIndustry(seed) {
  if (!seed?.industryCode && !seed?.industryName) return [];
  const peers = await brokerReportIndustryPeers(seed.industryCode || '', 2);
  const candidateMap = new Map();
  const industryName = seed.industryName || '';
  const pushCandidate = (x) => {
    const code = String(x?.code || '');
    if (!/^\d{6}$/.test(code)) return;
    if (candidateMap.has(code)) return;
    candidateMap.set(code, {
      code,
      name: x?.name || '',
      industryName: x?.industryName || industryName,
      reportCount: x?.reportCount || 0,
      brokerCount: x?.brokerCount || 0,
    });
  };
  for (const p of peers) pushCandidate(p);
  const seedCodes = getIndustrySeedCodes(industryName);
  for (const code of seedCodes) pushCandidate({ code, name: '', industryName, reportCount: 0, brokerCount: 0 });
  if (seed.code) pushCandidate({ code: seed.code, name: seed.name, industryName, reportCount: 1, brokerCount: 1 });
  const candidateList = [...candidateMap.values()];
  if (!candidateList.length) return [];

  const withRevenue = await Promise.all(
    candidateList.slice(0, 30).map(async (x) => {
      const rev = await withTimeout(fetchRevenue(x.code), 2500, { revenue: null, fiscalYear: null, source: '' });
      let stockName = x.name;
      if (!stockName) {
        const p = await withTimeout(stockProfile(mapSecId(x.code)), 1500, null);
        stockName = p?.name || x.name;
      }
      return {
        code: x.code,
        name: stockName,
        industryName: x.industryName || seed.industryName,
        reportCount: x.reportCount || 0,
        brokerCount: x.brokerCount || 0,
        revenue: rev.revenue,
        fiscalYear: rev.fiscalYear,
        revenueSource: rev.source,
      };
    }),
  );

  return withRevenue
    .sort((a, b) => {
      const ar = Number.isFinite(a.revenue) ? a.revenue : -1;
      const br = Number.isFinite(b.revenue) ? b.revenue : -1;
      if (br !== ar) return br - ar;
      if ((b.reportCount || 0) !== (a.reportCount || 0)) return (b.reportCount || 0) - (a.reportCount || 0);
      return (b.brokerCount || 0) - (a.brokerCount || 0);
    })
    .slice(0, 5);
}

async function top5ByIndustryNameFallback(industryName, limit = 5) {
  const ind = String(industryName || '').trim();
  if (!ind) return [];
  const china500Rows = CHINA500_INDEX.byIndustry.get(ind) || [];
  if (china500Rows.length) {
    return china500Rows.slice(0, limit).map((x) => ({
      code: '',
      name: x.name,
      industryName: ind,
      reportCount: 0,
      brokerCount: 0,
      revenue: Number.isFinite(x.revenue) && x.revenue > 0 ? x.revenue : null,
      fiscalYear: 2025,
      revenueSource: 'china500_public_ranking',
      sourceTier: 'tier2',
      sourceType: 'industry_public_ranking',
      confidence: 0.85,
    }));
  }
  const seededCodes = getIndustrySeedCodes(ind);
  if (seededCodes.length) {
    const seededRows = await Promise.all(
      seededCodes.slice(0, 20).map(async (code) => {
        const [rev, p] = await Promise.all([
          withTimeout(fetchRevenue(code), 2500, { revenue: null, fiscalYear: null, source: '' }),
          withTimeout(stockProfile(mapSecId(code)), 1500, null),
        ]);
        return {
          code,
          name: p?.name || '',
          industryName: p?.industryName || ind,
          reportCount: 0,
          brokerCount: 0,
          revenue: rev.revenue,
          fiscalYear: rev.fiscalYear,
          revenueSource: rev.source,
        };
      }),
    );
    const ranked = seededRows
      .filter((x) => Number.isFinite(x.revenue) && x.revenue > 0)
      .sort((a, b) => (b.revenue || 0) - (a.revenue || 0))
      .slice(0, limit);
    if (ranked.length >= Math.min(3, limit)) return ranked;
    const seen = new Set(ranked.map((x) => x.code));
    const extras = seededRows
      .filter((x) => !seen.has(x.code))
      .map((x) => ({ ...x, revenue: x.revenue, fiscalYear: x.fiscalYear, revenueSource: x.revenueSource }))
      .slice(0, Math.max(0, limit - ranked.length));
    return [...ranked, ...extras].slice(0, limit);
  }
  const terms = [ind];
  for (const x of ind.split(/[与和、/]/).map((s) => s.trim()).filter(Boolean)) {
    if (!terms.includes(x)) terms.push(x);
  }
  const h = industryHint(ind);
  if (h?.upstream?.length) {
    for (const x of h.upstream) {
      if (!terms.includes(x)) terms.push(x);
    }
  }
  if (h?.downstream?.length) {
    for (const x of h.downstream) {
      if (!terms.includes(x)) terms.push(x);
    }
  }
  if (/汽车|车载|网联|驾驶/.test(ind)) {
    for (const x of ['汽车电子', '智能驾驶', '车联网', '汽车零部件', '整车', '乘用车', '商用车']) {
      if (!terms.includes(x)) terms.push(x);
    }
  }
  if (/电气|电网|输配电/.test(ind)) {
    for (const x of ['电网设备', '高低压设备', '变压器', '开关设备']) {
      if (!terms.includes(x)) terms.push(x);
    }
  }
  if (/软件|信息|云/.test(ind)) {
    for (const x of ['软件开发', '工业软件', '云计算', '信息技术服务']) {
      if (!terms.includes(x)) terms.push(x);
    }
  }
  const map = new Map();
  for (const t of terms.slice(0, 16)) {
    const rows = await eastmoneySuggest(t, 20);
    for (const r of rows) {
      if (!isAStockCode(r.code)) continue;
      if (!map.has(r.code)) map.set(r.code, r);
      if (map.size >= 120) break;
    }
    if (map.size >= 120) break;
  }
  const candidates = [...map.values()];
  if (!candidates.length) return [];
  const rows = await Promise.all(
    candidates.slice(0, 40).map(async (x) => {
      if (!isCompanyLikeName(x.name || '')) return null;
      const [rev, p] = await Promise.all([
        withTimeout(fetchRevenue(x.code), 2500, { revenue: null, fiscalYear: null, source: '' }),
        withTimeout(stockProfile(mapSecId(x.code)), 1500, null),
      ]);
      if (p?.name && !isCompanyLikeName(p.name)) return null;
      return {
        code: x.code,
        name: p?.name || x.name,
        industryName: p?.industryName || ind,
        reportCount: 0,
        brokerCount: 0,
        revenue: rev.revenue,
        fiscalYear: rev.fiscalYear,
        revenueSource: rev.source,
      };
    }),
  );
  const industryFiltered = rows.filter(Boolean).filter((x) => {
    const n = cleanIndustryName(x.industryName || '');
    const target = cleanIndustryName(ind);
    if (!target) return true;
    if (sameIndustry(n, target)) return true;
    if (/证券|期货|券商/.test(target) && /证券|期货|券商/.test(n)) return true;
    if (/银行/.test(target) && /银行/.test(n)) return true;
    if (/保险/.test(target) && /保险/.test(n)) return true;
    return false;
  });
  const ranked = industryFiltered
    .filter((x) => Number.isFinite(x.revenue) && x.revenue > 0)
    .sort((a, b) => (b.revenue || 0) - (a.revenue || 0))
    .slice(0, limit);
  return ranked;
}

async function onlineRelationSuggest(companyName, keyword, limit = 20) {
  const q = String(companyName || '').trim();
  if (!q) return [];
  const web = await baiduSuggest(`${q} ${keyword}`, 20);
  const qNorm = normalizeName(q);
  const names = extractLegalNamesFromTexts(web, limit + 12).filter((x) => {
    const clean = cleanExtractedOrgName(x);
    if (!clean) return false;
    if (!isValidRelationEntityName(clean, q)) return false;
    const n = normalizeName(clean);
    return Boolean(n && n !== qNorm);
  });
  const uniq = [];
  const seen = new Set();
  for (const n of names) {
    const clean = cleanExtractedOrgName(n);
    if (!clean) continue;
    if (!isValidRelationEntityName(clean, q) || clean.includes(q) || q.includes(clean) || seen.has(clean)) continue;
    seen.add(clean);
    uniq.push(evidenceRow(clean, {
      reason: `联网检索关键词：${keyword}`,
      confidence: 0.45,
      sourceType: 'search_suggest_weak',
      sourceTier: 'tier3',
    }));
    if (uniq.length >= limit) break;
  }
  return uniq;
}

async function reverseCustomerValidation(companyName, limit = 20) {
  const q = String(companyName || '').trim();
  if (!q) return [];
  const core = coreCompanyName(q);
  const token = String(core || q).slice(0, Math.min(String(core || q).length, 6));
  if (!token || token.length < 2) return [];

  const queries = Array.from(
    new Set([
      `${q} 供应商`,
      `${q} 主要客户`,
      `${q} 采购`,
      `${q} 供货 对象`,
      `${q} 中标`,
      `${q} 客户 案例`,
      `${token} 供应商 客户`,
      `${token} 设备 采购`,
    ]),
  );

  const scoreMap = new Map();
  for (const one of queries) {
    const txt = await fetchMirrorSearchText(one);
    if (!txt) continue;
    const source = `https://r.jina.ai/http://www.baidu.com/s?wd=${encodeURIComponent(one)}`;
    const lines = splitUsefulLines(txt).filter((ln) => {
      if (!ln.includes(token)) return false;
      return /(供应商|客户|采购|供货|中标|订单|采用|招标)/.test(ln);
    });
    for (const line of lines) {
      const names = [...extractLegalNamesFromTexts([line], 12), ...extractEntityAliasesFromLine(line, q)];
      for (const raw of names) {
        const clean = cleanExtractedOrgName(raw);
        if (!clean) continue;
        const legalLike = looksLikeLegalEntityName(clean);
        if (legalLike && !isValidRelationEntityName(clean, q)) continue;
        if (!legalLike) {
          if (!isLikelyCompanyToken(clean) || clean.length > 12) continue;
          if (/(查询|涵盖|招标与|相关|包括|以及|采用|提供|解决方案|案例)/.test(clean)) continue;
          if (isSameEntityOrBrandFamily(clean, q)) continue;
        }
        const key = normalizeName(clean);
        const prev = scoreMap.get(key) || {
          name: clean,
          score: 0,
          mentions: 0,
          source,
          snippet: '',
        };
        prev.mentions += 1;
        prev.score += legalLike ? 2 : 1;
        if (!prev.snippet) prev.snippet = line.slice(0, 180);
        scoreMap.set(key, prev);
      }
    }
  }

  return [...scoreMap.values()]
    .sort((a, b) => b.score - a.score || b.mentions - a.mentions)
    .slice(0, limit)
    .map((x) =>
      evidenceRow(x.name, {
        reason: '反向披露：其他企业公告/新闻中提及其为供应商',
        confidence: Math.min(0.82, 0.56 + x.mentions * 0.06),
        source: x.source,
        sourceType: 'reverse_disclosure',
        sourceTier: looksLikeLegalEntityName(x.name) ? 'tier2' : 'tier3',
        evidenceSnippet: x.snippet || '',
      }),
    );
}

async function pickCustomersOnline(companyCode, companyName, industryName, limit = 20) {
  const [fromOfficial, fromSearch, reverseRows, weak1, weak2] = await Promise.all([
    officialSiteCustomers(companyName, limit),
    searchSnippetRelations(companyName, '客户 案例 合作伙伴', limit),
    reverseCustomerValidation(companyName, limit),
    onlineRelationSuggest(companyName, '客户', limit),
    onlineRelationSuggest(companyName, '主要客户', limit),
  ]);
  const strong = filterByEvidenceTier([...(fromOfficial.rows || []), ...fromSearch, ...reverseRows, ...weak1, ...weak2]);
  if (strong.length) return strong.slice(0, limit);
  const weak = mergeEvidenceRows([...(fromOfficial.rows || []), ...fromSearch, ...reverseRows, ...weak1, ...weak2])
    .filter((x) => {
      const n = String(x?.name || '').trim();
      if (!n) return false;
      if (isSameEntityOrBrandFamily(companyName, n) || isLikelyNearNameVariant(companyName, n)) return false;
      if (looksLikeLegalEntityName(n)) return true;
      return isLikelyCompanyToken(n) && n.length <= 12;
    })
    .slice(0, Math.min(8, limit))
    .map((x) => ({
      ...x,
      reason: x.reason || '公开网络线索（待人工核验）',
      sourceTier: x.sourceTier || 'tier3',
      confidence: Math.min(0.58, Math.max(0.42, Number(x.confidence || 0.45))),
    }));
  return weak;
}

async function pickSuppliers(companyCode, companyName, industryName, limit = 20) {
  const [fromSearch, weak1, weak2] = await Promise.all([
    searchSnippetRelations(companyName, '供应商 采购 供货', limit),
    onlineRelationSuggest(companyName, '供应商', limit),
    onlineRelationSuggest(companyName, '采购', limit),
  ]);
  const strong = filterByEvidenceTier([...fromSearch, ...weak1, ...weak2]);
  if (strong.length) return strong.slice(0, limit);
  const weak = mergeEvidenceRows([...fromSearch, ...weak1, ...weak2])
    .filter((x) => {
      const n = String(x?.name || '').trim();
      if (!n) return false;
      if (isSameEntityOrBrandFamily(companyName, n) || isLikelyNearNameVariant(companyName, n)) return false;
      if (looksLikeLegalEntityName(n)) return true;
      return isLikelyCompanyToken(n) && n.length <= 12;
    })
    .slice(0, Math.min(8, limit))
    .map((x) => ({
      ...x,
      reason: x.reason || '公开网络线索（待人工核验）',
      sourceTier: x.sourceTier || 'tier3',
      confidence: Math.min(0.58, Math.max(0.42, Number(x.confidence || 0.45))),
    }));
  return weak;
}

function normalizeAnnualRelationRows(rows = [], defaultReason = '年报披露') {
  return filterByEvidenceTier(
    (Array.isArray(rows) ? rows : []).map((x) =>
      evidenceRow(x?.name || x, {
        reason: x?.reason || defaultReason,
        confidence: Number.isFinite(x?.confidence) ? x.confidence : 0.9,
        source: x?.source || '',
        sourceType: 'annual_report',
        sourceTier: 'tier1',
        evidenceDate: x?.date || '',
        evidenceSnippet: x?.sourceSnippet || '',
      }),
    ),
  );
}

function json(res, obj, status = 200) {
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'access-control-allow-origin': '*',
    'cache-control': 'no-store',
  });
  res.end(JSON.stringify(obj));
}

function sseInit(res) {
  res.writeHead(200, {
    'content-type': 'text/event-stream; charset=utf-8',
    'cache-control': 'no-store',
    connection: 'keep-alive',
    'access-control-allow-origin': '*',
  });
}

function sseWrite(res, event, payload) {
  res.write(`event: ${event}\n`);
  res.write(`data: ${JSON.stringify(payload)}\n\n`);
}

function readReqBody(req, maxBytes = 2 * 1024 * 1024) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let total = 0;
    req.on('data', (c) => {
      total += c.length || 0;
      if (total > maxBytes) {
        reject(new Error('body_too_large'));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

async function resolveCompanyContext(q) {
  const query = String(q || '').trim();
  const ctxCacheKey = `ctx:${sanitizeLegalEntityName(query) || query}`;
  const cachedCtx = cacheGet(ctxCacheKey);
  if (cachedCtx) return cachedCtx;
  const quickNonListedProfile = (name) => {
    const industry = classifyIndustryDetailed(String(name || '').trim());
    return {
      code: '',
      name,
      industryName: industry.industryName || '',
      industryCode: '',
      website: '',
      totalMarketValue: 0,
      circulatingMarketValue: 0,
      peTtm: 0,
      pb: 0,
    };
  };
  const c500 = findChina500ByName(query);
  if (c500) {
    const out = {
      candidate: { code: '', name: c500.name || query, secid: '' },
      secid: '',
      profile: {
        code: '',
        name: c500.name || query,
        industryName: c500.l2 || '综合行业',
        industryCode: '',
        website: '',
        totalMarketValue: 0,
        circulatingMarketValue: 0,
        peTtm: 0,
        pb: 0,
      },
      nonListed: true,
    };
    cacheSet(ctxCacheKey, out, 20 * 60 * 1000);
    return out;
  }
  const quickOv = findIndustryOverrideByName(query);
  if (
    quickOv &&
    FINANCIAL_REVIEW_INDUSTRIES.has(quickOv.l2) &&
    /(交易所|票据交易|商品交易|期货交易|期货有限公司|证券通信|银联智策|中汇信息技术)/.test(query)
  ) {
    const out = {
      candidate: { code: '', name: query, secid: '' },
      secid: '',
      profile: {
        code: '',
        name: query,
        industryName: quickOv.l2,
        industryCode: '',
        website: '',
        totalMarketValue: 0,
        circulatingMarketValue: 0,
        peTtm: 0,
        pb: 0,
      },
      nonListed: true,
    };
    cacheSet(ctxCacheKey, out, 10 * 60 * 1000);
    return out;
  }
  const strictLegalQuery = shouldUseStrictSuggestMatch(query);
  const sugg = [];
  const firstQueries = buildSuggestQueries(query).slice(0, 3);
  const firstRows = await Promise.all(firstQueries.map((item) => withTimeout(eastmoneySuggest(item, 8), 900, [])));
  for (const rows of firstRows) {
    if (rows.length) sugg.push(...rows);
  }
  if (sugg.length > 16) sugg.length = 16;

  if (!sugg.length) {
    const localHit = localNameSuggest(query, 1)[0];
    const localStrong = localSuggest(query)[0];
    const fallbackNames = [localHit?.name, localStrong?.fullName, localStrong?.name].filter(Boolean);
    if (fallbackNames.length) {
      const localName = fallbackNames[0];
      const localQueries = buildSuggestQueries(localName).slice(0, 2);
      const localRows = await Promise.all(localQueries.map((item) => withTimeout(eastmoneySuggest(item, 8), 900, [])));
      for (const rows of localRows) {
        if (rows.length) sugg.push(...rows);
      }
      if (sugg.length > 16) sugg.length = 16;
      if (!sugg.length && localStrong?.code) {
        sugg.push({
          code: localStrong.code,
          name: localStrong.name,
          secid: localStrong.secid || mapSecId(localStrong.code),
        });
      }
    }
  }

  const primary = sugg.filter((x) => isAStockCode(x.code));
  const candidatePool = strictLegalQuery ? sugg : (primary.length ? primary : sugg);
  const withFull = await Promise.all(
    candidatePool.slice(0, 12).map(async (x) => {
      const full = x.code ? await withTimeout(fetchFullCompanyNameByCode(x.code), 800, '') : '';
      return { ...x, _fullName: full || '', _aliases: aliasesByCode(x.code) };
    }),
  );
  const querySanitized = sanitizeLegalEntityName(query);
  const listedExact = withFull.find(
    (x) => x.code && sanitizeLegalEntityName(x._fullName || '') === querySanitized,
  );
  const hasListedCoreCandidate = withFull.some(
    (x) => x.code && isAStockCode(x.code) && hasStrongCoreMatch(query, x._fullName || x.name || ''),
  );
  if (strictLegalQuery && !listedExact && !hasListedCoreCandidate) {
    const allowBranch = hasBranchIntent(query);
    const webNamesStrict = await withTimeout(onlineLegalNameSuggest(query, 6), 900, []);
    const strictCandidates = [...sugg.map((x) => x.name), ...webNamesStrict.map((x) => x.name)]
      .map((x) => sanitizeLegalEntityName(String(x || '').trim()))
      .filter((x) => looksLikeLegalEntityName(x))
      .filter((x) => allowBranch || !isBranchEntityName(x))
      .filter((x) => hasStrictLegalNameMatch(query, x));
    const strictSorted = strictCandidates.sort((a, b) => regionMatchBoost(query, b) - regionMatchBoost(query, a));
    const synthesized = synthesizeLegalNameCandidates(query)
      .map((x) => sanitizeLegalEntityName(x))
      .filter((x) => looksLikeLegalEntityName(x))
      .filter((x) => allowBranch || !isBranchEntityName(x))
      .sort((a, b) => regionMatchBoost(query, b) - regionMatchBoost(query, a));
    const strictBest =
      strictSorted.find((x) => sanitizeLegalEntityName(x) === querySanitized) ||
      (looksLikeLegalEntityName(query) ? query : synthesized[0]) ||
      strictSorted[0] ||
      '';
    if (looksLikeLegalEntityName(strictBest)) {
      const out = {
        candidate: { code: '', name: strictBest, secid: '' },
        secid: '',
        profile: quickNonListedProfile(strictBest),
        nonListed: true,
      };
      cacheSet(ctxCacheKey, out, 10 * 60 * 1000);
      return out;
    }
  }
  const financeAlias = /(证券|银行|保险|信托|期货|基金)/.test(query);
  const minScore = strictLegalQuery ? 78 : financeAlias ? 40 : query.length <= 3 ? 50 : 60;
  const token = extractIntentToken(query);
  const tokenHitRows = token ? withFull.filter((x) => candidateIntentHit(query, x.name || '', x._fullName || '')) : withFull;
  const scopedBase = token && tokenHitRows.length ? tokenHitRows : withFull;
  const scoped = strictLegalQuery
    ? scopedBase.filter((x) => hasStrictLegalNameMatch(query, x._fullName || x.name || '') || sanitizeLegalEntityName(x._fullName || '') === querySanitized)
    : scopedBase;
  const ranked = scoped
    .map((r) => {
      const aliasScore = Math.max(
        0,
        ...((r._aliases || []).map((a) => overlapScoreEnhanced(query, a)) || [0]),
      );
      const baseScore = Math.max(candidateMatchScore(query, r.name || '', r._fullName || ''), aliasScore);
      const cityBoost = regionMatchBoost(query, r._fullName || r.name || '');
      return { ...r, _score: baseScore + cityBoost };
    })
    .sort((a, b) => b._score - a._score);
  let candidate = ranked[0] && ranked[0]._score >= minScore ? ranked[0] : null;
  // For strong financial intents, never downgrade to a cross-industry listed company.
  if (candidate && token && isFinancialIntentToken(token) && !candidateIntentHit(query, candidate.name || '', candidate._fullName || '')) {
    candidate = null;
  }
  if (!candidate && token && isFinancialIntentToken(token) && !tokenHitRows.length) {
    candidate = null;
  }
  if (!candidate) {
    const webNames = await withTimeout(onlineLegalNameSuggest(query, 5), 900, []);
    const mergedNonListed = [...webNames.map((x) => x.name), ...synthesizeLegalNameCandidates(query)]
      .map((x) => sanitizeLegalEntityName(x))
      .filter((x) => looksLikeLegalEntityName(x))
      .filter((x) => (!token ? true : x.includes(token)))
      .sort((a, b) => regionMatchBoost(query, b) - regionMatchBoost(query, a));
    const nonListedName = mergedNonListed[0] || (looksLikeLegalEntityName(query) ? query : '');
    if (!nonListedName) return null;
    const out = {
      candidate: { code: '', name: nonListedName, secid: '' },
      secid: '',
      profile: quickNonListedProfile(nonListedName),
      nonListed: true,
    };
    cacheSet(ctxCacheKey, out, 10 * 60 * 1000);
    return out;
  }

  const secid = candidate.secid || mapSecId(candidate.code);
  const profile = (await stockProfile(secid)) || {
    code: candidate.code,
    name: candidate.name,
    industryName: '',
    industryCode: '',
    website: '',
    totalMarketValue: 0,
    circulatingMarketValue: 0,
    peTtm: 0,
    pb: 0,
  };
  const out = { candidate, secid, profile, nonListed: false };
  cacheSet(ctxCacheKey, out, 10 * 60 * 1000);
  return out;
}

function serveStatic(req, res, pathname) {
  const rel = pathname === '/' ? '/index.html' : pathname;
  const file = path.normalize(path.join(ROOT, rel));
  if (!file.startsWith(ROOT)) {
    res.writeHead(403);
    res.end('Forbidden');
    return;
  }
  fs.readFile(file, (err, buf) => {
    if (err) {
      res.writeHead(404);
      res.end('Not found');
      return;
    }
    const ext = path.extname(file);
    res.writeHead(200, {
      'content-type': MIME[ext] || 'application/octet-stream',
      'cache-control': 'no-store',
    });
    res.end(buf);
  });
}

const server = http.createServer(async (req, res) => {
  const u = new URL(req.url, `http://${req.headers.host || '127.0.0.1'}`);

  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'access-control-allow-origin': '*',
      'access-control-allow-methods': 'GET,POST,OPTIONS',
      'access-control-allow-headers': 'content-type',
    });
    res.end();
    return;
  }

  if (u.pathname === '/api/health') {
    return json(res, { ok: true, localNamePool: localNamePool.length, now: new Date().toISOString() });
  }

  if (u.pathname === '/api/perf') {
    return json(res, {
      now: new Date().toISOString(),
      stats: [...perfStats.entries()].reduce((acc, [k, v]) => {
        acc[k] = { avgMs: v.avgMs, count: v.count };
        return acc;
      }, {}),
    });
  }

  if (u.pathname === '/api/industry/knowledge') {
    const l2 = String(u.searchParams.get('l2') || '').trim();
    if (l2) {
      const row = industryKnowledge?.industries?.[l2] || null;
      return json(res, { updatedAt: industryKnowledge?.updatedAt || '', item: row });
    }
    const summary = Object.values(industryKnowledge?.industries || {}).map((x) => ({
      l1: x?.l1 || '',
      l2: x?.l2 || '',
      updatedAt: x?.updatedAt || '',
      sampleCount: Array.isArray(x?.sampleCompanies) ? x.sampleCompanies.length : 0,
    }));
    return json(res, { updatedAt: industryKnowledge?.updatedAt || '', count: summary.length, industries: summary });
  }

  if (u.pathname === '/api/industry/bootstrap') {
    const force = ['1', 'true', 'yes'].includes(String(u.searchParams.get('force') || '').toLowerCase());
    const l2 = String(u.searchParams.get('l2') || '').trim();
    const started = Date.now();
    const rows = await bootstrapIndustryKnowledge(force, l2);
    recordPerf('industry.bootstrap', Date.now() - started);
    return json(res, {
      ok: true,
      force,
      l2: l2 || '',
      count: rows.length,
      updatedAt: industryKnowledge?.updatedAt || '',
    });
  }

  if (u.pathname === '/api/industry/review') {
    const force = ['1', 'true', 'yes'].includes(String(u.searchParams.get('force') || '').toLowerCase());
    const targetRaw = Number(u.searchParams.get('target') || 100);
    const target = Number.isFinite(targetRaw) && targetRaw > 0 ? Math.min(200, Math.floor(targetRaw)) : 100;
    const started = Date.now();
    const report = await reviewIndustriesTarget(target, force);
    recordPerf('industry.review', Date.now() - started);
    return json(res, report);
  }

  if (u.pathname === '/api/industry/dynamic-overrides') {
    return json(res, {
      updatedAt: loadJson(DYNAMIC_COMPANY_INDUSTRY_OVERRIDES_PATH, { updatedAt: '' }).updatedAt || '',
      count: dynamicCompanyIndustryOverrides.length,
      rows: dynamicCompanyIndustryOverrides.slice(0, 200),
    });
  }

  if (u.pathname === '/api/industry/import-list' && req.method === 'POST') {
    try {
      const body = await readReqBody(req, 3 * 1024 * 1024);
      const maxItemsRaw = Number(u.searchParams.get('max') || 800);
      const maxItems = Number.isFinite(maxItemsRaw) ? Math.max(1, Math.min(2000, Math.floor(maxItemsRaw))) : 800;
      const started = Date.now();
      const out = await importIndustryOverridesFromCompanyList(body, maxItems);
      recordPerf('industry.import_list', Date.now() - started);
      return json(res, { ok: true, ...out });
    } catch (e) {
      return json(res, { ok: false, error: String(e?.message || e || 'import_failed') }, 400);
    }
  }

  if (u.pathname === '/api/suggest') {
    const q = (u.searchParams.get('q') || '').trim();
    if (!q) return json(res, { items: [] });
    const suggestCacheKey = `suggestFast:${q}`;
    const cached = cacheGet(suggestCacheKey);
    if (cached) return json(res, cached);
    const strictLegalQuery = shouldUseStrictSuggestMatch(q);
    const localFast = fastLocalSuggest(q, 12);
    const qNormLen = normalizeName(q).length;
    const token = extractIntentToken(q);
    const quickLocalItems = localFast
      .map((x) => {
        const dn = sanitizeLegalEntityName(String(x.displayName || x.name || '').trim());
        return { ...x, displayName: dn };
      })
      .filter((x) => looksLikeLegalEntityName(x.displayName || ''))
      .filter((x) => !isGenericLegalName(x.displayName || ''))
      .slice(0, 8);
    const quickScopedByIntent =
      token && quickLocalItems.some((x) => candidateIntentHit(q, x.name || '', x.displayName || ''))
        ? quickLocalItems.filter((x) => candidateIntentHit(q, x.name || '', x.displayName || ''))
        : quickLocalItems;
    const quickLocalStrict = strictLegalQuery
      ? quickScopedByIntent.filter((x) => hasStrongCoreMatch(q, x.displayName || x.name || ''))
      : quickScopedByIntent;
    const tokenCore = token ? stripLegalTail(coreCompanyName(String(q || '').replace(token, ''))) : '';
    const tokenCoreNorm = normalizeName(tokenCore);
    const quickLocalStrictScoped =
      tokenCoreNorm.length >= 2
        ? quickLocalStrict.filter((x) => normalizeName(x.displayName || x.name || '').includes(tokenCoreNorm))
        : quickLocalStrict;
    // For short abbreviations, local suggestions should return immediately.
    if (qNormLen <= 3 && quickLocalStrictScoped.length) {
      const out = { items: quickLocalStrictScoped, source: 'local_fast' };
      cacheSet(suggestCacheKey, out, 20 * 1000);
      return json(res, out);
    }
    // For common abbreviations, prefer local full legal names to keep first paint fast.
    if (qNormLen <= 6 && quickLocalStrictScoped.length >= 1) {
      const out = { items: quickLocalStrictScoped, source: 'local_fast' };
      cacheSet(suggestCacheKey, out, 20 * 1000);
      return json(res, out);
    }

    const remoteAll = [];
    const qs = buildSuggestQueriesForApi(q).slice(0, 2);
    const remoteRows = await Promise.all(qs.map((item) => withTimeout(eastmoneySuggest(item, 10), 800, [])));
    for (const rows of remoteRows) {
      if (rows.length) remoteAll.push(...rows);
    }
    const remote = remoteAll;
    const local = localSuggest(q);
    const localNames = localFast.length ? localFast : (remote.length ? localNameSuggest(q, 3) : localNameSuggest(q, 8));
    const needWebNames =
      strictLegalQuery &&
      !looksLikeLegalEntityName(q) &&
      !remote.length &&
      localFast.length < 3 &&
      qNormLen >= 4 &&
      qNormLen <= 8;
    const webNames = needWebNames ? await withTimeout(onlineLegalNameSuggest(q, 4), 700, []) : [];
    const qNorm = normalizeName(q);
    const minScore = strictLegalQuery ? 78 : qNorm.length <= 2 ? 65 : qNorm.length <= 4 ? 55 : 45;
    const merged = [];
    const seen = new Set();

    for (const r of [...remote, ...local, ...localNames, ...webNames]) {
      const key = `${r.code || 'NOCODE'}-${r.name}`;
      if (!r.name || seen.has(key)) continue;
      // Only keep A-share entities for online suggestion to avoid stock short names and derivatives.
      if (r.code && !isAStockCode(r.code)) continue;
      if (!r.code && overlapScoreEnhanced(qNorm, r.name) < minScore) continue;
      if (!r.code && strictLegalQuery && !hasStrongCoreMatch(q, r.name)) continue;
      seen.add(key);
      merged.push({ code: r.code, name: r.name, secid: r.secid || mapSecId(r.code) });
      if (merged.length >= 12) break;
    }
    const enriched = await Promise.all(
      merged.map(async (it, idx) => {
        if (!it.code) return { ...it, displayName: it.name };
        // Only resolve full-name for top items to control latency.
        if (idx > 1) return { ...it, displayName: it.name };
        const full = await withTimeout(fetchFullCompanyNameByCode(it.code), 450, '');
        return { ...it, displayName: full || it.name, aliases: aliasesByCode(it.code) };
      }),
    );
    const token2 = extractIntentToken(q);
    let enrichedScoped =
      token2 && enriched.some((x) => candidateIntentHit(q, x.name || '', x.displayName || ''))
        ? enriched.filter((x) => candidateIntentHit(q, x.name || '', x.displayName || ''))
        : enriched;
    if (token2 && isFinancialIntentToken(token2)) {
      const strict = enrichedScoped.filter((x) => candidateIntentHit(q, x.name || '', x.displayName || ''));
      if (strict.length) enrichedScoped = strict;
    }
    if ((!enrichedScoped.length || (token2 && isFinancialIntentToken(token2) && !enrichedScoped.length)) && token2) {
      const synthesized = synthesizeLegalNameCandidates(q)
        .map((x) => sanitizeLegalEntityName(x))
        .filter((x) => looksLikeLegalEntityName(x))
        .filter((x) => x.includes(token2))
        .slice(0, 3)
        .map((x) => ({ code: '', name: x, secid: '', displayName: x }));
      enrichedScoped = synthesized;
    }
    if (strictLegalQuery && looksLikeLegalEntityName(q)) {
      const qSan = sanitizeLegalEntityName(q);
      const exists = enrichedScoped.some((x) => sanitizeLegalEntityName(x.displayName || x.name || '') === qSan);
      if (!exists) {
        enrichedScoped.unshift({ code: '', name: q, secid: '', displayName: q, aliases: [] });
      }
    }
    enrichedScoped.sort((a, b) => {
      const asBase = Math.max(candidateMatchScore(q, a.name || '', a.displayName || ''), ...((a.aliases || []).map((x) => overlapScoreEnhanced(q, x))));
      const bsBase = Math.max(candidateMatchScore(q, b.name || '', b.displayName || ''), ...((b.aliases || []).map((x) => overlapScoreEnhanced(q, x))));
      const as = asBase + regionMatchBoost(q, a.displayName || a.name || '');
      const bs = bsBase + regionMatchBoost(q, b.displayName || b.name || '');
      return bs - as;
    });
    const seenName = new Set();
    const items = [];
    const qSan = sanitizeLegalEntityName(q);
    const fullLegalQuery = looksLikeLegalEntityName(q);
    const allowBranch = hasBranchIntent(q);
    for (const it of enrichedScoped) {
      const dn = sanitizeLegalEntityName(String(it.displayName || it.name || '').trim());
      if (!dn || seenName.has(dn)) continue;
      // Suggest list should show legal full company names only.
      if (!looksLikeLegalEntityName(dn)) continue;
      if (!allowBranch && isBranchEntityName(dn)) continue;
      if (isGenericLegalName(dn) && dn !== qSan) continue;
      const aliasHit = (it.aliases || []).some((x) => overlapScoreEnhanced(q, x) >= minScore);
      if (strictLegalQuery) {
        const strictHit = fullLegalQuery ? hasStrictLegalNameMatch(q, dn) : hasStrongCoreMatch(q, dn);
        if (!strictHit && !aliasHit) continue;
      }
      if (!strictLegalQuery && overlapScoreEnhanced(q, dn) < minScore && !aliasHit) continue;
      seenName.add(dn);
      items.push({ ...it, displayName: dn });
      if (items.length >= 12) break;
    }
    if (token2 && isFinancialIntentToken(token2)) {
      const tokenCore2 = stripLegalTail(coreCompanyName(String(q || '').replace(token2, '')));
      const coreNorm2 = normalizeName(tokenCore2);
      items.sort((a, b) => {
        const an = normalizeName(a.displayName || a.name || '');
        const bn = normalizeName(b.displayName || b.name || '');
        const aHit = coreNorm2 && an.includes(coreNorm2) ? 1 : 0;
        const bHit = coreNorm2 && bn.includes(coreNorm2) ? 1 : 0;
        if (aHit !== bHit) return bHit - aHit;
        return 0;
      });
      if (coreNorm2 && !items.some((x) => normalizeName(x.displayName || '').includes(coreNorm2))) {
        const financeSynth = sanitizeLegalEntityName(looksLikeLegalEntityName(q) ? q : `${q}有限公司`);
        if (looksLikeLegalEntityName(financeSynth) && !items.some((x) => sanitizeLegalEntityName(x.displayName || '') === financeSynth)) {
          items.unshift({ code: '', name: financeSynth, secid: '', displayName: financeSynth, aliases: [] });
        }
      }
    }
    if (strictLegalQuery && items.length > 1) {
      const exact = items.find((x) => sanitizeLegalEntityName(x.displayName || '') === qSan);
      if (exact) {
        const out = { items: [exact], source: remote.length ? 'eastmoney' : 'local_web_fallback' };
        cacheSet(suggestCacheKey, out, 20 * 1000);
        return json(res, out);
      }
    }
    const regionToken = queryRegionToken(q);
    if (regionToken && items.length > 1) {
      const sameCity = items.filter((x) => String(x.displayName || '').startsWith(regionToken));
      if (sameCity.length) {
        const out = { items: sameCity, source: remote.length ? 'eastmoney' : 'local_web_fallback' };
        cacheSet(suggestCacheKey, out, 20 * 1000);
        return json(res, out);
      }
    }
    if (regionToken && strictLegalQuery && !items.some((x) => String(x.displayName || '').startsWith(regionToken))) {
      const syntheticSameCity = synthesizeLegalNameCandidates(q)
        .map((x) => sanitizeLegalEntityName(x))
        .filter((x) => looksLikeLegalEntityName(x))
        .filter((x) => !isGenericLegalName(x))
        .filter((x) => !isBranchEntityName(x))
        .filter((x) => x.startsWith(regionToken))
        .slice(0, 1)
        .map((x) => ({ code: '', name: x, secid: '', displayName: x }));
      if (syntheticSameCity.length) {
        const out = { items: syntheticSameCity, source: 'local_web_fallback' };
        cacheSet(suggestCacheKey, out, 20 * 1000);
        return json(res, out);
      }
    }
    if (!items.length && strictLegalQuery) {
      const fallbackItems = synthesizeLegalNameCandidates(q)
        .map((x) => sanitizeLegalEntityName(x))
        .filter((x) => looksLikeLegalEntityName(x))
        .filter((x) => !isGenericLegalName(x))
        .filter((x) => allowBranch || !isBranchEntityName(x))
        .sort((a, b) => regionMatchBoost(q, b) - regionMatchBoost(q, a))
        .slice(0, 5)
        .map((x) => ({ code: '', name: x, secid: '', displayName: x }));
      if (fallbackItems.length) {
        const out = { items: fallbackItems, source: 'local_web_fallback' };
        cacheSet(suggestCacheKey, out, 20 * 1000);
        return json(res, out);
      }
    }
    const out = { items, source: remote.length ? 'eastmoney' : 'local_web_fallback' };
    cacheSet(suggestCacheKey, out, 20 * 1000);
    return json(res, out);
  }

  if (u.pathname === '/api/enrich') {
    const q = (u.searchParams.get('q') || '').trim();
    const disableSemiconFallback = ['0', 'false', 'off', 'no'].includes(String(u.searchParams.get('semicon_fallback') || '1').toLowerCase());
    if (!q) return json(res, { company: null, competitors: [], top5: [], suppliers: [], customers: [] });

    const ctx = await resolveCompanyContext(q);
    if (!ctx) return json(res, { company: null, competitors: [], top5: [], suppliers: [], customers: [] });
    const { candidate, secid, profile, nonListed } = ctx;
    const code = profile.code || candidate.code || '';

    const [revenue, brokerMeta, annual, financing] = await Promise.all([
      code ? withTimeout(fetchRevenue(code), 3000, { revenue: null, fiscalYear: null, source: '' }) : Promise.resolve({ revenue: null, fiscalYear: null, source: '' }),
      code ? withTimeout(brokerMetaForStock(code), 2500, { indvInduCode: '', indvInduName: '' }) : Promise.resolve({ indvInduCode: '', indvInduName: '' }),
      code ? withTimeout(extractAnnualRelations(code, 2024), 6500, { customers: [], suppliers: [], meta: { found: false } }) : Promise.resolve({ customers: [], suppliers: [], meta: { found: false } }),
      nonListed ? withTimeout(fetchNonListedFinancing(profile.name || candidate.name, 6), 3500, { roundsCount: null, events: [], source: '' }) : Promise.resolve({ roundsCount: null, events: [], source: '' }),
    ]);
    const industryCode = brokerMeta.indvInduCode || profile.industryCode || '';
    const industryName = brokerMeta.indvInduName || profile.industryName || '';
    const industry = classifyIndustryDetailed(`${profile.name || candidate.name || ''} ${industryName || profile.industryName || ''}`.trim());
    const isFinancialReviewIndustry = FINANCIAL_REVIEW_INDUSTRIES.has(industry.industryLevel2);
    const isChina500Fast = Boolean(findChina500ByName(profile.name || candidate.name || q));
    const consultingIntel = isFinancialReviewIndustry
      ? []
      : await withTimeout(fetchConsultingIntel(profile.name || candidate.name, profile.industryName || '', 10), 4500, []);
    const brokerPeers = industryCode ? await brokerReportIndustryPeers(industryCode, 2) : [];
    const forceTopDerivedCompetitors =
      Boolean(INDUSTRY_HEAD_SEED_CODES[industry.industryLevel2]) &&
      !sameIndustry(industry.industryName || '', industryName || '');
    const competitors = (forceTopDerivedCompetitors ? [] : brokerPeers)
      .filter((x) => String(x.code) !== String(code))
      .slice(0, 12)
      .map((x) => evidenceRow(x.name, {
        code: x.code,
        reason: `券商研报同业覆盖：${industry.industryName || x.industryName || '同业'}`,
        reportCount: x.reportCount || 0,
        brokerCount: x.brokerCount || 0,
        confidence: 0.72,
        sourceType: 'broker_report',
        sourceTier: 'tier2',
      }));

    let top5 = [];
    if (isFinancialReviewIndustry && nonListed) {
      top5 = buildFinancialTop5Fallback(industry.industryLevel2, revenue.fiscalYear || 2024, profile.name || candidate.name, 5);
    } else {
      const preferFineGrainedTop = !nonListed && industry.industryLevel2 && industry.industryLevel2 !== (industryName || '');
      const top5Raw =
        nonListed || preferFineGrainedTop
          ? await withTimeout(top5ByIndustryNameFallback(industry.industryName || industryName, 5), 6500, [])
          : await withTimeout(top5ByIndustry({
              code,
              name: profile.name || candidate.name,
              secid,
              industryName,
              industryCode,
            }), 6500, []);
      top5 = top5Raw.map((x) => ({
        ...x,
        sourceTier: 'tier1',
        sourceType: 'financial_statement',
        confidence: Number.isFinite(x.revenue) && x.revenue > 0 ? 0.92 : 0.65,
      }));
    }

    let competitorsFinal = !forceTopDerivedCompetitors && competitors.length
      ? competitors
      : top5
          .filter((x) => String(x.code) !== String(code))
          .slice(0, 10)
          .map((x) =>
            evidenceRow(x.name, {
              code: x.code,
              reason: `同属 ${industry.industryName || profile.industryName || '相关'} 领域（行业Top候选）`,
              confidence: 0.68,
              sourceType: 'industry_top_candidate',
              sourceTier: 'tier2',
            }),
          );
    if (consultingIntel.length) {
      const seen = new Set(competitorsFinal.map((x) => normalizeName(x.name)));
      const append = consultingIntel
        .filter((x) => x.name && !seen.has(normalizeName(x.name)))
        .map((x) =>
          evidenceRow(x.name, {
            code: '',
            reason: x.reason,
            confidence: x.confidence,
            sourceType: 'consulting_report',
            sourceTier: 'tier3',
            evidenceSnippet: x.sample || '',
          }),
        );
      competitorsFinal = [...competitorsFinal, ...append].slice(0, 20);
    }
    competitorsFinal = filterByEvidenceTier(competitorsFinal).slice(0, 20);
    if (!competitorsFinal.length) {
      competitorsFinal = buildChina500PeerFallback(profile.name || candidate.name, industry.industryLevel2, 10);
    }
    if (!competitorsFinal.length) {
      competitorsFinal = buildIndustryPeerFallback(industry.industryLevel2, profile.name || candidate.name, 10);
    }

    let customers = [];
    let suppliers = [];
    if (annual.customers?.length) {
      customers = normalizeAnnualRelationRows(annual.customers, '年报披露前五客户');
    }
    if (annual.suppliers?.length) {
      suppliers = normalizeAnnualRelationRows(annual.suppliers, '年报披露前五供应商');
    }
    const isSemiconIndustry = SEMICON_REVIEW_INDUSTRIES.has(industry.industryLevel2);
    if (!disableSemiconFallback && isSemiconIndustry) {
      suppliers = suppliers.length ? suppliers : buildSemiconLinkageRows('upstream', profile.name || candidate.name, 6);
      customers = customers.length ? customers : buildSemiconLinkageRows('downstream', profile.name || candidate.name, 6);
    }
    if (!isFinancialReviewIndustry && !isChina500Fast && (!customers.length || !suppliers.length)) {
      const [customersFetched, suppliersFetched] = await Promise.all([
        customers.length
          ? Promise.resolve(customers)
          : withTimeout(pickCustomersOnline(code, profile.name || candidate.name, industryName || profile.industryName, 20), 9000, []),
        suppliers.length
          ? Promise.resolve(suppliers)
          : withTimeout(pickSuppliers(code, profile.name || candidate.name, industryName || profile.industryName, 20), 9000, []),
      ]);
      customers = customers.length ? customers : customersFetched;
      suppliers = suppliers.length ? suppliers : suppliersFetched;
    }

    if (isFinancialReviewIndustry) {
      if (!top5.length) top5 = buildFinancialTop5Fallback(industry.industryLevel2, revenue.fiscalYear || 2024, profile.name || candidate.name, 5);
      if (!competitorsFinal.length) competitorsFinal = buildFinancialPeerFallback(industry.industryLevel2, profile.name || candidate.name, 10);
      suppliers = suppliers.length ? suppliers : buildFinancialLinkageRows(industry.industryLevel2, 'upstream', profile.name || candidate.name, 8);
      customers = customers.length ? customers : buildFinancialLinkageRows(industry.industryLevel2, 'downstream', profile.name || candidate.name, 8);
    }
    if (!disableSemiconFallback && isSemiconIndustry) {
      suppliers = suppliers.length ? suppliers : buildSemiconLinkageRows('upstream', profile.name || candidate.name, 6);
      customers = customers.length ? customers : buildSemiconLinkageRows('downstream', profile.name || candidate.name, 6);
    }
    if (isChina500Fast) {
      suppliers = suppliers.length ? suppliers : buildIndustryHintRows(industry.industryName || industryName, 'upstream', profile.name || candidate.name, 8);
      customers = customers.length ? customers : buildIndustryHintRows(industry.industryName || industryName, 'downstream', profile.name || candidate.name, 8);
    }
    suppliers = suppliers.length ? suppliers : buildIndustryHintRows(industry.industryName || industryName, 'upstream', profile.name || candidate.name, 6);
    customers = customers.length ? customers : buildIndustryHintRows(industry.industryName || industryName, 'downstream', profile.name || candidate.name, 6);
    if (!top5.length) {
      top5 = await withTimeout(top5ByIndustryNameFallback(industry.industryName || industryName, 5), 5000, []);
    }

    return json(res, {
      company: {
        code: profile.code || candidate.code,
        isListed: !nonListed,
        name: profile.name || candidate.name,
        secid,
        industryName: industry.industryName || industryName || profile.industryName || '',
        industryLevel1: industry.industryLevel1,
        industryLevel2: industry.industryLevel2,
        industryCode: industryCode || profile.industryCode || '',
        website: profile.website || '',
        revenue: revenue.revenue,
        fiscalYear: revenue.fiscalYear,
        revenueSource: revenue.source,
        totalMarketValue: profile.totalMarketValue,
        peTtm: profile.peTtm,
        pb: profile.pb,
        financing,
      },
      competitors: competitorsFinal,
      top5,
      suppliers,
      customers,
      source: {
        suggest: 'eastmoney_searchapi',
        profile: 'eastmoney_push2',
        revenue: revenue.source || 'not_found',
        customers: annual.customers?.length ? 'annual_report_pdf' : 'web_suggest_fallback',
        suppliers: annual.suppliers?.length ? 'annual_report_pdf' : 'web_suggest_fallback',
        annualReport: annual.meta || { found: false },
        localSearchPool: 'xlsx_uploaded_names_only',
        mode: nonListed ? 'non_listed_web_fallback' : 'listed_mode',
      },
    });
  }

  if (u.pathname === '/api/enrich-stream') {
    const q = (u.searchParams.get('q') || '').trim();
    sseInit(res);
    if (!q) {
      sseWrite(res, 'done', { ok: true });
      res.end();
      return;
    }
    try {
      const ctx = await resolveCompanyContext(q);
      if (!ctx) {
        sseWrite(res, 'company', { company: null });
        sseWrite(res, 'done', { ok: true });
        res.end();
        return;
      }
      const { candidate, secid, profile, nonListed } = ctx;
      const code = profile.code || candidate.code || '';
      const baseIndustry = classifyIndustryDetailed(`${profile.name || candidate.name || ''} ${profile.industryName || ''}`.trim());
      const isFinancialReviewIndustryBase = FINANCIAL_REVIEW_INDUSTRIES.has(baseIndustry.industryLevel2);
      const isChina500Fast = Boolean(findChina500ByName(profile.name || candidate.name || q));
      const baseCompany = {
        code,
        isListed: !nonListed,
        name: profile.name || candidate.name,
        secid,
        industryName: baseIndustry.industryName || profile.industryName || '',
        industryLevel1: baseIndustry.industryLevel1,
        industryLevel2: baseIndustry.industryLevel2,
        industryCode: profile.industryCode || '',
        website: profile.website || '',
        revenue: null,
        fiscalYear: null,
        revenueSource: '',
        totalMarketValue: profile.totalMarketValue,
        peTtm: profile.peTtm,
        pb: profile.pb,
        financing: { roundsCount: null, events: [], source: '' },
      };
      sseWrite(res, 'company', { company: baseCompany });
      sseWrite(res, 'eta', {
        competitorsMs: etaMs('competitors', nonListed ? 7000 : 4500),
        top5Ms: etaMs('top5', nonListed ? 6000 : 3500),
        suppliersMs: etaMs('suppliers', 5500),
        customersMs: etaMs('customers', 5500),
      });

      const pRevenue = code
        ? withTimeout(fetchRevenue(code), 3000, { revenue: null, fiscalYear: null, source: '' })
        : Promise.resolve({ revenue: null, fiscalYear: null, source: '' });
      const pBrokerMeta = code
        ? withTimeout(brokerMetaForStock(code), 3000, { indvInduCode: '', indvInduName: '' })
        : Promise.resolve({ indvInduCode: '', indvInduName: '' });
      const pAnnual = code
        ? withTimeout(extractAnnualRelations(code, 2024), 7000, { customers: [], suppliers: [], meta: { found: false } })
        : Promise.resolve({ customers: [], suppliers: [], meta: { found: false } });
      const pFinancing = nonListed
        ? withTimeout(fetchNonListedFinancing(baseCompany.name, 6), 4000, { roundsCount: null, events: [], source: '' })
        : Promise.resolve({ roundsCount: null, events: [], source: '' });

      pRevenue
        .then((revenue) => {
          sseWrite(res, 'company_update', {
            company: {
              ...baseCompany,
              revenue: revenue.revenue,
              fiscalYear: revenue.fiscalYear,
              revenueSource: revenue.source,
            },
          });
        })
        .catch(() => {});
      pFinancing
        .then((financing) => {
          sseWrite(res, 'company_update', {
            company: {
              ...baseCompany,
              financing,
            },
          });
        })
        .catch(() => {});

      const top5Task = withTimeout((async () => {
        const t0 = Date.now();
        const brokerMeta = await pBrokerMeta;
        const industryCode = brokerMeta.indvInduCode || profile.industryCode || '';
        const industryName = brokerMeta.indvInduName || profile.industryName || '';
        const industry = classifyIndustryDetailed(`${baseCompany.name || ''} ${industryName || profile.industryName || ''}`.trim());
        if (isFinancialReviewIndustryBase && nonListed) {
          const revenue = await pRevenue;
          const top5 = buildFinancialTop5Fallback(industry.industryLevel2, revenue.fiscalYear || 2024, baseCompany.name, 5);
          recordPerf('top5', Date.now() - t0);
          return { top5, industryName: industry.industryName || industryName, industryCode, industry };
        }
        const preferFineGrainedTop = !nonListed && industry.industryLevel2 && industry.industryLevel2 !== (industryName || '');
        const top5Raw =
          nonListed || preferFineGrainedTop
            ? await top5ByIndustryNameFallback(industry.industryName || industryName, 5)
            : await top5ByIndustry({
                code,
                name: baseCompany.name,
                secid,
                industryName,
                industryCode,
              });
        let top5 = top5Raw.map((x) => ({
          ...x,
          sourceTier: 'tier1',
          sourceType: 'financial_statement',
          confidence: Number.isFinite(x.revenue) && x.revenue > 0 ? 0.92 : 0.65,
        }));
        if (!top5.length && FINANCIAL_REVIEW_INDUSTRIES.has(industry.industryLevel2)) {
          const revenue = await pRevenue;
          top5 = buildFinancialTop5Fallback(industry.industryLevel2, revenue.fiscalYear || 2024, baseCompany.name, 5);
        }
        if (!top5.length) {
          top5 = await top5ByIndustryNameFallback(industry.industryName || industryName, 5);
        }
        recordPerf('top5', Date.now() - t0);
        return { top5, industryName: industry.industryName || industryName, industryCode, industry };
      })(), 6500, { top5: [], industryName: baseCompany.industryName || '', industryCode: '', industry: baseIndustry });

      const competitorsTask = withTimeout((async () => {
        const t0 = Date.now();
        const consultingIntel = isFinancialReviewIndustryBase
          ? []
          : await withTimeout(fetchConsultingIntel(baseCompany.name, profile.industryName || '', 10), 4500, []);
        const brokerMeta = await pBrokerMeta;
        const industryCode = brokerMeta.indvInduCode || profile.industryCode || '';
        const industryName = brokerMeta.indvInduName || profile.industryName || '';
        const industry = classifyIndustryDetailed(`${baseCompany.name || ''} ${industryName || profile.industryName || ''}`.trim());
        const brokerPeers = industryCode ? await brokerReportIndustryPeers(industryCode, 2) : [];
        const forceTopDerivedCompetitors =
          Boolean(INDUSTRY_HEAD_SEED_CODES[industry.industryLevel2]) &&
          !sameIndustry(industry.industryName || '', industryName || '');
        let competitors = forceTopDerivedCompetitors
          ? []
          : brokerPeers
          .filter((x) => String(x.code) !== String(code))
          .slice(0, 12)
          .map((x) => evidenceRow(x.name, {
            code: x.code,
            reason: `券商研报同业覆盖：${industry.industryName || x.industryName || '同业'}`,
            reportCount: x.reportCount || 0,
            brokerCount: x.brokerCount || 0,
            confidence: 0.72,
            sourceType: 'broker_report',
            sourceTier: 'tier2',
          }));
        if (!competitors.length) {
          const { top5 } = await top5Task;
          competitors = top5
            .filter((x) => String(x.code) !== String(code))
            .slice(0, 10)
            .map((x) =>
              evidenceRow(x.name, {
                code: x.code,
                reason: `同属 ${industry.industryName || profile.industryName || '相关'} 领域（行业Top候选）`,
                confidence: 0.68,
                sourceType: 'industry_top_candidate',
                sourceTier: 'tier2',
              }),
            );
        }
        if (consultingIntel.length) {
          const seen = new Set(competitors.map((x) => normalizeName(x.name)));
          const append = consultingIntel
            .filter((x) => x.name && !seen.has(normalizeName(x.name)))
            .map((x) =>
              evidenceRow(x.name, {
                code: '',
                reason: x.reason,
                confidence: x.confidence,
                sourceType: 'consulting_report',
                sourceTier: 'tier3',
                evidenceSnippet: x.sample || '',
              }),
            );
          competitors = [...competitors, ...append].slice(0, 20);
        }
        competitors = filterByEvidenceTier(competitors).slice(0, 20);
        if (!competitors.length && FINANCIAL_REVIEW_INDUSTRIES.has(industry.industryLevel2)) {
          competitors = buildFinancialPeerFallback(industry.industryLevel2, baseCompany.name, 10);
        }
        if (!competitors.length) {
          competitors = buildChina500PeerFallback(baseCompany.name, industry.industryLevel2, 10);
        }
        if (!competitors.length) {
          competitors = buildIndustryPeerFallback(industry.industryLevel2, baseCompany.name, 10);
        }
        recordPerf('competitors', Date.now() - t0);
        return competitors;
      })(), 8500, []);

      const customersTask = withTimeout((async () => {
        const t0 = Date.now();
        const annual = await pAnnual;
        if (annual.customers?.length) {
          recordPerf('customers', Date.now() - t0);
          return normalizeAnnualRelationRows(annual.customers, '年报披露前五客户');
        }
        if (isFinancialReviewIndustryBase) {
          const linked = buildFinancialLinkageRows(baseIndustry.industryLevel2, 'downstream', baseCompany.name, 8);
          recordPerf('customers', Date.now() - t0);
          return linked;
        }
        if (SEMICON_REVIEW_INDUSTRIES.has(baseIndustry.industryLevel2)) {
          const linked = buildSemiconLinkageRows('downstream', baseCompany.name, 6);
          recordPerf('customers', Date.now() - t0);
          return linked;
        }
        if (isChina500Fast) {
          const linked = buildIndustryHintRows(baseIndustry.industryName || profile.industryName || '', 'downstream', baseCompany.name, 8);
          recordPerf('customers', Date.now() - t0);
          return linked;
        }
        const out = await pickCustomersOnline(code, baseCompany.name, profile.industryName, 20);
        recordPerf('customers', Date.now() - t0);
        if (!out.length && SEMICON_REVIEW_INDUSTRIES.has(baseIndustry.industryLevel2)) {
          return buildSemiconLinkageRows('downstream', baseCompany.name, 6);
        }
        return out.length ? out : buildIndustryHintRows(baseIndustry.industryName || profile.industryName || '', 'downstream', baseCompany.name, 6);
      })(), 9000, []);

      const suppliersTask = withTimeout((async () => {
        const t0 = Date.now();
        const annual = await pAnnual;
        if (annual.suppliers?.length) {
          recordPerf('suppliers', Date.now() - t0);
          return normalizeAnnualRelationRows(annual.suppliers, '年报披露前五供应商');
        }
        if (isFinancialReviewIndustryBase) {
          const linked = buildFinancialLinkageRows(baseIndustry.industryLevel2, 'upstream', baseCompany.name, 8);
          recordPerf('suppliers', Date.now() - t0);
          return linked;
        }
        if (SEMICON_REVIEW_INDUSTRIES.has(baseIndustry.industryLevel2)) {
          const linked = buildSemiconLinkageRows('upstream', baseCompany.name, 6);
          recordPerf('suppliers', Date.now() - t0);
          return linked;
        }
        if (isChina500Fast) {
          const linked = buildIndustryHintRows(baseIndustry.industryName || profile.industryName || '', 'upstream', baseCompany.name, 8);
          recordPerf('suppliers', Date.now() - t0);
          return linked;
        }
        const out = await pickSuppliers(code, baseCompany.name, profile.industryName, 20);
        recordPerf('suppliers', Date.now() - t0);
        if (!out.length && SEMICON_REVIEW_INDUSTRIES.has(baseIndustry.industryLevel2)) {
          return buildSemiconLinkageRows('upstream', baseCompany.name, 6);
        }
        return out.length ? out : buildIndustryHintRows(baseIndustry.industryName || profile.industryName || '', 'upstream', baseCompany.name, 6);
      })(), 9000, []);

      top5Task
        .then((x) => sseWrite(res, 'top5', { rows: x.top5, industryName: x.industryName }))
        .catch(() => sseWrite(res, 'top5', { rows: [], industryName: baseCompany.industryName || '' }));
      competitorsTask
        .then((rows) => sseWrite(res, 'competitors', { rows }))
        .catch(() => sseWrite(res, 'competitors', { rows: [] }));
      customersTask
        .then((rows) => sseWrite(res, 'customers', { rows }))
        .catch(() => sseWrite(res, 'customers', { rows: [] }));
      suppliersTask
        .then((rows) => sseWrite(res, 'suppliers', { rows }))
        .catch(() => sseWrite(res, 'suppliers', { rows: [] }));

      await Promise.allSettled([top5Task, competitorsTask, customersTask, suppliersTask, pRevenue, pFinancing]);
      sseWrite(res, 'done', { ok: true });
      res.end();
      return;
    } catch {
      sseWrite(res, 'error', { message: 'stream failed' });
      res.end();
      return;
    }
  }

  serveStatic(req, res, u.pathname);
});

server.listen(PORT, HOST, () => {
  console.log(`server listening: http://${HOST}:${PORT}`);
  setTimeout(() => {
    bootstrapIndustryKnowledge(false).catch(() => {});
  }, 1200);
});
