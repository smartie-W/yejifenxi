import { initializeApp } from 'https://www.gstatic.com/firebasejs/10.12.5/firebase-app.js';
import {
  getAuth,
  signInAnonymously,
  setPersistence,
  browserLocalPersistence,
} from 'https://www.gstatic.com/firebasejs/10.12.5/firebase-auth.js';
import {
  getFirestore,
  collection,
  getDocs,
  addDoc,
  doc,
  setDoc,
  getDoc,
  serverTimestamp,
  writeBatch,
  query,
  orderBy,
  onSnapshot,
  enableIndexedDbPersistence,
} from 'https://www.gstatic.com/firebasejs/10.12.5/firebase-firestore.js';

const allowedHosts = new Set(['localhost', '127.0.0.1']);
if (!allowedHosts.has(window.location.hostname)) {
  throw new Error('Access denied: local host only');
}

const firebaseConfig = {
  apiKey: 'AIzaSyBXgjqxXu1icjHxrIhal1ncQ6ZwqDr5E64',
  authDomain: 'xiaoshouyejifenxi.firebaseapp.com',
  projectId: 'xiaoshouyejifenxi',
  storageBucket: 'xiaoshouyejifenxi.firebasestorage.app',
  messagingSenderId: '951263111259',
  appId: '1:951263111259:web:0877b8556416dbb90ff77e',
  measurementId: 'G-YSQJYXYSNX',
};

const app = initializeApp(firebaseConfig);
const auth = getAuth(app);
const db = getFirestore(app);

const salesPeople = ['郭淼', '周思', '唐龙军', '王雪靖', '张柳云', '李彤'];
const contractTypes = ['SAAS', '私有部署订阅', '私有部署买断'];
const quarters = ['Q1', 'Q2', 'Q3', 'Q4'];
const periods = [...quarters, '年度'];

const contractChartsEl = document.getElementById('contract-charts');
const contractTypeAmountChartsEl = document.getElementById('contract-type-amount-charts');
const contractTypeCustomerChartsEl = document.getElementById('contract-type-customer-charts');
const paymentChartsNewEl = document.getElementById('payment-charts-new');
const paymentChartsOldEl = document.getElementById('payment-charts-old');
const paymentChartsTotalEl = document.getElementById('payment-charts-total');
const paymentTypeChartsEl = document.getElementById('payment-type-charts');
const paymentTypeSalesEl = document.getElementById('payment-type-sales');
let paymentProgressEl = document.getElementById('payment-progress-chart');

const contractForm = document.getElementById('contract-form');
const paymentForm = document.getElementById('payment-form');

const contractRecentEl = document.getElementById('contract-recent');
const paymentRecentEl = document.getElementById('payment-recent');
const syncStatusEl = document.getElementById('sync-status');
const modalOverlay = document.getElementById('modal-overlay');
const modalTitle = document.getElementById('modal-title');
const modalBody = document.getElementById('modal-body');
const modalPrev = document.getElementById('modal-prev');
const modalNext = document.getElementById('modal-next');
const modalAction = document.getElementById('modal-action');
const authOverlay = document.getElementById('auth-overlay');
const authForm = document.getElementById('auth-form');
const authCancel = document.getElementById('auth-cancel');
const eyeButtons = document.querySelectorAll('[data-eye]');
const binButtons = document.querySelectorAll('[data-bin]');
const customerInputs = document.querySelectorAll('input[name="customer"]');
const customerSuggestLists = document.querySelectorAll('.suggest-list[data-suggest="customer"]');

const tabButtons = document.querySelectorAll('.tab');
const panels = document.querySelectorAll('.panel');
let subTabButtons = document.querySelectorAll('.sub-tab');
let subPanels = document.querySelectorAll('.sub-panel');

const ensureProgressTab = () => {
  const tabsContainer = document.querySelector('.sub-tabs');
  if (!tabsContainer) return;
  const existingTab = tabsContainer.querySelector('[data-subtab="payment-progress"]');
  const existingPanel = document.getElementById('payment-progress');
  if (!existingTab) {
    const btn = document.createElement('button');
    btn.className = 'sub-tab';
    btn.dataset.subtab = 'payment-progress';
    btn.textContent = '到款进度分析';
    tabsContainer.insertBefore(btn, tabsContainer.firstChild);
  }
  if (!existingPanel) {
    const panel = document.createElement('section');
    panel.id = 'payment-progress';
    panel.className = 'sub-panel';
    panel.innerHTML = `
      <div class="card">
        <div class="card-head">
          <h2>到款进度分析</h2>
          <p>东区一组 · 到款金额 / 合计成本 / 实际计提金额</p>
        </div>
        <div id="payment-progress-chart" class="progress-chart"></div>
      </div>
    `;
    const paymentAll = document.getElementById('payment-all');
    if (paymentAll && paymentAll.parentNode) {
      paymentAll.parentNode.insertBefore(panel, paymentAll);
    }
  }
  paymentProgressEl = document.getElementById('payment-progress-chart');
};

tabButtons.forEach((btn) => {
  btn.addEventListener('click', () => {
    tabButtons.forEach((b) => b.classList.remove('active'));
    panels.forEach((p) => p.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById(btn.dataset.tab).classList.add('active');
  });
});

const bindSubTabs = () => {
  subTabButtons = document.querySelectorAll('.sub-tab');
  subPanels = document.querySelectorAll('.sub-panel');
  subTabButtons.forEach((btn) => {
    btn.addEventListener('click', () => {
      subTabButtons.forEach((b) => b.classList.remove('active'));
      subPanels.forEach((p) => p.classList.remove('active'));
      btn.classList.add('active');
      const target = document.getElementById(btn.dataset.subtab);
      if (target) target.classList.add('active');
    });
  });
};


const formatMoney = (value) => {
  if (!value || isNaN(value)) return '0';
  return Number(value).toLocaleString('zh-CN', {
    minimumFractionDigits: 0,
    maximumFractionDigits: 2,
  });
};

const normalizeText = (value) => String(value || '').trim();
const escapeHtml = (value) =>
  String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');

const hashString = (value) => {
  let hash = 5381;
  const str = String(value || '');
  for (let i = 0; i < str.length; i += 1) {
    hash = (hash * 33) ^ str.charCodeAt(i);
  }
  return (hash >>> 0).toString(16);
};

const makeSeedId = (prefix, parts) => {
  const key = parts.map((p) => String(p || '').trim()).join('|');
  return `${prefix}_${hashString(key)}`;
};

const toDateKey = (value) => {
  if (!value) return 0;
  const str = String(value).trim();
  const digits = str.replace(/\D/g, '');
  if (digits.length >= 8) {
    const y = Number(digits.slice(0, 4));
    const m = Number(digits.slice(4, 6));
    const d = Number(digits.slice(6, 8));
    if (Number.isFinite(y) && Number.isFinite(m) && Number.isFinite(d)) {
      return y * 10000 + m * 100 + d;
    }
  }
  const match = str.match(/^(\d{4})[\/.-](\d{1,2})[\/.-](\d{1,2})/);
  if (match) {
    const year = Number(match[1]);
    const month = Number(match[2]);
    const day = Number(match[3]);
    if (Number.isFinite(year) && Number.isFinite(month) && Number.isFinite(day)) {
      return year * 10000 + month * 100 + day;
    }
  }
  const parsed = new Date(str);
  if (!Number.isNaN(parsed.valueOf())) {
    return parsed.getFullYear() * 10000 + (parsed.getMonth() + 1) * 100 + parsed.getDate();
  }
  return 0;
};

const getEntryDateKey = (item) => {
  const displayed = formatEntryDate(item);
  const displayKey = toDateKey(displayed);
  if (displayKey) return displayKey;
  const direct = toDateKey(item.date || item.day || item.createdAt);
  if (direct) return direct;
  const year = Number(item.year);
  const month = Number(item.month);
  const day = Number(item.day);
  if (Number.isFinite(year) && Number.isFinite(month) && Number.isFinite(day)) {
    return year * 10000 + month * 100 + day;
  }
  return 0;
};

const parseNumber = (value) => {
  if (value === null || value === undefined) return 0;
  const num = Number(String(value).replace(/,/g, ''));
  return Number.isFinite(num) ? num : 0;
};

const formatInputNumber = (value) => {
  if (value === null || value === undefined) return '';
  const raw = String(value).replace(/,/g, '').replace(/[^\d.-]/g, '');
  if (raw === '' || raw === '-' || raw === '.') return raw;
  const [intPart, decPart] = raw.split('.');
  const intNum = intPart ? Number(intPart) : 0;
  const intFormatted = Number.isFinite(intNum)
    ? intNum.toLocaleString('zh-CN')
    : intPart;
  return decPart !== undefined ? `${intFormatted}.${decPart}` : intFormatted;
};

const bindAmountFormatting = () => {
  document.querySelectorAll('input[inputmode="decimal"]').forEach((input) => {
    input.addEventListener('input', (event) => {
      const { value } = event.target;
      event.target.value = formatInputNumber(value);
      const name = event.target.name;
      if (
        paymentForm &&
        paymentForm.contains(event.target) &&
        ['amount', 'secondDevCost', 'outsourcingCost', 'unplannedCost'].includes(name)
      ) {
        updateActualAccrual();
      }
    });
    input.value = formatInputNumber(input.value);
  });
};

const updateTotalCost = () => {
  const scope = paymentForm || document;
  const get = (name) => parseNumber(scope.querySelector(`input[name="${name}"]`)?.value);
  const total = get('secondDevCost') + get('outsourcingCost') + get('unplannedCost');
  const totalInput = scope.querySelector('input[name="totalCost"]');
  if (totalInput) {
    totalInput.value = formatInputNumber(total);
  }
  return total;
};

const updateActualAccrual = () => {
  const scope = paymentForm || document;
  const amount = parseNumber(scope.querySelector('input[name="amount"]')?.value);
  const totalCost = updateTotalCost();
  const actual = amount - totalCost;
  const actualInput = scope.querySelector('input[name="actualAccrual"]');
  if (actualInput) {
    actualInput.value = formatInputNumber(actual);
  }
};

const normalizePaymentEntry = (entry) => {
  const amount = parseNumber(entry.amount);
  const secondDevCost = parseNumber(entry.secondDevCost);
  const outsourcingCost = parseNumber(entry.outsourcingCost);
  const unplannedCost = parseNumber(entry.unplannedCost);
  const totalCost = secondDevCost + outsourcingCost + unplannedCost;
  const actualAccrual = amount - totalCost;
  return {
    ...entry,
    amount,
    secondDevCost,
    outsourcingCost,
    unplannedCost,
    totalCost,
    actualAccrual,
  };
};

const bindTotalCostCalc = () => {
  ['implementationFee', 'secondDevCost', 'outsourcingCost', 'unplannedCost'].forEach((name) => {
    const input = paymentForm?.querySelector(`input[name="${name}"]`);
    if (!input) return;
    input.addEventListener('input', () => {
      updateTotalCost();
      updateActualAccrual();
    });
  });
  const amountInput = paymentForm?.querySelector('input[name="amount"]');
  if (amountInput) {
    amountInput.addEventListener('input', updateActualAccrual);
  }
  if (paymentForm) {
    paymentForm.addEventListener('input', (event) => {
      const name = event.target?.name;
      if (['amount', 'secondDevCost', 'outsourcingCost', 'unplannedCost'].includes(name)) {
        updateActualAccrual();
      }
    });
  }
  updateTotalCost();
  updateActualAccrual();
};

const quarterFromDate = (dateStr) => {
  const date = new Date(dateStr);
  const month = date.getMonth() + 1;
  if (month <= 3) return 'Q1';
  if (month <= 6) return 'Q2';
  if (month <= 9) return 'Q3';
  return 'Q4';
};

const buildSelectOptions = () => {
  document.querySelectorAll('select[name="sales"]').forEach((select) => {
    select.innerHTML = [
      '<option value="" selected>请选择销售</option>',
      ...salesPeople.map((name) => `<option value="${name}">${name}</option>`),
    ].join('');
  });
};

const todayString = () => {
  const now = new Date();
  const pad = (n) => String(n).padStart(2, '0');
  return `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())}`;
};

const resetForms = () => {
  contractForm.reset();
  paymentForm.reset();
  document
    .querySelectorAll('select[name="sales"]')
    .forEach((select) => (select.value = ''));
};

const setDefaultDates = () => {
  const today = todayString();
  document.querySelectorAll('input[type="date"]').forEach((input) => {
    input.value = today;
  });
};

const loadJson = async (path) => {
  const res = await fetch(path);
  return res.json();
};

const collections = {
  contracts: 'contracts',
  payments: 'payments',
  meta: 'meta',
};

let baseContractData = [];
let basePaymentData = [];
let contractData = [];
let paymentData = [];
let kpiData = [];

let contractLogList = [];
let paymentLogList = [];
let contractLogViewList = [];
let paymentLogViewList = [];

let activeList = [];
let activeIndex = 0;
let activeType = 'contracts';
let activeMode = 'main';
let pendingAction = null;
let hasRendered = false;

let unsubscribeContracts = null;
let unsubscribePayments = null;
let contractSnapshotReady = false;
let paymentSnapshotReady = false;
let contractHasPendingWrites = false;
let paymentHasPendingWrites = false;
let lastSyncedAt = null;
let syncErrorMessage = '';

const cacheKeys = {
  contracts: 'yejifenxi_cache_contracts',
  payments: 'yejifenxi_cache_payments',
};

const readCache = (key) => {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : null;
  } catch (err) {
    return null;
  }
};

const writeCache = (key, data) => {
  try {
    localStorage.setItem(key, JSON.stringify(data));
  } catch (err) {
    // Ignore cache write failures (e.g., quota).
  }
};

const formatTime = (date) => {
  if (!date) return '';
  const h = String(date.getHours()).padStart(2, '0');
  const m = String(date.getMinutes()).padStart(2, '0');
  const s = String(date.getSeconds()).padStart(2, '0');
  return `${h}:${m}:${s}`;
};

const setSyncStatus = (text, cls) => {
  if (!syncStatusEl) return;
  syncStatusEl.textContent = `云端状态：${text}`;
  syncStatusEl.className = `sync-status ${cls}`;
};

const refreshSyncStatus = () => {
  if (!navigator.onLine) {
    setSyncStatus('离线，待联网后自动同步', 'sync-offline');
    return;
  }
  if (syncErrorMessage) {
    setSyncStatus(`同步失败（${syncErrorMessage}）`, 'sync-error');
    return;
  }
  if (!contractSnapshotReady || !paymentSnapshotReady) {
    setSyncStatus('连接中', 'sync-connecting');
    return;
  }
  if (contractHasPendingWrites || paymentHasPendingWrites) {
    setSyncStatus('同步中', 'sync-syncing');
    return;
  }
  if (lastSyncedAt) {
    setSyncStatus(`已同步 ${formatTime(lastSyncedAt)}`, 'sync-ok');
    return;
  }
  setSyncStatus('连接中', 'sync-connecting');
};

const getCustomerPool = () => {
  const pool = new Set();
  baseContractData.forEach((item) => {
    const name = normalizeText(item.customer);
    if (name) pool.add(name);
  });
  basePaymentData.forEach((item) => {
    const name = normalizeText(item.customer);
    if (name) pool.add(name);
  });
  contractData.forEach((item) => {
    const name = normalizeText(item.customer);
    if (name) pool.add(name);
  });
  paymentData.forEach((item) => {
    const name = normalizeText(item.customer);
    if (name) pool.add(name);
  });
  return Array.from(pool);
};

const getCustomerSuggestions = (query) => {
  const keyword = normalizeText(query);
  const pool = getCustomerPool();
  if (!keyword) return pool.slice(0, 30);
  const hits = pool.filter((name) => name.includes(keyword));
  return hits.slice(0, 30);
};

const bindCustomerAutoComplete = () => {
  if (!customerInputs.length || !customerSuggestLists.length) return;
  customerInputs.forEach((input, index) => {
    const list = customerSuggestLists[index];
    if (!list) return;
    const render = (query) => {
      const keyword = normalizeText(query);
      if (!keyword) {
        list.classList.remove('show');
        list.innerHTML = '';
        return;
      }
      const suggestions = getCustomerSuggestions(keyword);
      list.innerHTML = '';
      suggestions.forEach((name) => {
        const row = document.createElement('div');
        row.className = 'suggest-item';
        row.dataset.name = name;
        row.textContent = name;
        list.appendChild(row);
      });
      list.classList.toggle('show', suggestions.length > 0);
    };
    input.addEventListener('input', (event) => {
      render(event.target.value);
    });
    input.addEventListener('focus', (event) => {
      render(event.target.value);
    });
    input.addEventListener('blur', () => {
      setTimeout(() => list.classList.remove('show'), 120);
    });
    list.addEventListener('mousedown', (event) => {
      const item = event.target.closest('.suggest-item');
      if (!item) return;
      input.value = item.dataset.name || item.textContent || '';
    });
  });
};

const fetchCollection = async (name, options = {}) => {
  const { deletedOnly = false } = options;
  const q = query(collection(db, name), orderBy('createdAt', 'asc'));
  const snap = await getDocs(q);
  const rows = snap.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
  if (deletedOnly) {
    return rows.filter((row) => row.deletedAt);
  }
  return rows.filter((row) => !row.deletedAt);
};

const startRealtime = () => {
  if (unsubscribeContracts) unsubscribeContracts();
  if (unsubscribePayments) unsubscribePayments();

  const contractQuery = query(collection(db, collections.contracts), orderBy('createdAt', 'asc'));
  const paymentQuery = query(collection(db, collections.payments), orderBy('createdAt', 'asc'));

  unsubscribeContracts = onSnapshot(contractQuery, { includeMetadataChanges: true }, (snap) => {
    contractSnapshotReady = true;
    contractHasPendingWrites = snap.metadata.hasPendingWrites;
    if (!snap.metadata.hasPendingWrites && navigator.onLine) {
      lastSyncedAt = new Date();
      syncErrorMessage = '';
    }
    contractData = snap.docs
      .map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }))
      .filter((row) => !row.deletedAt);
    writeCache(cacheKeys.contracts, contractData);
    refreshSyncStatus();
    refresh();
  });

  unsubscribePayments = onSnapshot(paymentQuery, { includeMetadataChanges: true }, (snap) => {
    paymentSnapshotReady = true;
    paymentHasPendingWrites = snap.metadata.hasPendingWrites;
    if (!snap.metadata.hasPendingWrites && navigator.onLine) {
      lastSyncedAt = new Date();
      syncErrorMessage = '';
    }
    paymentData = snap.docs
      .map((docSnap) => normalizePaymentEntry({ id: docSnap.id, ...docSnap.data() }))
      .filter((row) => !row.deletedAt);
    writeCache(cacheKeys.payments, paymentData);
    refreshSyncStatus();
    refresh();
  });
};

const groupTotals = (items, periodKey) => {
  const totals = {};
  salesPeople.forEach((name) => {
    totals[name] = 0;
  });

  items.forEach((item) => {
    if (item.sales !== undefined && item[periodKey] !== undefined) {
      const person = item.sales;
      if (!totals.hasOwnProperty(person)) return;
      totals[person] += parseNumber(item.amount);
    }
  });

  return totals;
};

const buildContractEntries = (rows) => {
  return rows.map((row) => ({
    year: row['签约年度'],
    quarter: row['签约季度'],
    sales: row['销售人员'],
    amount: parseNumber(row['合同金额']),
    type: row['合同类型'],
    customer: row['客户名称'],
    date: row['日期'],
    day: row['日期'],
    month: row['签约月份'],
  }));
};

const buildPaymentEntries = (rows) => {
  return rows.map((row) => ({
    year: row['回款年'] || row['回款年限'],
    quarter: row['回款季度'],
    sales: row['销售人员'],
    amount: parseNumber(row['本次到款金额']),
    indicator: row['款项归属指标类型'],
    customerType: row['客户类型'],
    contractType: row['合同类型'],
    customer: row['客户名称'],
    secondDevProfit: parseNumber(row['本次到款-二开利润']),
    implementationFee: parseNumber(row['本次到款-实施费用']),
    secondDevCost: parseNumber(row['本次到款-二开成本']),
    outsourcingCost: parseNumber(row['本次到款-委外成本']),
    unplannedCost: parseNumber(row['本次到款-计划外成本']),
    totalCost: 0,
    actualAccrual: 0,
    date: row['日期'],
    day: row['日期'],
    month: row['回款月份'],
  })).map((entry) => normalizePaymentEntry(entry));
};

const seedCollection = async (name, items, getId) => {
  const chunkSize = 400;
  for (let i = 0; i < items.length; i += chunkSize) {
    const batch = writeBatch(db);
    const slice = items.slice(i, i + chunkSize);
    slice.forEach((item) => {
      const id = getId(item);
      const ref = doc(db, name, id);
      batch.set(ref, item, { merge: true });
    });
    await batch.commit();
  }
};

const ensureSeeded = async () => {
  const metaRef = doc(db, collections.meta, 'seeded');
  const metaSnap = await getDoc(metaRef);
  if (metaSnap.exists()) return;

  const contractSeed = baseContractData.map((item) => ({
    ...item,
    createdAt: serverTimestamp(),
    source: 'seed',
  }));
  const paymentSeed = basePaymentData.map((item) => ({
    ...item,
    createdAt: serverTimestamp(),
    source: 'seed',
  }));

  await seedCollection(collections.contracts, contractSeed, (item) =>
    makeSeedId('contract', [item.date, item.customer, item.sales, item.type, item.amount])
  );
  await seedCollection(collections.payments, paymentSeed, (item) =>
    makeSeedId('payment', [
      item.date,
      item.customer,
      item.sales,
      item.contractType,
      item.indicator,
      item.amount,
      item.actualAccrual,
    ])
  );
  await setDoc(metaRef, { seeded: true, createdAt: serverTimestamp() });
};

const buildKpiMap = (rows) => {
  const map = {};
  rows.forEach((row) => {
    const person = row['员工姓名'];
    const type = row['新老客户'];
    if (!person || !type) return;
    map[`${person}_${type}`] = {
      year: parseNumber(row['全年净回款金额']),
      Q1: parseNumber(row['第1季度净回款金额']),
      Q2: parseNumber(row['第2季度净回款金额']),
      Q3: parseNumber(row['第3季度净回款金额']),
      Q4: parseNumber(row['第4季度净回款金额']),
    };
  });
  return map;
};

const renderCharts = (
  container,
  titlePrefix,
  totalsByPeriod,
  targetsByPeriod = null,
  showPercent = false,
  headerRate = null
) => {
  container.innerHTML = '';
  periods.forEach((period) => {
    const chart = document.createElement('div');
    chart.className = 'chart';
    const periodTotal = Object.values(totalsByPeriod[period] || {}).reduce(
      (sum, val) => sum + parseNumber(val),
      0
    );
    const rateText = headerRate ? `完成率 ${headerRate(period)} · ` : '';
    chart.innerHTML = `<h4>${titlePrefix} · ${period}（${rateText}合计 ${formatMoney(
      periodTotal
    )}）</h4>`;

    const list = document.createElement('div');
    list.className = 'bar-list';

    const values = totalsByPeriod[period];
    const targetValues = targetsByPeriod ? targetsByPeriod[period] : null;
    const maxValue = Math.max(...Object.values(values), 1);
    const maxTarget = targetValues ? Math.max(...Object.values(targetValues), 1) : 1;
    const max = Math.max(maxValue, maxTarget, 1);

    salesPeople.forEach((name) => {
      const value = values[name] || 0;
      const target = targetValues ? targetValues[name] || 0 : 0;
      const row = document.createElement('div');
      row.className = 'bar-row';

      const label = document.createElement('div');
      label.textContent = name;

      const bar = document.createElement('div');
      bar.className = 'bar';

      const fill = document.createElement('span');
      fill.style.width = `${(value / max) * 100}%`;
      bar.appendChild(fill);

      if (targetValues) {
        const targetEl = document.createElement('div');
        targetEl.className = 'target';
        targetEl.style.width = `${(target / max) * 100}%`;
        bar.appendChild(targetEl);

        if (showPercent) {
          const percentEl = document.createElement('div');
          percentEl.className = 'percent';
          const percent = target > 0 ? (value / target) * 100 : 0;
          percentEl.textContent = `${percent.toFixed(1)}%`;
          bar.appendChild(percentEl);
        }
      }

      const valueEl = document.createElement('div');
      valueEl.textContent = formatMoney(value);
      if (targetValues && target) {
        valueEl.title = `KPI: ${formatMoney(target)}`;
      }

      row.appendChild(label);
      row.appendChild(bar);
      row.appendChild(valueEl);
      list.appendChild(row);
    });

    chart.appendChild(list);
    container.appendChild(chart);
  });
};

const renderProgressChart = (container, totals) => {
  if (!container) return;
  const values = [
    { label: '到款金额', value: parseNumber(totals.amount) },
    { label: '合计成本', value: parseNumber(totals.totalCost) },
    { label: '实际计提金额', value: parseNumber(totals.actualAccrual) },
  ];
  const max = Math.max(...values.map((v) => v.value), 1);
  container.innerHTML = values
    .map((item) => {
      const height = Math.max(4, Math.round((item.value / max) * 100));
      return `
        <div class="vbar">
          <div class="value">${formatMoney(item.value)}</div>
          <div class="track"><div class="fill" style="height:${height}%;"></div></div>
          <div class="label">${item.label}</div>
        </div>
      `;
    })
    .join('');
};

const computeContractTotals = (entries) => {
  const totals = {};
  periods.forEach((p) => (totals[p] = {}));
  salesPeople.forEach((name) => {
    periods.forEach((p) => (totals[p][name] = 0));
  });

  entries.forEach((item) => {
    const person = item.sales;
    if (!salesPeople.includes(person)) return;
    const q = item.quarter;
    const amount = parseNumber(item.amount);
    if (quarters.includes(q)) {
      totals[q][person] += amount;
    }
    totals['年度'][person] += amount;
  });

  return totals;
};

const computeContractTypeAmountTotals = (entries) => {
  const totals = {};
  periods.forEach((p) => (totals[p] = {}));
  contractTypes.forEach((type) => {
    periods.forEach((p) => (totals[p][type] = 0));
  });

  entries.forEach((item) => {
    const type = item.type;
    if (!contractTypes.includes(type)) return;
    const amount = parseNumber(item.amount);
    const q = item.quarter;
    if (quarters.includes(q)) {
      totals[q][type] += amount;
    }
    totals['年度'][type] += amount;
  });

  return totals;
};

const computeContractTypeCustomerTotals = (entries) => {
  const sets = {};
  periods.forEach((p) => (sets[p] = {}));
  contractTypes.forEach((type) => {
    periods.forEach((p) => (sets[p][type] = new Set()));
  });

  entries.forEach((item) => {
    const type = item.type;
    const customer = normalizeText(item.customer);
    if (!contractTypes.includes(type) || !customer) return;
    const q = item.quarter;
    if (quarters.includes(q)) {
      sets[q][type].add(customer);
    }
    sets['年度'][type].add(customer);
  });

  const totals = {};
  periods.forEach((p) => (totals[p] = {}));
  contractTypes.forEach((type) => {
    periods.forEach((p) => {
      totals[p][type] = sets[p][type].size;
    });
  });
  return totals;
};

const renderCategoryCharts = (container, titlePrefix, totalsByPeriod, categories, isCount = false) => {
  if (!container) return;
  container.innerHTML = '';
  periods.forEach((period) => {
    const chart = document.createElement('div');
    chart.className = 'chart';
    const periodTotal = Object.values(totalsByPeriod[period] || {}).reduce(
      (sum, val) => sum + parseNumber(val),
      0
    );
    const periodTotalText = isCount ? `${Math.round(periodTotal)}` : formatMoney(periodTotal);
    chart.innerHTML = `<h4>${titlePrefix} · ${period}（合计 ${periodTotalText}）</h4>`;

    const list = document.createElement('div');
    list.className = 'bar-list';
    const values = totalsByPeriod[period] || {};
    const max = Math.max(...Object.values(values), 1);

    categories.forEach((name) => {
      const value = parseNumber(values[name] || 0);
      const row = document.createElement('div');
      row.className = 'bar-row';

      const label = document.createElement('div');
      label.textContent = name;

      const bar = document.createElement('div');
      bar.className = 'bar';
      const fill = document.createElement('span');
      fill.style.width = `${(value / max) * 100}%`;
      bar.appendChild(fill);

      const valueEl = document.createElement('div');
      valueEl.textContent = isCount ? `${Math.round(value)}` : formatMoney(value);

      row.appendChild(label);
      row.appendChild(bar);
      row.appendChild(valueEl);
      list.appendChild(row);
    });

    chart.appendChild(list);
    container.appendChild(chart);
  });
};

const computePaymentTotals = (entries, indicatorType) => {
  const totals = {};
  periods.forEach((p) => (totals[p] = {}));
  salesPeople.forEach((name) => {
    periods.forEach((p) => (totals[p][name] = 0));
  });

  entries
    .filter((item) => item.indicator === indicatorType)
    .forEach((item) => {
      const person = item.sales;
      if (!salesPeople.includes(person)) return;
      const q = item.quarter;
      const amount = parseNumber(item.actualAccrual ?? item.amount);
      if (quarters.includes(q)) {
        totals[q][person] += amount;
      }
      totals['年度'][person] += amount;
    });

  return totals;
};

const computePaymentTotalsAll = (entries) => {
  const totals = {};
  periods.forEach((p) => (totals[p] = {}));
  salesPeople.forEach((name) => {
    periods.forEach((p) => (totals[p][name] = 0));
  });

  entries.forEach((item) => {
    const person = item.sales;
    if (!salesPeople.includes(person)) return;
    const q = item.quarter;
    const amount = parseNumber(item.actualAccrual ?? item.amount);
    if (quarters.includes(q)) {
      totals[q][person] += amount;
    }
    totals['年度'][person] += amount;
  });

  return totals;
};

const computePaymentTargets = (kpiMap, indicatorType) => {
  const totals = {};
  const kpiLabel = indicatorType === '新客户指标' ? '新客户' : '老客户';
  periods.forEach((p) => (totals[p] = {}));
  salesPeople.forEach((name) => {
    const key = `${name}_${kpiLabel}`;
    const data = kpiMap[key] || { Q1: 0, Q2: 0, Q3: 0, Q4: 0, year: 0 };
    totals['Q1'][name] = data.Q1 || 0;
    totals['Q2'][name] = data.Q2 || 0;
    totals['Q3'][name] = data.Q3 || 0;
    totals['Q4'][name] = data.Q4 || 0;
    totals['年度'][name] = data.year || 0;
  });
  return totals;
};

const computePaymentTargetsTotal = (kpiMap) => {
  const totals = {};
  periods.forEach((p) => (totals[p] = {}));
  salesPeople.forEach((name) => {
    const newKey = `${name}_新客户`;
    const oldKey = `${name}_老客户`;
    const newData = kpiMap[newKey] || { Q1: 0, Q2: 0, Q3: 0, Q4: 0, year: 0 };
    const oldData = kpiMap[oldKey] || { Q1: 0, Q2: 0, Q3: 0, Q4: 0, year: 0 };
    totals['Q1'][name] = (newData.Q1 || 0) + (oldData.Q1 || 0);
    totals['Q2'][name] = (newData.Q2 || 0) + (oldData.Q2 || 0);
    totals['Q3'][name] = (newData.Q3 || 0) + (oldData.Q3 || 0);
    totals['Q4'][name] = (newData.Q4 || 0) + (oldData.Q4 || 0);
    totals['年度'][name] = (newData.year || 0) + (oldData.year || 0);
  });
  return totals;
};

const computeCompletionRates = (totalsByPeriod, targetsByPeriod) => {
  const rates = {};
  periods.forEach((period) => {
    const total = Object.values(totalsByPeriod[period] || {}).reduce(
      (sum, val) => sum + parseNumber(val),
      0
    );
    const target = Object.values(targetsByPeriod[period] || {}).reduce(
      (sum, val) => sum + parseNumber(val),
      0
    );
    const percent = target > 0 ? (total / target) * 100 : 0;
    rates[period] = `${percent.toFixed(1)}%`;
  });
  return rates;
};

const paymentTypes = ['新签', '增购', '续约', '维保'];

const computePaymentTypeTotals = (entries, salesFilter = 'all') => {
  const totals = {};
  periods.forEach((p) => (totals[p] = {}));
  paymentTypes.forEach((type) => {
    periods.forEach((p) => (totals[p][type] = 0));
  });

  entries.forEach((item) => {
    if (salesFilter !== 'all' && item.sales !== salesFilter) return;
    const type = item.contractType;
    if (!paymentTypes.includes(type)) return;
    const q = item.quarter;
    const amount = parseNumber(item.actualAccrual ?? item.amount);
    if (quarters.includes(q)) {
      totals[q][type] += amount;
    }
    totals['年度'][type] += amount;
  });

  return totals;
};

const renderTypeCharts = (container, totalsByPeriod) => {
  if (!container) return;
  container.innerHTML = '';
  periods.forEach((period) => {
    const chart = document.createElement('div');
    chart.className = 'chart';
    const periodTotal = Object.values(totalsByPeriod[period] || {}).reduce(
      (sum, val) => sum + parseNumber(val),
      0
    );
    chart.innerHTML = `<h4>回款类型 · ${period}（合计 ${formatMoney(periodTotal)}）</h4>`;

    const list = document.createElement('div');
    list.className = 'bar-list';

    const values = totalsByPeriod[period];
    const maxValue = Math.max(...Object.values(values), 1);

    paymentTypes.forEach((type) => {
      const value = values[type] || 0;
      const row = document.createElement('div');
      row.className = 'bar-row';

      const label = document.createElement('div');
      label.textContent = type;

      const bar = document.createElement('div');
      bar.className = 'bar';

      const fill = document.createElement('span');
      fill.style.width = `${(value / maxValue) * 100}%`;
      bar.appendChild(fill);

      const percentEl = document.createElement('div');
      percentEl.className = 'percent';
      const percent = periodTotal > 0 ? (value / periodTotal) * 100 : 0;
      percentEl.textContent = `${percent.toFixed(1)}%`;
      bar.appendChild(percentEl);

      const valueEl = document.createElement('div');
      valueEl.textContent = formatMoney(value);

      row.appendChild(label);
      row.appendChild(bar);
      row.appendChild(valueEl);
      list.appendChild(row);
    });

    chart.appendChild(list);
    container.appendChild(chart);
  });
};

const formatEntryDate = (item) => {
  if (item.date && String(item.date).includes('-')) return item.date;
  const year = item.year || '';
  const month = item.month || '';
  const day = item.day || item.date || '';
  if (!year || !month || !day) return item.date || '';
  return `${year}/${String(month).padStart(2, '0')}/${String(day).padStart(2, '0')}`;
};

const renderRecent = (container, entries, formatter, type) => {
  const recent = entries;
  container.innerHTML = '';
  if (!recent.length) {
    container.textContent = '暂无新增记录';
    return;
  }
  recent.forEach((item, idx) => {
    const row = document.createElement('div');
    row.innerHTML = formatter(item);
    row.dataset.index = String(idx);
    row.dataset.type = type;
    row.dataset.mode = 'main';
    container.appendChild(row);
  });
};

const buildDetailRows = (items) => {
  return items
    .map(
      ({ label, value }) =>
        `<div class="row"><div class="label">${escapeHtml(label)}</div><div>${escapeHtml(value)}</div></div>`
    )
    .join('');
};

const getContractDetails = (item) => [
  { label: '日期', value: formatEntryDate(item) },
  { label: '客户名称', value: item.customer || '' },
  { label: '销售人员', value: item.sales || '' },
  { label: '合同类型', value: item.type || '' },
  { label: '合同金额', value: formatMoney(item.amount || 0) },
];

const getPaymentDetails = (item) => [
  { label: '日期', value: formatEntryDate(item) },
  { label: '客户名称', value: item.customer || '' },
  { label: '销售人员', value: item.sales || '' },
  { label: '客户类型', value: item.customerType || '' },
  { label: '指标类型', value: item.indicator || '' },
  { label: '回款类型', value: item.contractType || '' },
  { label: '到款金额', value: formatMoney(item.amount || 0) },
  { label: '二开利润', value: formatMoney(item.secondDevProfit || 0) },
  { label: '实施费用', value: formatMoney(item.implementationFee || 0) },
  { label: '二开成本', value: formatMoney(item.secondDevCost || 0) },
  { label: '委外成本', value: formatMoney(item.outsourcingCost || 0) },
  { label: '计划外成本', value: formatMoney(item.unplannedCost || 0) },
  { label: '合计成本', value: formatMoney(item.totalCost || 0) },
  { label: '实际计提金额', value: formatMoney(item.actualAccrual || 0) },
];

const openModal = (list, index, type, mode) => {
  activeList = list;
  activeIndex = index;
  activeType = type;
  activeMode = mode;
  const item = activeList[activeIndex];
  if (!item) return;
  modalTitle.textContent = mode === 'bin' ? '回收站详情' : '数据详情';
  const rows = type === 'contracts' ? getContractDetails(item) : getPaymentDetails(item);
  modalBody.innerHTML = buildDetailRows(rows);
  modalAction.textContent = mode === 'bin' ? '恢复' : '删除';
  modalOverlay.classList.remove('hidden');
};

const closeModal = () => {
  modalOverlay.classList.add('hidden');
};

const openAuth = (action) => {
  pendingAction = action;
  authForm.reset();
  authOverlay.classList.remove('hidden');
};

const closeAuth = () => {
  authOverlay.classList.add('hidden');
  pendingAction = null;
};

const refresh = () => {
  const contractTotals = computeContractTotals(contractData);
  renderCharts(contractChartsEl, '新签合同', contractTotals);
  const contractTypeAmountTotals = computeContractTypeAmountTotals(contractData);
  const contractTypeCustomerTotals = computeContractTypeCustomerTotals(contractData);
  renderCategoryCharts(
    contractTypeAmountChartsEl,
    '合同类型金额',
    contractTypeAmountTotals,
    contractTypes,
    false
  );
  renderCategoryCharts(
    contractTypeCustomerChartsEl,
    '合同类型客户数',
    contractTypeCustomerTotals,
    contractTypes,
    true
  );

  const kpiMap = buildKpiMap(kpiData);
  const paymentTotalsNew = computePaymentTotals(paymentData, '新客户指标');
  const paymentTotalsOld = computePaymentTotals(paymentData, '老客户指标');
  const paymentTargetsNew = computePaymentTargets(kpiMap, '新客户指标');
  const paymentTargetsOld = computePaymentTargets(kpiMap, '老客户指标');
  const paymentTotalsTotal = computePaymentTotalsAll(paymentData);
  const paymentTargetsTotal = computePaymentTargetsTotal(kpiMap);
  const paymentRatesNew = computeCompletionRates(paymentTotalsNew, paymentTargetsNew);
  const paymentRatesOld = computeCompletionRates(paymentTotalsOld, paymentTargetsOld);
  const paymentRatesTotal = computeCompletionRates(paymentTotalsTotal, paymentTargetsTotal);
  const paymentTypeTotals = computePaymentTypeTotals(
    paymentData,
    paymentTypeSalesEl?.value || 'all'
  );

  renderCharts(
    paymentChartsNewEl,
    '新客户指标',
    paymentTotalsNew,
    paymentTargetsNew,
    true,
    (p) => paymentRatesNew[p]
  );
  renderCharts(
    paymentChartsOldEl,
    '老客户指标',
    paymentTotalsOld,
    paymentTargetsOld,
    true,
    (p) => paymentRatesOld[p]
  );
  renderCharts(
    paymentChartsTotalEl,
    '新老客户指标合计',
    paymentTotalsTotal,
    paymentTargetsTotal,
    true,
    (p) => paymentRatesTotal[p]
  );
  renderTypeCharts(paymentTypeChartsEl, paymentTypeTotals);

  const progressTotals = paymentData.reduce(
    (acc, item) => {
      acc.amount += parseNumber(item.amount);
      acc.totalCost += parseNumber(item.totalCost);
      acc.actualAccrual += parseNumber(item.actualAccrual);
      return acc;
    },
    { amount: 0, totalCost: 0, actualAccrual: 0 }
  );
  renderProgressChart(paymentProgressEl, progressTotals);

  contractLogList = contractData.filter((item) => parseNumber(item.amount) > 0);
  contractLogViewList = contractLogList
    .slice()
    .sort((a, b) => getEntryDateKey(b) - getEntryDateKey(a));
  renderRecent(contractRecentEl, contractLogViewList, (item) => {
    const date = formatEntryDate(item);
    return `<span>${escapeHtml(date)} | 客户：${escapeHtml(
      item.customer || ''
    )} | 销售：${escapeHtml(item.sales || '')} | 合同类型：${escapeHtml(
      item.type || ''
    )}</span><span>${formatMoney(item.amount)}</span>`;
  }, 'contracts');

  paymentLogList = paymentData.filter((item) => {
    const amount = parseNumber(item.amount);
    const actual = parseNumber(item.actualAccrual);
    const extra =
      parseNumber(item.secondDevProfit) +
      parseNumber(item.implementationFee) +
      parseNumber(item.secondDevCost) +
      parseNumber(item.outsourcingCost) +
      parseNumber(item.unplannedCost) +
      parseNumber(item.totalCost);
    return amount > 0 || actual > 0 || extra > 0;
  });
  paymentLogViewList = paymentLogList
    .slice()
    .sort((a, b) => getEntryDateKey(b) - getEntryDateKey(a));
  renderRecent(paymentRecentEl, paymentLogViewList, (item) => {
    const date = formatEntryDate(item);
    return `<span>${escapeHtml(date)} | 客户：${escapeHtml(
      item.customer || ''
    )} | 销售：${escapeHtml(item.sales || '')} | 客户类型：${escapeHtml(
      item.customerType || ''
    )} | 指标类型：${escapeHtml(item.indicator || '')} | 回款类型：${escapeHtml(
      item.contractType || ''
    )} | 二开利润：${formatMoney(
      item.secondDevProfit || 0
    )} | 实施费用：${formatMoney(item.implementationFee || 0)} | 二开成本：${formatMoney(
      item.secondDevCost || 0
    )} | 委外成本：${formatMoney(item.outsourcingCost || 0)} | 计划外成本：${formatMoney(
      item.unplannedCost || 0
    )} | 合计成本：${formatMoney(item.totalCost || 0)} | 实际计提：${formatMoney(
      item.actualAccrual || 0
    )}</span><span>${formatMoney(item.amount)}</span>`;
  }, 'payments');

  if (!hasRendered) {
    document.body.classList.remove('is-loading');
    hasRendered = true;
  }
};

contractForm.addEventListener('submit', (event) => {
  event.preventDefault();
  const formData = new FormData(contractForm);
  const date = formData.get('date');
  const amount = parseNumber(formData.get('amount'));
  const entry = {
    year: date.slice(0, 4),
    quarter: quarterFromDate(date),
    sales: formData.get('sales'),
    amount,
    type: formData.get('type'),
    customer: formData.get('customer'),
    date,
    month: String(new Date(date).getMonth() + 1),
    createdAt: serverTimestamp(),
    source: 'manual',
  };

  addDoc(collection(db, collections.contracts), entry)
    .then(() => {
      contractForm.reset();
      setDefaultDates();
    })
    .catch((err) => {
      console.error(err);
      syncErrorMessage = err?.code || '写入失败';
      refreshSyncStatus();
      alert('新签合同登记失败，请检查网络后重试。');
    });
});

paymentForm.addEventListener('submit', (event) => {
  event.preventDefault();
  const formData = new FormData(paymentForm);
  const date = formData.get('date');
  const amount = parseNumber(formData.get('amount'));
  const totalCost = updateTotalCost();
  const actualAccrual = amount - totalCost;
  const entry = {
    year: date.slice(0, 4),
    quarter: quarterFromDate(date),
    sales: formData.get('sales'),
    amount,
    indicator: formData.get('indicatorType'),
    customerType: formData.get('customerType'),
    contractType: formData.get('contractType'),
    customer: formData.get('customer'),
    secondDevProfit: parseNumber(formData.get('secondDevProfit')),
    implementationFee: parseNumber(formData.get('implementationFee')),
    secondDevCost: parseNumber(formData.get('secondDevCost')),
    outsourcingCost: parseNumber(formData.get('outsourcingCost')),
    unplannedCost: parseNumber(formData.get('unplannedCost')),
    totalCost,
    actualAccrual,
    date,
    month: String(new Date(date).getMonth() + 1),
    createdAt: serverTimestamp(),
    source: 'manual',
  };

  addDoc(collection(db, collections.payments), entry)
    .then(() => {
      paymentForm.reset();
      setDefaultDates();
      updateTotalCost();
      updateActualAccrual();
    })
    .catch((err) => {
      console.error(err);
      syncErrorMessage = err?.code || '写入失败';
      refreshSyncStatus();
      alert('销售回款登记失败，请检查网络后重试。');
    });
});

modalOverlay.addEventListener('click', (event) => {
  if (event.target === modalOverlay) closeModal();
});

authOverlay.addEventListener('click', (event) => {
  if (event.target === authOverlay) closeAuth();
});

modalPrev.addEventListener('click', () => {
  if (!activeList.length) return;
  activeIndex = (activeIndex - 1 + activeList.length) % activeList.length;
  openModal(activeList, activeIndex, activeType, activeMode);
});

modalNext.addEventListener('click', () => {
  if (!activeList.length) return;
  activeIndex = (activeIndex + 1) % activeList.length;
  openModal(activeList, activeIndex, activeType, activeMode);
});

modalAction.addEventListener('click', () => {
  openAuth(activeMode === 'bin' ? 'restore' : 'delete');
});

authCancel.addEventListener('click', closeAuth);

authForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  const formData = new FormData(authForm);
  const username = formData.get('username');
  const password = formData.get('password');
  if (username !== 'wangqiming' || password !== 'wqm211700') {
    alert('账号或密码错误');
    return;
  }

  const item = activeList[activeIndex];
  if (!item) return;

  const mainName = activeType === 'contracts' ? collections.contracts : collections.payments;

  if (pendingAction === 'delete') {
    if (item.id) {
      await setDoc(doc(db, mainName, item.id), { deletedAt: serverTimestamp() }, { merge: true });
    }
  }

  if (pendingAction === 'restore') {
    if (item.id) {
      await setDoc(
        doc(db, mainName, item.id),
        { deletedAt: null, restoredAt: serverTimestamp() },
        { merge: true }
      );
    }
  }

  closeAuth();
  closeModal();
});

const init = async () => {
  ensureProgressTab();
  bindSubTabs();
  buildSelectOptions();
  resetForms();
  setDefaultDates();
  bindAmountFormatting();
  bindTotalCostCalc();
  bindCustomerAutoComplete();
  window.addEventListener('online', refreshSyncStatus);
  window.addEventListener('offline', refreshSyncStatus);
  refreshSyncStatus();
  if (paymentTypeSalesEl) {
    paymentTypeSalesEl.addEventListener('change', refresh);
  }
  contractRecentEl.addEventListener('click', (event) => {
    const row = event.target.closest('div[data-index]');
    if (!row) return;
    const idx = Number(row.dataset.index);
    openModal(contractLogViewList, idx, 'contracts', 'main');
  });
  paymentRecentEl.addEventListener('click', (event) => {
    const row = event.target.closest('div[data-index]');
    if (!row) return;
    const idx = Number(row.dataset.index);
    openModal(paymentLogViewList, idx, 'payments', 'main');
  });
  binButtons.forEach((btn) => {
    btn.addEventListener('click', async () => {
      const type = btn.dataset.bin;
      const mainName = type === 'contracts' ? collections.contracts : collections.payments;
      const list = await fetchCollection(mainName, { deletedOnly: true });
      if (!list.length) {
        alert('回收站暂无数据');
        return;
      }
      const sorted = list.slice().sort((a, b) => getEntryDateKey(b) - getEntryDateKey(a));
      openModal(sorted, 0, type, 'bin');
    });
  });

  const seedDataPromise = Promise.all([
    loadJson('data/合同管理-新签.json'),
    loadJson('data/回款明细账.json'),
    loadJson('data/目标数据.json'),
  ]);

  const cachedContracts = readCache(cacheKeys.contracts);
  const cachedPayments = readCache(cacheKeys.payments);
  if (cachedContracts) {
    contractData = cachedContracts;
  }
  if (cachedPayments) {
    paymentData = cachedPayments;
  }
  if (cachedContracts || cachedPayments) {
    refresh();
  }

  const [contractsRaw, paymentsRaw, kpiRaw] = await seedDataPromise;
  baseContractData = buildContractEntries(contractsRaw);
  basePaymentData = buildPaymentEntries(paymentsRaw);
  kpiData = kpiRaw;
  if (!cachedContracts && !cachedPayments) {
    // First load without cache: render bundled baseline data before cloud sync.
    contractData = baseContractData.slice();
    paymentData = basePaymentData.slice();
    refresh();
  } else {
    // Cached render may have used empty KPI temporarily.
    refresh();
  }

  try {
    try {
      await enableIndexedDbPersistence(db);
    } catch (err) {
      // Ignore persistence errors (multi-tab or unsupported).
    }
    await setPersistence(auth, browserLocalPersistence);
    await signInAnonymously(auth);
    await ensureSeeded();
    startRealtime();
    refreshSyncStatus();
  } catch (err) {
    console.error(err);
    syncErrorMessage = err?.code || '连接异常';
    refreshSyncStatus();
    alert('云端连接失败，请在 Firebase Authentication 的登录方法中启用匿名登录。');
  }
};

init();
