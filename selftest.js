const runBtn = document.querySelector('#runBtn');
const summaryEl = document.querySelector('#summary');
const listEl = document.querySelector('#testList');

runBtn.addEventListener('click', runAllTests);
runAllTests();

async function api(path) {
  const res = await fetch(path);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

function ok(name, detail = '') {
  return { name, pass: true, detail };
}

function fail(name, detail = '') {
  return { name, pass: false, detail };
}

async function testSuggestHuataiQihuo() {
  const data = await api('/api/suggest?q=' + encodeURIComponent('华泰期货'));
  const items = Array.isArray(data.items) ? data.items : [];
  const names = items.map((x) => String(x.displayName || x.name || ''));
  if (!names.includes('华泰期货有限公司')) {
    return fail('华泰期货建议命中', `未包含“华泰期货有限公司”，当前：${names.join(' / ')}`);
  }
  if (names.some((x) => /正规公司|靠谱吗|手续费|开户|怎么样/.test(x))) {
    return fail('华泰期货建议去噪', `存在脏建议：${names.join(' / ')}`);
  }
  return ok('华泰期货建议命中与去噪', names.join(' / '));
}

async function testEnrichHuataiQihuo() {
  const data = await api('/api/enrich?q=' + encodeURIComponent('华泰期货'));
  const c = data.company || {};
  if (String(c.name || '') !== '华泰期货有限公司') {
    return fail('华泰期货主体识别', `主体错误：${String(c.name || '-')}`);
  }
  if (!/证券|期货/.test(String(c.industryName || ''))) {
    return fail('华泰期货行业识别', `行业异常：${String(c.industryName || '-')}`);
  }
  const top5 = Array.isArray(data.top5) ? data.top5 : [];
  if (!top5.length) {
    return fail('华泰期货Top5结果', 'Top5为空');
  }
  if (top5.some((x) => /医疗|造纸/.test(String(x.industryName || '') + String(x.name || '')))) {
    return fail('华泰期货Top5跨行业污染', JSON.stringify(top5.slice(0, 3), null, 2));
  }
  return ok('华泰期货 enrich 主体与Top5', `主体=${c.name}；行业=${c.industryName}`);
}

async function testSuggestFulin() {
  const data = await api('/api/suggest?q=' + encodeURIComponent('深圳复临科技有限公司'));
  const items = Array.isArray(data.items) ? data.items : [];
  const names = items.map((x) => String(x.displayName || x.name || ''));
  if (!names.includes('深圳复临科技有限公司')) {
    return fail('复临建议命中', `未命中目标主体，当前：${names.join(' / ')}`);
  }
  if (names.some((x) => /特锐德|科技有限公司$/.test(x) && x !== '深圳复临科技有限公司')) {
    return fail('复临建议污染', `存在污染项：${names.join(' / ')}`);
  }
  return ok('复临建议准确', names.join(' / '));
}

async function testYinhe() {
  const data = await api('/api/enrich?q=' + encodeURIComponent('银河证券'));
  const c = data.company || {};
  if (String(c.code || '') !== '601881') {
    return fail('银河证券主体代码', `期望601881，当前=${String(c.code || '-')}`);
  }
  if (!/证券/.test(String(c.industryName || ''))) {
    return fail('银河证券行业', `行业异常：${String(c.industryName || '-')}`);
  }
  return ok('银河证券识别正确', `主体=${c.name} (${c.code})`);
}

async function testGuangfaQihuo() {
  const data = await api('/api/enrich?q=' + encodeURIComponent('广发期货有限公司'));
  const c = data.company || {};
  if (!/广发/.test(String(c.name || '')) || !/期货/.test(String(c.name || ''))) {
    return fail('广发期货主体识别', `主体异常：${String(c.name || '-')}`);
  }
  const top5 = Array.isArray(data.top5) ? data.top5 : [];
  if (!top5.length) return fail('广发期货Top5', 'Top5为空');
  if (top5.some((x) => /医疗|造纸/.test(String(x.name || '') + String(x.industryName || '')))) {
    return fail('广发期货Top5跨行业污染', JSON.stringify(top5.slice(0, 5), null, 2));
  }
  return ok('广发期货行业回归', `主体=${c.name}；Top5=${top5.map((x) => x.name).join(' / ')}`);
}

async function runAllTests() {
  runBtn.disabled = true;
  runBtn.textContent = '自测中...';
  summaryEl.innerHTML = '';
  listEl.innerHTML = '';

  const tests = [testSuggestHuataiQihuo, testEnrichHuataiQihuo, testSuggestFulin, testYinhe, testGuangfaQihuo];
  const results = [];
  for (const t of tests) {
    try {
      results.push(await t());
    } catch (err) {
      results.push(fail(t.name, String(err?.message || err)));
    }
  }

  renderResults(results);
  runBtn.disabled = false;
  runBtn.textContent = '重新自测';
}

function renderResults(results) {
  const passCount = results.filter((x) => x.pass).length;
  const failCount = results.length - passCount;
  summaryEl.innerHTML = `
    <span class="badge ${failCount ? 'fail' : 'pass'}">总计 ${results.length}</span>
    <span class="badge pass">通过 ${passCount}</span>
    <span class="badge ${failCount ? 'fail' : 'pass'}">失败 ${failCount}</span>
  `;
  listEl.innerHTML = results
    .map(
      (r) => `
      <li class="test-item ${r.pass ? 'pass' : 'fail'}">
        <div class="test-title">${r.pass ? 'PASS' : 'FAIL'} · ${escapeHtml(r.name)}</div>
        <div class="test-detail">${escapeHtml(r.detail || '')}</div>
      </li>
    `,
    )
    .join('');
}

function escapeHtml(text) {
  return String(text)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}
