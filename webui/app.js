/* ==================================================================
 * B站监控插件 WebUI 前端（计划 todo 12）
 * 纯原生 JS + fetch，无构建工具链、无 CDN、完全离线可用。
 *
 * 与后端（webui/server.py，todo 11）的 API 契约：
 *   GET  /api/subscriptions -> {subscriptions:[...]}
 *   POST /api/subscriptions   body {subscriptions:[...]}
 *     200 {ok,count,rebuild} | 400 {ok:false,error,rejected:[{index,reason}],errors:[...]}
 *     任一条目被拒则整表 400、不部分落盘。
 *   GET  /api/status        -> {sub_id:{last_poll,last_error,error_count,
 *                                live_status,last_push_at,auto_disabled}}
 *   GET/POST /api/settings  credential/poll/webui 三组（webui 含 token）
 *   POST /api/test-push      body {session,message} -> {ok,detail}
 *   GET  /api/logs?tail=N   -> {logs:[...],total}
 *   鉴权：Authorization: Bearer <token>；401 {error:"unauthorized"} ->
 *   清除本地 token 并回到令牌门（绝不静默失败）。
 * ================================================================== */
'use strict';

/* ---------- 常量 ---------- */

var TOKEN_KEY = 'bilibili_webui_token';
var SUB_TYPE_LABELS = { live: '直播', dynamic: '动态', collection: '合集' };
var SUB_TYPE_TO_EVENT = { live: 'live_on', dynamic: 'dynamic', collection: 'collection' };
var POLL_DEFAULT_SEC = 300;
var STATUS_REFRESH_MS = 10000;
var LOG_REFRESH_MS = 5000;
var LOGIN_REFRESH_MS = 30000;
var LOG_TAIL = 200;

/* ---------- 状态 ---------- */

var state = {
  token: localStorage.getItem(TOKEN_KEY) || '',
  subs: [],              /* 订阅行（wire 形态：id 仅在既有行携带） */
  settings: null,
  settingsLoaded: false,
  status: {},
  activeTab: 'subs',
  editingIndex: null,    /* state.subs 下标；null = 新增 */
  editingId: null,       /* 打开编辑器时快照的订阅 id（按 id 写回，防错位） */
  editingToken: 0,       /* 编辑器代次（迟到的异步回调据此识别过期弹窗） */
  statusTimer: null,
  logTimer: null,
  loginTimer: null,
  logAutoScroll: true,
};

/* ---------- 小工具 ---------- */

function $(sel, root) {
  return (root || document).querySelector(sel);
}

function esc(value) {
  return String(value == null ? '' : value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

/* ISO 时间 -> UTC+8 "YYYY-MM-DD HH:MM:SS"；空/非法 -> "—" */
function fmtTime(iso) {
  if (!iso) return '—';
  var d = new Date(iso);
  if (isNaN(d.getTime())) return String(iso);
  // 统一按 UTC+8（中国标准时间）展示，与后端推送事件时间一致
  var cst = new Date(d.getTime() + 8 * 3600 * 1000);
  function p(n) { return String(n).padStart(2, '0'); }
  return cst.getUTCFullYear() + '-' + p(cst.getUTCMonth() + 1) + '-' + p(cst.getUTCDate()) +
    ' ' + p(cst.getUTCHours()) + ':' + p(cst.getUTCMinutes()) + ':' + p(cst.getUTCSeconds());
}

/* 数字输入读取：空 -> fallback；非法 -> {value:null, err} */
function readNumInput(sel, fallback) {
  var raw = $(sel).value.trim();
  if (raw === '') return { value: fallback, err: null };
  var n = Number(raw);
  if (isNaN(n)) return { value: null, err: '请输入数字' };
  return { value: n, err: null };
}

/* ---------- Toast / 连接灯 ---------- */

var toastTimer = null;

function toast(msg, kind) {
  var el = $('#toast');
  el.textContent = msg;
  el.className = 'toast' + (kind ? ' show' : ' show');
  if (toastTimer) clearTimeout(toastTimer);
  toastTimer = setTimeout(function () {
    el.classList.remove('show');
  }, 2600);
}

function setConn(ok, text) {
  var dot = $('.conn-dot');
  var label = $('#conn-text');
  if (dot) dot.className = 'conn-dot ' + (ok ? 'ok' : 'err');
  if (label) label.textContent = text;
}

/* ---------- 鉴权 / 令牌门 ---------- */

function getToken() {
  return state.token;
}

function setToken(t) {
  state.token = t || '';
  if (t) {
    localStorage.setItem(TOKEN_KEY, t);
  } else {
    localStorage.removeItem(TOKEN_KEY);
  }
}

function setTokenMsg(text, kind) {
  var el = $('#token-msg');
  el.textContent = text;
  el.className = 'msg' + (text ? ' ' + (kind || 'info') : '');
}

function showTokenGate(msg) {
  stopTimers();
  $('#app').classList.add('hidden');
  $('#token-gate').classList.remove('hidden');
  setTokenMsg(msg || '', msg ? 'error' : '');
  $('#token-input').value = state.token || '';
  setTimeout(function () { $('#token-input').focus(); }, 60);
}

function hideTokenGate() {
  $('#token-gate').classList.add('hidden');
  $('#app').classList.remove('hidden');
}

function handleUnauthorized(msg) {
  setToken('');
  showTokenGate(msg || '令牌无效或已过期，请重新输入');
}

/* ---------- API 封装 ---------- */

function api(path, opts) {
  opts = opts || {};
  var headers = { 'Authorization': 'Bearer ' + getToken() };
  var init = { method: opts.method || 'GET', headers: headers };
  if (opts.body !== undefined) {
    init.body = JSON.stringify(opts.body);
    headers['Content-Type'] = 'application/json';
  }
  return fetch(path, init).then(function (resp) {
    if (resp.status === 401) {
      var authErr = new Error('unauthorized');
      authErr.status = 401;
      handleUnauthorized('令牌无效或已过期，请重新输入（插件日志中打印的 token）');
      throw authErr;
    }
    setConn(true, '已连接');
    return resp.json().catch(function () { return null; }).then(function (data) {
      if (!resp.ok) {
        var e = new Error((data && data.error) || ('HTTP ' + resp.status));
        e.status = resp.status;
        e.data = data;
        throw e;
      }
      return data;
    });
  }).catch(function (err) {
    if (err && err.status === 401) throw err;
    setConn(false, '连接失败');
    throw err;
  });
}

/* ---------- Tab ---------- */

function switchTab(name) {
  state.activeTab = name;
  var tabs = document.querySelectorAll('.tab');
  for (var i = 0; i < tabs.length; i++) {
    tabs[i].classList.toggle('active', tabs[i].dataset.tab === name);
    tabs[i].setAttribute('aria-selected', tabs[i].dataset.tab === name ? 'true' : 'false');
  }
  var panels = document.querySelectorAll('.tab-panel');
  for (var j = 0; j < panels.length; j++) {
    panels[j].classList.toggle('hidden', panels[j].id !== 'tab-' + name);
  }
  if (name === 'settings') ensureSettings();
  if (name === 'status') refreshStatus();
  if (name === 'logs') refreshLogs();
}

/* ---------- 订阅列表 ---------- */

function loadSubs() {
  return api('/api/subscriptions').then(function (data) {
    applySubs(data.subscriptions);
  });
}

function applySubs(list) {
  state.subs = Array.isArray(list) ? list : [];
  setSubErrors([]);
  renderSubs();
}

function renderSubs() {
  var body = $('#subs-body');
  var hasSubs = state.subs.length > 0;
  $('#subs-table').classList.toggle('hidden', !hasSubs);
  $('#subs-empty').classList.toggle('hidden', hasSubs);
  if (!hasSubs) {
    body.innerHTML = '';
    return;
  }
  body.innerHTML = state.subs.map(function (s, i) {
    var sessions = (s.push_session_ids || []).join(', ');
    return '<tr data-i="' + i + '">' +
      '<td class="col-enable"><input type="checkbox" class="row-enabled" data-i="' + i + '"' +
        (s.enabled ? ' checked' : '') + '></td>' +
      '<td class="cell-name" title="' + esc(s.name) + '">' + esc(s.name) + '</td>' +
      '<td><span class="badge badge-type-' + esc(s.type) + '">' +
        esc(SUB_TYPE_LABELS[s.type] || s.type) + '</span></td>' +
      '<td>' + esc(s.uid) + '</td>' +
      '<td>' + esc(s.poll_interval_sec) + '</td>' +
      '<td class="cell-sessions" title="' + esc(sessions) + '">' + esc(sessions) + '</td>' +
      '<td class="col-actions">' +
        '<button class="btn btn-sm row-edit" data-i="' + i + '">编辑</button>' +
        '<button class="btn btn-sm row-test" data-i="' + i + '">试推</button>' +
        '<button class="btn btn-sm row-test-all" data-i="' + i + '">试推全部</button>' +
        '<button class="btn btn-sm btn-danger row-del" data-i="' + i + '">删除</button>' +
      '</td>' +
    '</tr>';
  }).join('');
}

function setSubErrors(list) {
  var el = $('#sub-errors');
  if (!list || !list.length) {
    el.classList.add('hidden');
    el.innerHTML = '';
    return;
  }
  el.classList.remove('hidden');
  el.innerHTML = '<strong>保存被拒绝（整表未写入）：</strong><ul>' +
    list.map(function (t) { return '<li>' + esc(t) + '</li>'; }).join('') + '</ul>';
}

/* 单条订阅立即保存（新增或按 id 替换），成功后用后端返回的规范化列表替换本地状态 */
function upsertSub(wire, onOk, onErr) {
  api('/api/subscriptions/item', { method: 'POST', body: { subscription: wire } })
    .then(function (data) {
      applySubs(data.subscriptions);
      if (onOk) onOk(data);
    })
    .catch(function (err) {
      if (err.status === 401) return;
      if (onErr) onErr(err);
    });
}

/* 单条订阅立即删除，成功后用后端返回的剩余列表替换本地状态 */
function deleteSubById(id, onOk, onErr) {
  api('/api/subscriptions/' + encodeURIComponent(id), { method: 'DELETE' })
    .then(function (data) {
      applySubs(data.subscriptions);
      if (onOk) onOk(data);
    })
    .catch(function (err) {
      if (err.status === 401) return;
      if (onErr) onErr(err);
    });
}

/* ---------- 订阅编辑器（弹窗） ---------- */

function openSubEditor(index) {
  state.editingIndex = index;
  var s = index == null ? null : state.subs[index];
  /* 打开时快照 id 与代次：保存/关闭回调据此判定目标，避免在途请求重排
     表格后把编辑写到错误的订阅、或迟到的成功回调关闭新打开的编辑器 */
  state.editingId = s ? (s.id || null) : null;
  state.editingToken = (state.editingToken || 0) + 1;
  $('#sub-modal-title').textContent = s ? '编辑订阅' : '新增订阅';
  $('#f-id-line').classList.toggle('hidden', !s);
  $('#f-id').textContent = s ? s.id : '';
  $('#f-type').value = s ? s.type : 'live';
  $('#f-name').value = s ? (s.name || '') : '';
  $('#f-uid').value = s && s.uid != null ? String(s.uid) : '';
  $('#f-poll').value = s ? String(s.poll_interval_sec) : String(POLL_DEFAULT_SEC);
  $('#f-list-id').value = s && s.list_id != null ? String(s.list_id) : '';
  $('#f-series').value = s && s.series_type != null ? String(s.series_type) : '0';
  $('#f-enabled').checked = s ? !!s.enabled : true;
  $('#f-sessions').value = s ? (s.push_session_ids || []).join('\n') : '';
  setModalErrors([]);
  updateCollectionFields();
  $('#sub-modal').classList.remove('hidden');
  $('#f-name').focus();
}

function closeSubEditor() {
  $('#sub-modal').classList.add('hidden');
}

function updateCollectionFields() {
  var isCol = $('#f-type').value === 'collection';
  $('#f-list-id-wrap').classList.toggle('hidden', !isCol);
  $('#f-series-wrap').classList.toggle('hidden', !isCol);
}

function setModalErrors(list) {
  var el = $('#sub-modal-errors');
  if (!list || !list.length) {
    el.classList.add('hidden');
    el.innerHTML = '';
    return;
  }
  el.classList.remove('hidden');
  el.innerHTML = '<strong>无法保存：</strong><ul>' +
    list.map(function (t) { return '<li>' + esc(t) + '</li>'; }).join('') + '</ul>';
}

/* 会话文本：按 换行 / 逗号（中英文）拆分，去空白，过滤格式非法项 */
function parseSessions(text) {
  var dropped = [];
  var sessions = text.split(/[\n,，]/)
    .map(function (t) { return t.trim(); })
    .filter(function (t) { return t !== ''; })
    .filter(function (t) {
      var parts = t.split(':');
      var ok = parts.length >= 3 && parts[0] !== '' && parts[1] !== '';
      if (!ok) dropped.push(t);
      return ok;
    });
  return { sessions: sessions, dropped: dropped };
}

function onSubEditorSave() {
  var isCol = $('#f-type').value === 'collection';
  var parsed = parseSessions($('#f-sessions').value);
  var errors = [];

  var uidRaw = $('#f-uid').value.trim();
  if (uidRaw === '' || !Number.isInteger(Number(uidRaw))) {
    errors.push('UID 必须为数字');
  }
  var listRaw = $('#f-list-id').value.trim();
  var seriesRaw = $('#f-series').value;
  if (isCol) {
    if (listRaw === '' || !Number.isInteger(Number(listRaw))) {
      errors.push('list_id 必须为数字（collection 必填）');
    }
    if (seriesRaw !== '0' && seriesRaw !== '1') {
      errors.push('series_type 必须为 0（视频合集）或 1（收藏夹）');
    }
  }
  if (parsed.dropped.length) {
    errors.push('以下会话格式非法，已被忽略（应为 platform:message_type:session_id）：' +
      parsed.dropped.join('、'));
  }
  if (!parsed.sessions.length) {
    errors.push('推送会话不能为空（至少一个合法会话）');
  }

  var pollRaw = $('#f-poll').value.trim();
  if (pollRaw === '') pollRaw = String(POLL_DEFAULT_SEC);
  var pollNum = Number(pollRaw);
  if (!Number.isFinite(pollNum)) errors.push('轮询间隔必须为数字');

  if (errors.length) {
    setModalErrors(errors);
    return;
  }

  /* wire 形态；新增行不带 id（后端分配），编辑行按打开时快照的 id 写回——
     即使编辑期间表格因其他请求重排，也绝不会把编辑写到错误的订阅 */
  var wire = {
    type: $('#f-type').value,
    name: $('#f-name').value.trim(),
    uid: Number(uidRaw),
    list_id: isCol ? Number(listRaw) : null,
    series_type: isCol ? Number(seriesRaw) : null,
    poll_interval_sec: Math.max(1, Math.round(pollNum)),
    enabled: $('#f-enabled').checked,
    push_session_ids: parsed.sessions,
  };
  if (state.editingId) wire.id = state.editingId;
  var btn = $('#f-save');
  var token = state.editingToken;
  btn.disabled = true;
  btn.textContent = '保存中…';
  upsertSub(wire, function () {
    btn.disabled = false;
    btn.textContent = '保存';
    if (token !== state.editingToken) return;  /* 用户已重开编辑器：不干扰新弹窗 */
    closeSubEditor();
    toast('订阅已保存', 'success');
  }, function (err) {
    btn.disabled = false;
    btn.textContent = '保存';
    if (token !== state.editingToken) return;
    setModalErrors([(err.data && err.data.error) || err.message]);
  });
}

/* 删除按点击时快照的 id 定位：确认框停留期间表格重排也不会删错订阅 */
function removeSub(id, label) {
  if (!id) return;
  if (!window.confirm('确定删除订阅「' + label + '」吗？该操作会立即写回后端。')) {
    return;
  }
  deleteSubById(id, function () {
    toast('订阅已删除', 'success');
  }, function (err) {
    toast('删除失败：' + ((err.data && err.data.error) || err.message), 'error');
  });
}

function onSubRowClick(e) {
  var btn = e.target.closest('button');
  if (!btn) return;
  var i = Number(btn.dataset.i);
  var s = state.subs[i];
  if (!s) return;
  var id = s.id || '';
  var label = s.name || id || ('#' + i);
  if (btn.classList.contains('row-edit')) openSubEditor(i);
  else if (btn.classList.contains('row-test')) fillTestPush(i);
  else if (btn.classList.contains('row-test-all')) doTestPushAll(i);
  else if (btn.classList.contains('row-del')) removeSub(id, label);
}

function onSubRowChange(e) {
  var cb = e.target;
  if (!cb.classList.contains('row-enabled')) return;
  var i = Number(cb.dataset.i);
  var s = state.subs[i];
  if (!s) return;
  var wire = Object.assign({}, s, { enabled: cb.checked });
  upsertSub(wire, function () {
    toast((wire.enabled ? '已启用 ' : '已停用 ') + (s.name || ('#' + i)), 'success');
  }, function (err) {
    /* 失败回滚：按服务器状态整体重渲染（captured 节点可能已因在途
       请求被替换出 DOM，直接改 cb 会失效） */
    applySubs(state.subs);
    toast('保存失败：' + ((err.data && err.data.error) || err.message), 'error');
  });
}

/* ---------- 测试推送 ---------- */

function fillTestPush(index) {
  var s = state.subs[index];
  if (!s) return;
  var first = (s.push_session_ids || [])[0];
  if (first) $('#tp-session').value = first;
  $('#tp-event-type').value = SUB_TYPE_TO_EVENT[s.type] || 'dynamic';
  $('#tp-message').value = '测试推送（订阅：' + (s.name || s.id || ('#' + index)) + '）';
  setTpResult('', '');
  $('#tp-send').focus();
}

function setTpResult(text, kind) {
  var el = $('#tp-result');
  el.textContent = text;
  el.className = 'msg' + (text ? ' ' + (kind || 'info') : '');
}

function doTestPush() {
  var session = $('#tp-session').value.trim();
  var message = $('#tp-message').value.trim();
  if (!session || !message) {
    setTpResult('会话与消息均不能为空', 'error');
    return;
  }
  var btn = $('#tp-send');
  btn.disabled = true;
  btn.textContent = '发送中…';
  setTpResult('', '');
  api('/api/test-push', {
    method: 'POST',
    body: { session: session, message: message, event_type: $('#tp-event-type').value },
  })
    .then(function (data) {
      setTpResult((data.ok ? '成功：' : '失败：') + (data.detail || ''), data.ok ? 'success' : 'error');
    })
    .catch(function (err) {
      if (err.status === 401) return;
      if (err.status === 400 && err.data) {
        setTpResult('失败：' + (err.data.detail || err.data.error || '请求被拒绝'), 'error');
      } else {
        setTpResult('失败：' + err.message, 'error');
      }
    })
    .then(function () {
      btn.disabled = false;
      btn.textContent = '发送';
    });
}

function doTestPushAll(index) {
  var s = state.subs[index];
  if (!s) return;
  var sessions = (s.push_session_ids || []).slice();
  if (!sessions.length) {
    toast('该订阅没有可推送的会话', 'error');
    return;
  }
  var message = '测试推送（订阅：' + (s.name || s.id || ('#' + index)) + '）';
  if (!window.confirm('向该订阅的全部 ' + sessions.length + ' 个会话发送测试推送？')) return;
  api('/api/test-push', {
    method: 'POST',
    body: { sessions: sessions, message: message, event_type: SUB_TYPE_TO_EVENT[s.type] || 'dynamic' },
  })
    .then(function (data) {
      var okCount = data.results
        ? Object.keys(data.results).filter(function (k) { return data.results[k]; }).length
        : 0;
      toast(
        (data.ok ? '试推全部成功：' : '试推部分失败：') +
          (data.detail || (okCount + '/' + sessions.length)),
        data.ok ? 'success' : 'error'
      );
    })
    .catch(function (err) {
      if (err.status === 401) return;
      toast('试推失败：' + ((err.data && (err.data.detail || err.data.error)) || err.message), 'error');
    });
}

/* ---------- 设置 ---------- */

var CREDENTIAL_FIELDS = ['sessdata', 'bili_jct', 'buvid3', 'buvid4', 'dedeuserid', 'ac_time_value'];

function ensureSettings() {
  if (state.settingsLoaded && state.settings) {
    renderSettings();
    return Promise.resolve();
  }
  return api('/api/settings').then(function (data) {
    state.settings = data || {};
    state.settingsLoaded = true;
    renderSettings();
  }).catch(function (err) {
    if (err.status !== 401) setConn(false, '连接失败');
  });
}

function renderSettings() {
  var s = state.settings;
  if (!s) return;
  var cred = s.credential || {};
  CREDENTIAL_FIELDS.forEach(function (k) {
    $('#set-cred-' + k).value = cred[k] == null ? '' : String(cred[k]);
  });
  var poll = s.poll || {};
  $('#set-poll-min').value = poll.global_min_interval_sec == null ? '' : String(poll.global_min_interval_sec);
  $('#set-poll-jitter').value = poll.poll_jitter_sec == null ? '' : String(poll.poll_jitter_sec);
  $('#set-poll-title').checked = !!poll.push_title_change;
  var webui = s.webui || {};
  $('#set-webui-enabled').checked = !!webui.enabled;
  $('#set-webui-host').value = webui.host == null ? '' : String(webui.host);
  $('#set-webui-port').value = webui.port == null ? '' : String(webui.port);
  $('#set-webui-token').value = webui.token == null ? '' : String(webui.token);
}

function setSettingsErrors(list) {
  var el = $('#settings-errors');
  if (!list || !list.length) {
    el.classList.add('hidden');
    el.innerHTML = '';
    return;
  }
  el.classList.remove('hidden');
  el.innerHTML = '<strong>无法保存：</strong><ul>' +
    list.map(function (t) { return '<li>' + esc(t) + '</li>'; }).join('') + '</ul>';
}

function setSettingsMsg(text, kind) {
  var el = $('#settings-msg');
  el.textContent = text;
  el.className = 'msg' + (text ? ' ' + (kind || 'info') : '');
}

function saveSettings() {
  var prev = state.settings;
  if (!prev) {
    ensureSettings().then(function () {
      if (state.settings) saveSettings();
    });
    return;
  }
  var errors = [];
  var prevPoll = prev.poll || {};
  var prevWebui = prev.webui || {};

  var min = readNumInput('#set-poll-min', prevPoll.global_min_interval_sec);
  if (min.err || min.value == null || min.value < 1) {
    errors.push('全局最小轮询间隔必须为 ≥ 1 的数字（秒）');
  }
  var jitter = readNumInput('#set-poll-jitter', prevPoll.poll_jitter_sec);
  if (jitter.err || jitter.value == null || jitter.value < 0) {
    errors.push('随机波动上限不能为负数字');
  }
  var port = readNumInput('#set-webui-port', prevWebui.port);
  if (port.err || !Number.isInteger(port.value) || port.value < 1 || port.value > 65535) {
    errors.push('监听端口必须是 1-65535 的整数');
  }
  if (errors.length) {
    setSettingsErrors(errors);
    setSettingsMsg('', '');
    return;
  }

  var body = {
    credential: {},
    poll: {
      global_min_interval_sec: min.value,
      poll_jitter_sec: jitter.value,
      push_title_change: $('#set-poll-title').checked,
    },
    webui: {
      enabled: $('#set-webui-enabled').checked,
      host: $('#set-webui-host').value.trim() || '127.0.0.1',
      port: port.value,
      token: prevWebui.token || '',
    },
  };
  CREDENTIAL_FIELDS.forEach(function (k) {
    body.credential[k] = $('#set-cred-' + k).value;
  });

  var btn = $('#btn-save-settings');
  btn.disabled = true;
  btn.textContent = '保存中…';
  setSettingsErrors([]);
  setSettingsMsg('正在保存…', 'info');
  api('/api/settings', { method: 'POST', body: body })
    .then(function () {
      state.settings = body; /* 与后端合并结果一致（token 原样回传） */
      setSettingsMsg('设置已保存（host / port / enabled 重载插件后生效）', 'success');
    })
    .catch(function (err) {
      if (err.status !== 401) {
        setSettingsMsg('保存失败：' + (err.data && err.data.error ? err.data.error : err.message), 'error');
      }
    })
    .then(function () {
      btn.disabled = false;
      btn.textContent = '保存设置';
    });
}

/* ---------- 状态 ---------- */

function refreshStatus() {
  if (state.activeTab !== 'status' || document.hidden) return;
  api('/api/config-status').then(renderConfigStatus).catch(function () {
    /* 401 已回令牌门；网络错误保留上次渲染 */
  });
  api('/api/status').then(function (data) {
    state.status = data || {};
    renderStatus();
  }).catch(function () {
    /* 401 已回令牌门；网络错误保留上次渲染，仅连接灯变红 */
  });
}

function renderConfigStatus(data) {
  var el = $('#config-status');
  if (!data) return;
  if (data.ok) {
    el.textContent = '配置文件：正常';
    el.className = 'msg success';
  } else {
    el.textContent = '配置文件读取失败：' + (data.last_error || '未知错误');
    el.className = 'msg error';
  }
}

/* ---------- 顶栏登录状态 ---------- */

function refreshLoginStatus() {
  api('/api/login-status').then(renderLoginStatus).catch(function () {
    /* 401 已回令牌门；网络错误保留上次渲染 */
  });
}

function renderLoginStatus(data) {
  var el = $('#login-status');
  if (!el || !data) return;
  if (data.last_ok_at) {
    el.textContent = '登录校验通过：' + fmtTime(data.last_ok_at);
    el.className = 'login-status ok';
  } else if (data.consecutive_failures) {
    el.textContent = '登录校验失败 ×' + data.consecutive_failures;
    el.className = 'login-status err';
  } else {
    el.textContent = '登录校验：—';
    el.className = 'login-status';
  }
}

function renderStatus() {
  var grid = $('#status-grid');
  var subs = Array.isArray(state.subs) ? state.subs : [];

  var liveNow = 0;
  var errorSubs = 0;
  var autoDisabled = 0;
  var stopped = 0;
  subs.forEach(function (sub) {
    var st = state.status[sub.id];
    if (sub.enabled === false) stopped++;
    if (st && st.live_status === 1) liveNow++;
    if (st && st.error_count) errorSubs++;
    if (st && st.auto_disabled) autoDisabled++;
  });

  var meta = '共 ' + subs.length + ' 条 · 直播中 ' + liveNow + ' · 异常 ' + errorSubs +
    ' · 自动禁用 ' + autoDisabled;
  if (stopped) meta += ' · 已停用 ' + stopped;
  $('#status-meta').textContent = subs.length ? meta : '';

  if (!subs.length) {
    grid.innerHTML = '<div class="empty">暂无状态数据（还没有订阅）。</div>';
    return;
  }

  grid.innerHTML = subs.map(function (sub) {
    var id = sub.id || '';
    var st = state.status[id] || null;
    var name = sub.name || (id.length > 12 ? id.slice(0, 12) + '…' : id) || '未命名';
    var badges = '';
    if (sub.enabled === false) {
      badges += '<span class="badge badge-muted">已停用</span>';
    } else if (!st) {
      badges += '<span class="badge badge-muted">未轮询</span>';
    } else {
      if (st.auto_disabled) badges += '<span class="badge badge-amber">自动禁用</span>';
      if (st.live_status === 1) badges += '<span class="badge badge-green">直播中</span>';
      else if (st.live_status === 2) badges += '<span class="badge badge-blue">轮播中</span>';
      else if (st.live_status === 0) badges += '<span class="badge badge-muted">未开播</span>';
    }

    return '<div class="status-card">' +
      '<div class="sc-head"><span class="sc-name" title="' + esc(name) + '">' + esc(name) + '</span>' + badges + '</div>' +
      '<div class="sc-grid">' +
        '<div class="sc-item"><span class="sc-label">上次轮询</span><span class="sc-value">' + esc(st ? fmtTime(st.last_poll) : '—') + '</span></div>' +
        '<div class="sc-item"><span class="sc-label">错误次数</span><span class="sc-value' + (st && st.error_count ? ' v-err' : '') + '">' + esc(st ? st.error_count : '—') + '</span></div>' +
        '<div class="sc-item"><span class="sc-label">上次推送</span><span class="sc-value">' + esc(st ? fmtTime(st.last_push_at) : '—') + '</span></div>' +
      '</div>' +
      (st && st.last_error
        ? '<div class="sc-error" title="' + esc(st.last_error) + '">' + esc(st.last_error) + '</div>'
        : '') +
      '<div class="sc-id">' + esc(id) + '</div>' +
    '</div>';
  }).join('');
}

/* ---------- 日志 ---------- */

function refreshLogs() {
  if (state.activeTab !== 'logs' || document.hidden) return;
  api('/api/logs?tail=' + LOG_TAIL).then(function (data) {
    var logs = Array.isArray(data.logs) ? data.logs : [];
    var box = $('#log-box');
    box.textContent = logs.join('\n');
    $('#log-meta').textContent = '共 ' + (data.total || 0) + ' 条（最近 ' + LOG_TAIL + '）· ' +
      fmtTime(new Date().toISOString());
    if (state.logAutoScroll) box.scrollTop = box.scrollHeight;
  }).catch(function () {
    /* 401 已回令牌门；网络错误保留上次内容 */
  });
}

function onLogScroll() {
  var box = $('#log-box');
  state.logAutoScroll = box.scrollHeight - box.scrollTop - box.clientHeight < 40;
}

/* ---------- 定时器 ---------- */

function startTimers() {
  if (!state.statusTimer) {
    state.statusTimer = setInterval(refreshStatus, STATUS_REFRESH_MS);
  }
  if (!state.logTimer) {
    state.logTimer = setInterval(refreshLogs, LOG_REFRESH_MS);
  }
  if (!state.loginTimer) {
    state.loginTimer = setInterval(refreshLoginStatus, LOGIN_REFRESH_MS);
  }
}

function stopTimers() {
  if (state.statusTimer) {
    clearInterval(state.statusTimer);
    state.statusTimer = null;
  }
  if (state.logTimer) {
    clearInterval(state.logTimer);
    state.logTimer = null;
  }
  if (state.loginTimer) {
    clearInterval(state.loginTimer);
    state.loginTimer = null;
  }
}

/* ---------- 令牌提交 ---------- */

function submitToken() {
  var t = $('#token-input').value.trim();
  if (!t) {
    setTokenMsg('请输入令牌', 'error');
    return;
  }
  setToken(t);
  setTokenMsg('正在验证…', 'info');
  api('/api/subscriptions').then(function () {
    setTokenMsg('', '');
    hideTokenGate();
    boot();
  }).catch(function (err) {
    if (err.status === 401) {
      setTokenMsg('令牌验证失败，请检查后重试', 'error');
    } else {
      /* 网络问题：仍进入界面，连接灯会显示为失败 */
      hideTokenGate();
      boot();
    }
  });
}

/* ---------- 启动 ---------- */

function boot() {
  startTimers();
  refreshLoginStatus();
  switchTab('subs');
  Promise.all([loadSubs(), ensureSettings()]).catch(function (err) {
    if (err.status !== 401) setConn(false, '连接失败');
  });
}

function bindEvents() {
  $('#tabs').addEventListener('click', function (e) {
    var btn = e.target.closest('.tab');
    if (btn) switchTab(btn.dataset.tab);
  });

  $('#token-save').addEventListener('click', submitToken);
  $('#token-input').addEventListener('keydown', function (e) {
    if (e.key === 'Enter') submitToken();
  });
  $('#btn-change-token').addEventListener('click', function () {
    setTokenMsg('', '');
    showTokenGate('');
  });

  $('#btn-add-sub').addEventListener('click', function () { openSubEditor(null); });
  $('#subs-body').addEventListener('click', onSubRowClick);
  $('#subs-body').addEventListener('change', onSubRowChange);

  $('#f-cancel').addEventListener('click', closeSubEditor);
  $('#f-save').addEventListener('click', onSubEditorSave);
  $('#f-type').addEventListener('change', updateCollectionFields);
  $('#sub-modal').addEventListener('click', function (e) {
    if (e.target === $('#sub-modal')) closeSubEditor();
  });
  document.addEventListener('keydown', function (e) {
    if (e.key === 'Escape' && !$('#sub-modal').classList.contains('hidden')) closeSubEditor();
  });

  $('#tp-send').addEventListener('click', doTestPush);

  $('#btn-save-settings').addEventListener('click', saveSettings);

  $('#btn-log-refresh').addEventListener('click', refreshLogs);
  $('#log-box').addEventListener('scroll', onLogScroll);
}

function init() {
  bindEvents();
  if (!getToken()) {
    showTokenGate('');
    return;
  }
  hideTokenGate();
  boot();
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', init);
} else {
  init();
}
