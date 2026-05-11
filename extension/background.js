/**
 * Taleos Extension - Background Service Worker
 * Orchestre : ouverture onglet, récupération profil Firestore, injection du script banque
 */

/** Watchdog des candidatures : timeout produit à 5 min + capture diagnostic. */
const APPLY_STUCK_ALARM = 'taleos_apply_watchdog_1min';
const APPLY_TIMEOUT_MINUTES = 5;
const APPLY_WATCHDOG_PERIOD_MINUTES = 1;
const EXTENSION_APPLICATION_RUNS_COLLECTION = 'extension_application_runs';
const ACTIVE_APPLY_RUNS_STORAGE_KEY = 'taleos_apply_runs_by_tab';
const CREDIT_MUTUEL_LAST_APPLY_KEY = 'taleos_last_credit_mutuel_apply';
const STUCK_REPORT_CF_URL = 'https://europe-west1-project-taleos.cloudfunctions.net/report-stuck-automation';
const SAVE_EXTENSION_RUN_CF_URL = 'https://europe-west1-project-taleos.cloudfunctions.net/saveExtensionApplicationRun';

chrome.alarms.create('taleos-keepalive', { periodInMinutes: 4 });
chrome.alarms.onAlarm.addListener((alarm) => {
  if (alarm.name === 'taleos-keepalive') { /* keep service worker warm */ }
  if (alarm.name === APPLY_STUCK_ALARM) {
    handleApplyStuckAlarm().catch((e) => console.error('[Taleos] Stuck watchdog:', e));
  }
});

chrome.runtime.onInstalled.addListener((details) => {
  chrome.alarms.create('taleos-keepalive', { periodInMinutes: 4 });
  if (details.reason === 'update') {
    const patterns = ['https://*.taleos.co/*', 'http://localhost/*', 'http://127.0.0.1/*'];
    patterns.forEach((url) => {
      chrome.tabs.query({ url }, (tabs) => {
        tabs.forEach((tab) => { try { chrome.tabs.reload(tab.id); } catch (_) {} });
      });
    });
  }
});

(function setLastUpdate() {
  const now = new Date();
  const dd = String(now.getDate()).padStart(2, '0');
  const mm = String(now.getMonth() + 1).padStart(2, '0');
  const yyyy = now.getFullYear();
  const hh = String(now.getHours()).padStart(2, '0');
  const min = String(now.getMinutes()).padStart(2, '0');
  chrome.storage.local.set({ taleosLastUpdate: `${dd}/${mm}/${yyyy} ${hh}:${min}` });
})();

const BANK_SCRIPT_MAP = {
  credit_agricole: 'scripts/credit_agricole.js',
  societe_generale: 'scripts/societe_generale.js',
  deloitte: 'content/deloitte-careers-filler.js',
  bpce: 'content/bpce-careers-filler.js',
  bnp_paribas: 'content/bnp-careers-filler.js',
  credit_mutuel: 'content/credit-mutuel-careers-filler.js',
  bpifrance: 'content/bpifrance-careers-filler.js',
  jp_morgan: 'content/jp-morgan-careers-filler.js',
  goldman_sachs: 'content/goldman-sachs-careers-filler.js',
  axa: 'content/axa-careers-filler.js'
};

function hasBankAutomation(bankId) {
  return Boolean(bankId && BANK_SCRIPT_MAP[bankId]);
}

const PROJECT_ID = 'project-taleos';
const GMAIL_STORAGE_KEY_PREFIX = 'taleos_gmail_auth_';
const OUTLOOK_LINK_STATE_KEY_PREFIX = 'taleos_outlook_link_state_';
const GMAIL_REQUIRED_SCOPE = 'https://www.googleapis.com/auth/gmail.readonly';
const OUTLOOK_OAUTH_SCOPE = 'offline_access Mail.Read User.Read openid profile email';
const OUTLOOK_CONFIG_CF_URL = 'https://europe-west1-project-taleos.cloudfunctions.net/outlookOAuthConfig';
const OUTLOOK_EXCHANGE_CF_URL = 'https://europe-west1-project-taleos.cloudfunctions.net/outlookOAuthExchange';
const OUTLOOK_FETCH_OTP_CF_URL = 'https://europe-west1-project-taleos.cloudfunctions.net/outlookFetchLatestOtp';
const OUTLOOK_UNLINK_CF_URL = 'https://europe-west1-project-taleos.cloudfunctions.net/outlookUnlinkSecure';

/** Injecté avant chaque script d'automatisation banque (bannière commune). */
const TALEOS_BANNER_SCRIPT = 'scripts/taleos-automation-banner.js';
const CA_BLUEPRINT_SCRIPT = 'scripts/credit_agricole_blueprint.js';
const SG_BLUEPRINT_SCRIPT = 'scripts/societe_generale_blueprint.js';
const BPCE_BLUEPRINT_SCRIPT = 'scripts/bpce_blueprint.js';
const DELOITTE_BLUEPRINT_SCRIPT = 'scripts/deloitte_blueprint.js';
const BNP_BLUEPRINT_SCRIPT = 'scripts/bnp_paribas_blueprint.js';
const CREDIT_MUTUEL_BLUEPRINT_SCRIPT = 'scripts/credit_mutuel_blueprint.js';
const BPIFRANCE_BLUEPRINT_SCRIPT = 'scripts/bpifrance_blueprint.js';
const JP_MORGAN_BLUEPRINT_SCRIPT = 'scripts/jp_morgan_blueprint.js';
const GOLDMAN_SACHS_BLUEPRINT_SCRIPT = 'scripts/goldman_sachs_blueprint.js';
const AXA_BLUEPRINT_SCRIPT = 'scripts/axa_blueprint.js';

function injectFilesWithBanner(mainFiles) {
  const arr = Array.isArray(mainFiles) ? mainFiles : [mainFiles];
  if (arr[0] === TALEOS_BANNER_SCRIPT) return arr;
  return [TALEOS_BANNER_SCRIPT, ...arr];
}

function injectBankFiles(bankId, mainFiles) {
  const arr = Array.isArray(mainFiles) ? mainFiles : [mainFiles];
  if (bankId === 'credit_agricole') {
    return injectFilesWithBanner([CA_BLUEPRINT_SCRIPT, ...arr]);
  }
  if (bankId === 'societe_generale') {
    return injectFilesWithBanner([SG_BLUEPRINT_SCRIPT, ...arr]);
  }
  if (bankId === 'bpce') {
    return injectFilesWithBanner([BPCE_BLUEPRINT_SCRIPT, ...arr]);
  }
  if (bankId === 'deloitte') {
    return injectFilesWithBanner([DELOITTE_BLUEPRINT_SCRIPT, ...arr]);
  }
  if (bankId === 'bnp_paribas') {
    return injectFilesWithBanner([BNP_BLUEPRINT_SCRIPT, ...arr]);
  }
  if (bankId === 'credit_mutuel') {
    return injectFilesWithBanner([CREDIT_MUTUEL_BLUEPRINT_SCRIPT, ...arr]);
  }
  if (bankId === 'bpifrance') {
    return injectFilesWithBanner([BPIFRANCE_BLUEPRINT_SCRIPT, ...arr]);
  }
  if (bankId === 'jp_morgan') {
    return injectFilesWithBanner([JP_MORGAN_BLUEPRINT_SCRIPT, ...arr]);
  }
  if (bankId === 'goldman_sachs') {
    return injectFilesWithBanner([GOLDMAN_SACHS_BLUEPRINT_SCRIPT, ...arr]);
  }
  if (bankId === 'axa') {
    return injectFilesWithBanner([AXA_BLUEPRINT_SCRIPT, ...arr]);
  }
  return injectFilesWithBanner(arr);
}

async function clearPendingStateForBank(bankId, tabId) {
  const bid = String(bankId || '').toLowerCase();
  const keys = [];
  if (bid === 'societe_generale') keys.push('taleos_pending_sg', 'taleos_sg_tab_id');
  if (bid === 'credit_agricole') keys.push('taleos_pending_offer', 'taleos_ca_apply_tab_id');
  if (bid === 'deloitte') keys.push('taleos_pending_deloitte', 'taleos_deloitte_did_login_click');
  if (bid === 'bpce') keys.push('taleos_pending_bpce', 'taleos_bpce_tab_id');
  if (bid === 'bnp_paribas') keys.push('taleos_pending_bnp', 'taleos_bnp_tab_id');
  if (bid === 'credit_mutuel') keys.push('taleos_pending_credit_mutuel', 'taleos_credit_mutuel_tab_id');
  if (bid === 'jp_morgan') keys.push('taleos_pending_jp_morgan', 'taleos_jp_morgan_tab_id');
  if (bid === 'goldman_sachs') keys.push('taleos_pending_goldman_sachs', 'taleos_gs_tab_id');
  if (bid === 'axa') keys.push('taleos_pending_axa', 'taleos_axa_tab_id');
  if (keys.length) {
    await chrome.storage.local.remove(keys);
  }
  if (tabId && bid === 'societe_generale') sgLastInject.delete(tabId);
  if (tabId && bid === 'credit_agricole') caLastInject.delete(tabId);
}

let authSyncResolve = null;
const sgLastInject = new Map();
const caLastInject = new Map();
const bpifranceLastInject = new Map();

async function scheduleApplyStuckWatchdog() {
  try {
    const { [ACTIVE_APPLY_RUNS_STORAGE_KEY]: activeRuns = {} } = await chrome.storage.local.get([ACTIVE_APPLY_RUNS_STORAGE_KEY]);
    if (!activeRuns || Object.keys(activeRuns).length === 0) return;
    await chrome.alarms.clear(APPLY_STUCK_ALARM);
    await chrome.alarms.create(APPLY_STUCK_ALARM, { periodInMinutes: APPLY_WATCHDOG_PERIOD_MINUTES });
  } catch (e) {
    console.error('[Taleos] scheduleApplyStuckWatchdog:', e);
  }
}

async function clearApplyStuckWatchdog() {
  try {
    const { [ACTIVE_APPLY_RUNS_STORAGE_KEY]: activeRuns = {} } = await chrome.storage.local.get([ACTIVE_APPLY_RUNS_STORAGE_KEY]);
    if (!activeRuns || Object.keys(activeRuns).length === 0) {
      await chrome.alarms.clear(APPLY_STUCK_ALARM);
    }
  } catch (_) {}
}

function nowIso() {
  return new Date().toISOString();
}

function addMinutes(dateLike, minutes) {
  return new Date(new Date(dateLike).getTime() + minutes * 60 * 1000).toISOString();
}

function sanitizeRunText(value, max = 500) {
  return String(value || '').trim().slice(0, max);
}

function computeDurationSeconds(startedAt, endedAt) {
  const startMs = Date.parse(startedAt || '') || 0;
  const endMs = Date.parse(endedAt || '') || 0;
  if (!startMs || !endMs || endMs < startMs) return 0;
  return Math.round((endMs - startMs) / 1000);
}

function buildApplyRunId(bankId, jobId) {
  const bank = String(bankId || 'unknown').toLowerCase().replace(/[^a-z0-9_]+/g, '_');
  const job = String(jobId || 'unknown').trim().replace(/[^a-zA-Z0-9_-]+/g, '_').slice(0, 80) || 'unknown';
  return `${bank}_${job}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

function normalizeRunLogEntry(entry) {
  const ts = entry?.ts || nowIso();
  const source = sanitizeRunText(entry?.source || 'extension', 80);
  const level = sanitizeRunText(entry?.level || 'info', 20).toLowerCase();
  const message = sanitizeRunText(entry?.message || '', 500);
  if (!message) return null;
  return { ts, source, level, message };
}

async function getActiveApplyRuns() {
  try {
    const out = await chrome.storage.local.get([ACTIVE_APPLY_RUNS_STORAGE_KEY]);
    return out[ACTIVE_APPLY_RUNS_STORAGE_KEY] || {};
  } catch (_) {
    return {};
  }
}

async function setActiveApplyRuns(activeRuns) {
  await chrome.storage.local.set({ [ACTIVE_APPLY_RUNS_STORAGE_KEY]: activeRuns || {} });
}

function buildFirestoreFieldsFromObject(obj) {
  const fields = {};
  for (const [key, value] of Object.entries(obj || {})) {
    if (value === undefined) continue;
    if (value === null) {
      fields[key] = { nullValue: null };
      continue;
    }
    if (typeof value === 'string') {
      fields[key] = { stringValue: value };
      continue;
    }
    if (typeof value === 'boolean') {
      fields[key] = { booleanValue: value };
      continue;
    }
    if (typeof value === 'number') {
      if (Number.isFinite(value) && Number.isInteger(value)) fields[key] = { integerValue: String(value) };
      else if (Number.isFinite(value)) fields[key] = { doubleValue: value };
      continue;
    }
    if (Array.isArray(value)) {
      fields[key] = {
        arrayValue: {
          values: value
            .filter((item) => item !== undefined)
            .map((item) => {
              if (item === null) return { nullValue: null };
              if (typeof item === 'string') return { stringValue: item };
              if (typeof item === 'boolean') return { booleanValue: item };
              if (typeof item === 'number') {
                if (Number.isFinite(item) && Number.isInteger(item)) return { integerValue: String(item) };
                if (Number.isFinite(item)) return { doubleValue: item };
              }
              return { stringValue: JSON.stringify(item) };
            })
        }
      };
      continue;
    }
    fields[key] = { stringValue: JSON.stringify(value) };
  }
  return fields;
}

async function persistExtensionApplicationRun(run) {
  const { taleosIdToken } = await chrome.storage.local.get(['taleosIdToken']);
  if (!taleosIdToken || !run?.runId) return false;
  const res = await fetch(SAVE_EXTENSION_RUN_CF_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${taleosIdToken}`
    },
    body: JSON.stringify(run)
  });
  if (!res.ok) {
    console.error('[Taleos] Apply run save:', await res.text());
    return false;
  }
  return true;
}

async function registerApplyRunForTab(tabId, payload) {
  const { taleosUserId, taleosUserEmail } = await chrome.storage.local.get(['taleosUserId', 'taleosUserEmail']);
  const extVer = getExtensionVersionForGa4();
  const startedAt = nowIso();
  const run = {
    runId: buildApplyRunId(payload?.bankId, payload?.jobId),
    startedAt,
    deadlineAt: addMinutes(startedAt, APPLY_TIMEOUT_MINUTES),
    status: 'running',
    outcome: 'running',
    timedOut: false,
    userId: sanitizeRunText(taleosUserId, 120),
    userEmail: sanitizeRunText(taleosUserEmail, 200),
    bankId: sanitizeRunText(payload?.bankId, 80),
    companyName: sanitizeRunText(payload?.companyName, 200),
    jobId: sanitizeRunText(payload?.jobId, 200),
    jobTitle: sanitizeRunText(payload?.jobTitle, 300),
    offerUrl: sanitizeRunText(payload?.offerUrl, 1200),
    location: sanitizeRunText(payload?.location, 200),
    contractType: sanitizeRunText(payload?.contractType, 120),
    experienceLevel: sanitizeRunText(payload?.experienceLevel, 120),
    jobFamily: sanitizeRunText(payload?.jobFamily, 160),
    publicationDate: sanitizeRunText(payload?.publicationDate, 120),
    routeAs: sanitizeRunText(payload?.routeAs, 40),
    routingSource: sanitizeRunText(payload?.routingSource, 40),
    automationSource: sanitizeRunText(payload?.automationSource, 40),
    extensionVersion: extVer.extension_version,
    extensionVersionName: extVer.extension_version_name,
    taleosTabId: payload?.taleosTabId ?? null,
    careersTabId: tabId ?? null
  };
  const activeRuns = await getActiveApplyRuns();
  activeRuns[String(tabId)] = run;
  await setActiveApplyRuns(activeRuns);
  await persistExtensionApplicationRun(run);
  await scheduleApplyStuckWatchdog();
  return run;
}

async function finalizeApplyRunForTab(tabId, terminal, details = {}) {
  const tabKey = String(tabId || '');
  if (!tabKey) return null;
  const activeRuns = await getActiveApplyRuns();
  const run = activeRuns[tabKey];
  if (!run) return null;

  const finishedAt = nowIso();
  const alreadyTimedOut = run.outcome === 'timeout' || run.timedOut === true;

  if (alreadyTimedOut && terminal === 'success') {
    run.lateSuccessAt = finishedAt;
    run.lastSignal = 'success';
    if (details.successType) run.lateSuccessType = sanitizeRunText(details.successType, 80);
    if (details.successMessage) run.lateSuccessMessage = sanitizeRunText(details.successMessage, 500);
  } else if (alreadyTimedOut && terminal === 'failed') {
    run.lateFailureAt = finishedAt;
    run.lastSignal = 'failed';
    if (details.failureType) run.lateFailureType = sanitizeRunText(details.failureType, 80);
    if (details.failureMessage) run.lateFailureMessage = sanitizeRunText(details.failureMessage, 500);
  } else if (alreadyTimedOut && terminal === 'aborted') {
    run.lateFailureAt = finishedAt;
    run.lastSignal = 'aborted';
    if (details.failureType) run.lateFailureType = sanitizeRunText(details.failureType, 80);
    if (details.failureMessage) run.lateFailureMessage = sanitizeRunText(details.failureMessage, 500);
  } else {
    run.status = terminal;
    run.outcome = terminal;
    run.completedAt = finishedAt;
    run.durationSeconds = computeDurationSeconds(run.startedAt, finishedAt);
    run.lastSignal = terminal;
    if (terminal === 'success') {
      run.successAt = finishedAt;
      if (details.successType) run.successType = sanitizeRunText(details.successType, 80);
      if (details.successMessage) run.successMessage = sanitizeRunText(details.successMessage, 500);
    } else if (terminal === 'aborted') {
      run.abortedAt = finishedAt;
      run.failureType = sanitizeRunText(details.failureType || 'user_closed_tab', 80);
      run.failureMessage = sanitizeRunText(details.failureMessage || 'L’utilisateur a fermé l’onglet avant la fin de la candidature.', 500);
    } else {
      run.failedAt = finishedAt;
      run.failureType = sanitizeRunText(details.failureType || 'failure', 80);
      run.failureMessage = sanitizeRunText(details.failureMessage || '', 500);
    }
  }

  await persistExtensionApplicationRun(run);
  delete activeRuns[tabKey];
  await setActiveApplyRuns(activeRuns);
  await clearApplyStuckWatchdog();
  return run;
}

async function markTimedOutApplyRun(tabId, details = {}) {
  const tabKey = String(tabId || '');
  if (!tabKey) return null;
  const activeRuns = await getActiveApplyRuns();
  const run = activeRuns[tabKey];
  if (!run || run.outcome === 'timeout') return run || null;

  const timedOutAt = nowIso();
  run.status = 'timeout';
  run.outcome = 'timeout';
  run.timedOut = true;
  run.timedOutAt = timedOutAt;
  run.completedAt = timedOutAt;
  run.durationSeconds = computeDurationSeconds(run.startedAt, timedOutAt);
  run.failureType = sanitizeRunText(details.failureType || 'timeout', 80);
  run.failureMessage = sanitizeRunText(details.failureMessage || 'La candidature n’a pas atteint l’état succès dans les 5 minutes.', 500);
  await persistExtensionApplicationRun(run);
  activeRuns[tabKey] = run;
  await setActiveApplyRuns(activeRuns);
  return run;
}

async function appendApplyRunLogForTab(tabId, entry) {
  const tabKey = String(tabId || '');
  if (!tabKey) return null;
  const activeRuns = await getActiveApplyRuns();
  const run = activeRuns[tabKey];
  if (!run) return null;
  const normalized = normalizeRunLogEntry(entry);
  if (!normalized) return run;
  const existing = Array.isArray(run.recentLogs) ? run.recentLogs : [];
  run.recentLogs = [...existing, normalized].slice(-25);
  run.lastLogAt = normalized.ts;
  run.lastLogMessage = normalized.message;
  activeRuns[tabKey] = run;
  await setActiveApplyRuns(activeRuns);
  await persistExtensionApplicationRun({
    runId: run.runId,
    recentLogs: run.recentLogs,
    lastLogAt: run.lastLogAt,
    lastLogMessage: run.lastLogMessage
  });
  return run;
}

/**
 * Résout l’onglet de candidature et les métadonnées pour la capture (SG, CA, Deloitte, BPCE).
 */
async function resolveTabAndMetaForStuckReport() {
  const s = await chrome.storage.local.get([
    'taleos_pending_sg',
    'taleos_sg_tab_id',
    'taleos_pending_bpce',
    'taleos_bpce_tab_id',
    'taleos_pending_bnp',
    'taleos_bnp_tab_id',
    'taleos_pending_credit_mutuel',
    'taleos_credit_mutuel_tab_id',
    'taleos_pending_jp_morgan',
    'taleos_jp_morgan_tab_id',
    'taleos_pending_goldman_sachs',
    'taleos_gs_tab_id',
    'taleos_pending_axa',
    'taleos_axa_tab_id',
    'taleos_pending_deloitte',
    'taleos_pending_offer',
    'taleos_ca_apply_tab_id'
  ]);
  if (s.taleos_pending_sg?.profile && s.taleos_sg_tab_id) {
    const tab = await chrome.tabs.get(s.taleos_sg_tab_id).catch(() => null);
    if (tab?.id) {
      return {
        tabId: tab.id,
        bankId: 'societe_generale',
        jobId: s.taleos_pending_sg.jobId || s.taleos_pending_sg.profile?.__jobId || '',
        offerUrl: s.taleos_pending_sg.offerUrl || s.taleos_pending_sg.profile?.__offerUrl || ''
      };
    }
    const q = await chrome.tabs.query({ url: '*://socgen.taleo.net/*' });
    if (q[0]?.id) {
      return {
        tabId: q[0].id,
        bankId: 'societe_generale',
        jobId: s.taleos_pending_sg.jobId || '',
        offerUrl: s.taleos_pending_sg.offerUrl || ''
      };
    }
  }
  if (s.taleos_pending_bpce && s.taleos_bpce_tab_id) {
    const tab = await chrome.tabs.get(s.taleos_bpce_tab_id).catch(() => null);
    if (tab?.id) {
      return {
        tabId: tab.id,
        bankId: 'bpce',
        jobId: s.taleos_pending_bpce.jobId || '',
        offerUrl: s.taleos_pending_bpce.offerUrl || ''
      };
    }
  }
  if (s.taleos_pending_bnp && s.taleos_bnp_tab_id) {
    const tab = await chrome.tabs.get(s.taleos_bnp_tab_id).catch(() => null);
    if (tab?.id) {
      return {
        tabId: tab.id,
        bankId: 'bnp_paribas',
        jobId: s.taleos_pending_bnp.jobId || '',
        offerUrl: s.taleos_pending_bnp.offerUrl || ''
      };
    }
  }
  if (s.taleos_pending_credit_mutuel && s.taleos_credit_mutuel_tab_id) {
    const tab = await chrome.tabs.get(s.taleos_credit_mutuel_tab_id).catch(() => null);
    if (tab?.id) {
      return {
        tabId: tab.id,
        bankId: 'credit_mutuel',
        jobId: s.taleos_pending_credit_mutuel.jobId || '',
        offerUrl: s.taleos_pending_credit_mutuel.offerUrl || ''
      };
    }
  }
  if (s.taleos_pending_jp_morgan && s.taleos_jp_morgan_tab_id) {
    const tab = await chrome.tabs.get(s.taleos_jp_morgan_tab_id).catch(() => null);
    if (tab?.id) {
      return {
        tabId: tab.id,
        bankId: 'jp_morgan',
        jobId: s.taleos_pending_jp_morgan.jobId || '',
        offerUrl: s.taleos_pending_jp_morgan.offerUrl || ''
      };
    }
  }
  if (s.taleos_pending_deloitte?.profile) {
    const tid = s.taleos_pending_deloitte.tabId;
    if (tid) {
      const tab = await chrome.tabs.get(tid).catch(() => null);
      if (tab?.id) {
        return {
          tabId: tab.id,
          bankId: 'deloitte',
          jobId: s.taleos_pending_deloitte.jobId || '',
          offerUrl: s.taleos_pending_deloitte.offerUrl || ''
        };
      }
    }
  }
  if (s.taleos_pending_axa && s.taleos_axa_tab_id) {
    const tab = await chrome.tabs.get(s.taleos_axa_tab_id).catch(() => null);
    if (tab) {
      await registerApplyRunForTab(s.taleos_axa_tab_id, {
        bankId: 'axa',
        jobId: s.taleos_pending_axa.jobId || '',
        offerUrl: s.taleos_pending_axa.offerUrl || ''
      });
    }
  }
  if (s.taleos_pending_offer?.profile && s.taleos_ca_apply_tab_id) {
    const tab = await chrome.tabs.get(s.taleos_ca_apply_tab_id).catch(() => null);
    if (tab?.id) {
      return {
        tabId: tab.id,
        bankId: 'credit_agricole',
        jobId: s.taleos_pending_offer.profile?.__jobId || '',
        offerUrl: s.taleos_pending_offer.offerUrl || s.taleos_pending_offer.profile?.__offerUrl || ''
      };
    }
    const q = await chrome.tabs.query({ url: '*://groupecreditagricole.jobs/*' });
    if (q[0]?.id) {
      return {
        tabId: q[0].id,
        bankId: 'credit_agricole',
        jobId: s.taleos_pending_offer.profile?.__jobId || '',
        offerUrl: s.taleos_pending_offer.offerUrl || ''
      };
    }
  }
  return null;
}

function dataUrlToJpegBase64(dataUrl) {
  if (!dataUrl || typeof dataUrl !== 'string') return '';
  const i = dataUrl.indexOf('base64,');
  return i >= 0 ? dataUrl.slice(i + 7) : dataUrl.replace(/^data:image\/\w+;base64,/, '');
}

async function saveStuckAutomationReportToFirestore(payload) {
  const { taleosUserId, taleosIdToken } = await chrome.storage.local.get(['taleosUserId', 'taleosIdToken']);
  if (!taleosUserId || !taleosIdToken) return;
  const docId = `stuck_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
  const fields = {
    userId: { stringValue: String(taleosUserId) },
    jobId: { stringValue: String(payload.jobId || '') },
    offerUrl: { stringValue: String(payload.offerUrl || '') },
    pageUrl: { stringValue: String(payload.pageUrl || '') },
    bankId: { stringValue: String(payload.bankId || '') },
    createdAt: { timestampValue: new Date().toISOString() }
  };
  const b64 = payload.screenshotBase64 || '';
  if (b64 && b64.length < 900000) {
    fields.screenshotBase64 = { stringValue: b64 };
  } else if (payload.screenshotStoragePath) {
    fields.screenshotStoragePath = { stringValue: String(payload.screenshotStoragePath) };
  }
  const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/stuck_automation_reports/${docId}`;
  const res = await fetch(url, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${taleosIdToken}` },
    body: JSON.stringify({ fields })
  });
  if (!res.ok) console.error('[Taleos] Stuck report Firestore:', await res.text());
}

async function uploadStuckScreenshotToStorage(base64Jpeg, userId, token) {
  const path = `stuck_reports/${userId}/${Date.now()}.jpg`;
  const binStr = atob(base64Jpeg);
  const bytes = new Uint8Array(binStr.length);
  for (let i = 0; i < binStr.length; i++) bytes[i] = binStr.charCodeAt(i);
  const bucket = 'project-taleos.firebasestorage.app';
  const url = `https://firebasestorage.googleapis.com/v0/b/${bucket}/o?name=${encodeURIComponent(path)}&uploadType=media`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'image/jpeg' },
    body: bytes
  });
  if (!res.ok) throw new Error(`Storage ${res.status}`);
  return path;
}

async function sendStuckReportToCloudFunction(payload, idToken) {
  const body = {
    reportType: 'stuck_automation',
    userId: payload.userId,
    jobId: payload.jobId,
    offerUrl: payload.offerUrl,
    pageUrl: payload.pageUrl,
    bankId: payload.bankId,
    screenshotBase64: payload.screenshotBase64 || ''
  };
  const res = await fetch(STUCK_REPORT_CF_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...(idToken ? { Authorization: `Bearer ${idToken}` } : {}) },
    body: JSON.stringify(body)
  });
  return res.ok;
}

async function handleApplyStuckAlarm() {
  const activeRuns = await getActiveApplyRuns();
  const entries = Object.entries(activeRuns || {});
  if (!entries.length) {
    await clearApplyStuckWatchdog();
    return;
  }

  const { taleosUserId, taleosIdToken } = await chrome.storage.local.get(['taleosUserId', 'taleosIdToken']);
  const timeoutMs = APPLY_TIMEOUT_MINUTES * 60 * 1000;

  for (const [tabKey, run] of entries) {
    if (!run || run.outcome === 'timeout') continue;
    const startedMs = Date.parse(run.startedAt || '') || 0;
    if (!startedMs || Date.now() - startedMs < timeoutMs) continue;

    const meta = {
      tabId: Number(tabKey),
      bankId: run.bankId || '',
      jobId: run.jobId || '',
      offerUrl: run.offerUrl || ''
    };

    let pageUrl = '';
    let screenshotBase64 = '';
    let prevActiveId = null;
    try {
      const cur = await chrome.tabs.query({ active: true, currentWindow: true });
      prevActiveId = cur[0]?.id || null;
      await chrome.tabs.update(meta.tabId, { active: true });
      await new Promise((r) => setTimeout(r, 450));
      const tab = await chrome.tabs.get(meta.tabId);
      pageUrl = tab?.url || '';
      const dataUrl = await chrome.tabs.captureVisibleTab(tab.windowId, { format: 'jpeg', quality: 52 });
      screenshotBase64 = dataUrlToJpegBase64(dataUrl);
    } catch (e) {
      console.error('[Taleos] Capture écran stuck:', e);
    } finally {
      if (prevActiveId && prevActiveId !== meta.tabId) {
        try {
          await chrome.tabs.update(prevActiveId, { active: true });
        } catch (_) {}
      }
    }

    const timeoutMessage = 'La candidature n’a pas atteint l’état succès dans les 5 minutes.';
    const timedOutRun = await markTimedOutApplyRun(meta.tabId, {
      failureType: 'timeout',
      failureMessage: timeoutMessage
    });

    const payload = {
      userId: taleosUserId,
      jobId: meta.jobId,
      offerUrl: meta.offerUrl,
      pageUrl,
      bankId: meta.bankId,
      screenshotBase64
    };

    try {
      if (screenshotBase64.length > 700000 && taleosIdToken) {
        const path = await uploadStuckScreenshotToStorage(screenshotBase64, taleosUserId, taleosIdToken);
        payload.screenshotStoragePath = path;
        payload.screenshotBase64 = '';
      }
      await saveStuckAutomationReportToFirestore({
        ...payload,
        screenshotBase64: payload.screenshotBase64 || undefined,
        screenshotStoragePath: payload.screenshotStoragePath
      });
    } catch (e) {
      console.error('[Taleos] Stuck Firestore/Storage:', e);
    }

    try {
      await sendStuckReportToCloudFunction(
        { ...payload, userId: taleosUserId, screenshotBase64: payload.screenshotBase64 || '' },
        taleosIdToken
      );
    } catch (e) {
      console.error('[Taleos] Stuck e-mail CF:', e);
    }

    try {
      await trackError('apply_timeout', timeoutMessage, run.bankId, run.jobId, run.offerUrl);
    } catch (_) {}

    try {
      await notifyTaleosCandidatureFailure({
        jobId: run.jobId,
        error: timeoutMessage
      });
    } catch (_) {}

    if (timedOutRun?.runId) {
      console.warn('[Taleos] Timeout candidature:', timedOutRun.runId, timedOutRun.jobTitle || timedOutRun.jobId || meta.tabId);
    }
  }
}

chrome.storage.onChanged.addListener((changes, area) => {
  if (area !== 'local') return;
  if (changes[ACTIVE_APPLY_RUNS_STORAGE_KEY]) {
    clearApplyStuckWatchdog();
    return;
  }
  const keys = ['taleos_pending_sg', 'taleos_pending_offer', 'taleos_pending_deloitte', 'taleos_pending_bpce', 'taleos_pending_bnp', 'taleos_pending_jp_morgan', 'taleos_pending_goldman_sachs', 'taleos_pending_axa'];
  for (const k of keys) {
    const ch = changes[k];
    if (ch && (ch.newValue === undefined || ch.newValue === null)) {
      clearApplyStuckWatchdog();
      if (k === 'taleos_pending_offer') {
        chrome.storage.local.remove('taleos_ca_apply_tab_id').catch(() => {});
      }
      return;
    }
  }
});

async function injectSgAutomation(tabId, profile) {
  const now = Date.now();
  if (sgLastInject.get(tabId) && now - sgLastInject.get(tabId) < 3000) {
    console.log('[Taleos SG] Injection ignorée (debounce 3s)');
    return;
  }
  sgLastInject.set(tabId, now);
  console.log('[Taleos SG] Injection dans tab', tabId);
  try {
    await new Promise(r => setTimeout(r, 1500));
    const target = { tabId, allFrames: true };
    let sessionRa = null;
    try {
      const s = await chrome.storage.session.get('taleos_remote_automation');
      sessionRa = s.taleos_remote_automation;
    } catch (_) {}
    const useRemoteSg =
      sessionRa &&
      sessionRa.scriptKey === 'societe_generale' &&
      sessionRa.remoteSource &&
      typeof sessionRa.until === 'number' &&
      Date.now() < sessionRa.until;
    if (useRemoteSg) {
      await chrome.scripting.executeScript({
        target,
        files: injectFilesWithBanner(['scripts/job-family-mapping.js', 'scripts/remote-loader.js'])
      });
      await chrome.scripting.executeScript({
        target,
        func: (payload) => {
          if (window.__taleosInjectRemote) window.__taleosInjectRemote(payload.source, payload.data);
        },
        args: [{ source: sessionRa.remoteSource, data: profile }]
      });
      console.log('[Taleos SG] OK — script distant (legacy URL)');
    } else {
      await chrome.scripting.executeScript({
        target,
        files: injectFilesWithBanner(['scripts/job-family-mapping.js', BANK_SCRIPT_MAP.societe_generale])
      });
      await chrome.scripting.executeScript({
        target,
        func: (data) => { if (window.__taleosRun) window.__taleosRun(data); },
        args: [profile]
      });
      console.log('[Taleos SG] OK — bundle local');
    }
  } catch (e) {
    console.error('[Taleos SG] Erreur injection:', e);
  }
}

/** Injection programmatique du taleos-injector (fallback si content_scripts ne s'exécute pas) */
const TALEOS_SITE_PATTERNS = [
  'taleos.co',
  'github.io',
  'localhost',
  '127.0.0.1'
];
chrome.tabs.onUpdated.addListener(async (tabId, info, tab) => {
  if (info.status !== 'complete') return;
  const url = (tab?.url || '');
  const urlLower = url.toLowerCase();
  const isTaleosSite = TALEOS_SITE_PATTERNS.some(p => urlLower.includes(p));
  if (isTaleosSite && (urlLower.startsWith('https://') || urlLower.startsWith('http://'))) {
    const inject = async (retry) => {
      try {
        await chrome.scripting.executeScript({
          target: { tabId },
          files: ['content/taleos-injector.js']
        });
      } catch (e) {
        if (retry < 2) setTimeout(() => inject(retry + 1), 800);
      }
    };
    inject(0);
  }
});

chrome.tabs.onRemoved.addListener(async (tabId) => {
  try {
    const activeRuns = await getActiveApplyRuns();
    if (activeRuns[String(tabId)]) {
      await finalizeApplyRunForTab(tabId, 'aborted', {
        failureType: 'user_closed_tab',
        failureMessage: 'L’utilisateur a fermé l’onglet de candidature avant le succès.'
      }).catch(() => null);
    }
    const state = await chrome.storage.local.get([
      'taleos_sg_tab_id',
      'taleos_bpce_tab_id',
      'taleos_bnp_tab_id',
      'taleos_credit_mutuel_tab_id',
      'taleos_jp_morgan_tab_id',
      'taleos_axa_tab_id',
      'taleos_ca_apply_tab_id',
      'taleos_pending_sg',
      'taleos_pending_bpce',
      'taleos_pending_bnp',
      'taleos_pending_credit_mutuel',
      'taleos_pending_jp_morgan',
      'taleos_pending_goldman_sachs',
      'taleos_pending_axa',
      'taleos_gs_tab_id',
      'taleos_pending_deloitte',
      'taleos_pending_offer',
      'taleos_ca_candidature_pending',
      'taleos_deloitte_did_login_click'
    ]);
    const keysToRemove = new Set();
    if (state.taleos_sg_tab_id === tabId) {
      keysToRemove.add('taleos_pending_sg');
      keysToRemove.add('taleos_sg_tab_id');
    }
    if (state.taleos_bpce_tab_id === tabId || state.taleos_pending_bpce?.tabId === tabId) {
      keysToRemove.add('taleos_pending_bpce');
      keysToRemove.add('taleos_bpce_tab_id');
      keysToRemove.add('taleos_bpce_pin_code');
    }
    if (state.taleos_bnp_tab_id === tabId || state.taleos_pending_bnp?.tabId === tabId) {
      keysToRemove.add('taleos_pending_bnp');
      keysToRemove.add('taleos_bnp_tab_id');
    }
    if (state.taleos_credit_mutuel_tab_id === tabId || state.taleos_pending_credit_mutuel?.tabId === tabId) {
      keysToRemove.add('taleos_pending_credit_mutuel');
      keysToRemove.add('taleos_credit_mutuel_tab_id');
    }
    if (state.taleos_jp_morgan_tab_id === tabId || state.taleos_pending_jp_morgan?.tabId === tabId) {
      keysToRemove.add('taleos_pending_jp_morgan');
      keysToRemove.add('taleos_jp_morgan_tab_id');
    }
    if (state.taleos_gs_tab_id === tabId || state.taleos_pending_goldman_sachs?.tabId === tabId) {
      keysToRemove.add('taleos_pending_goldman_sachs');
      keysToRemove.add('taleos_gs_tab_id');
    }
    if (state.taleos_axa_tab_id === tabId || state.taleos_pending_axa?.tabId === tabId) {
      keysToRemove.add('taleos_pending_axa');
      keysToRemove.add('taleos_axa_tab_id');
    }
    if (state.taleos_ca_apply_tab_id === tabId || state.taleos_ca_candidature_pending?.tabId === tabId) {
      keysToRemove.add('taleos_pending_offer');
      keysToRemove.add('taleos_ca_apply_tab_id');
      keysToRemove.add('taleos_ca_candidature_pending');
      keysToRemove.add('taleos_ca_candidature_reloaded');
      keysToRemove.add('taleos_redirect_fallback');
    }
    if (state.taleos_pending_deloitte?.tabId === tabId) {
      keysToRemove.add('taleos_pending_deloitte');
      keysToRemove.add('taleos_deloitte_did_login_click');
    }
    if (keysToRemove.size) {
      await chrome.storage.local.remove(Array.from(keysToRemove));
    }
  } catch (_) {}
});

chrome.tabs.onUpdated.addListener(async (tabId, info, tab) => {
  if (info.status !== 'complete') return;
  const url = (tab?.url || '').toLowerCase();
  if (!url.includes('socgen.taleo.net')) return;
  const { taleos_pending_sg, taleos_sg_tab_id } = await chrome.storage.local.get(['taleos_pending_sg', 'taleos_sg_tab_id']);
  if (!taleos_pending_sg) return;
  if (tabId !== taleos_sg_tab_id) return;
  const age = Date.now() - (taleos_pending_sg.timestamp || 0);
  if (age > 3 * 60 * 1000) {
    chrome.storage.local.remove(['taleos_pending_sg', 'taleos_sg_tab_id']);
    return;
  }
  const { profile } = taleos_pending_sg;
  if (!profile) return;
  injectSgAutomation(tabId, profile);
});

/** Listener persistant CA candidature : injecte phase 3 après reload (fallback si handleApply listener perdu) */
chrome.tabs.onUpdated.addListener(async (tabId, info, tab) => {
  if (info.status !== 'complete') return;
  const url = (tab?.url || '').toLowerCase();
  if (!url.includes('groupecreditagricole.jobs')) return;
  if (!url.includes('/candidature/') && !url.includes('/application/') && !url.includes('/apply/')) return;
  const { taleos_ca_candidature_pending } = await chrome.storage.local.get('taleos_ca_candidature_pending');
  if (!taleos_ca_candidature_pending?.profile || taleos_ca_candidature_pending.tabId !== tabId) return;
  const age = Date.now() - (taleos_ca_candidature_pending.timestamp || 0);
  if (age > 2 * 60 * 1000) {
    chrome.storage.local.remove(['taleos_ca_candidature_pending', 'taleos_ca_candidature_reloaded']);
    return;
  }
  if (caLastInject.get(tabId) && Date.now() - caLastInject.get(tabId) < 8000) return;
  caLastInject.set(tabId, Date.now());
  chrome.storage.local.remove(['taleos_ca_candidature_pending', 'taleos_ca_candidature_reloaded']);
  console.log('[Taleos CA] Injection phase 3 (listener persistant candidature)');
  await new Promise(r => setTimeout(r, 6000));
  try {
    await chrome.scripting.executeScript({
      target: { tabId },
      files: injectBankFiles('credit_agricole', [BANK_SCRIPT_MAP.credit_agricole])
    });
    await chrome.scripting.executeScript({
      target: { tabId },
      func: (data) => { if (window.__taleosRun) window.__taleosRun(data); },
      args: [taleos_ca_candidature_pending.profile]
    });
  } catch (e) {
    console.error('[Taleos CA] Injection phase 3:', e);
  }
});

/** Listener persistant CA : injecte sur page offre après connexion (fallback si handleApply listener perdu) */
chrome.tabs.onUpdated.addListener(async (tabId, info, tab) => {
  if (info.status !== 'complete') return;
  const url = (tab?.url || '').toLowerCase();
  if (!url.includes('groupecreditagricole.jobs')) return;
  if (!url.includes('/nos-offres-emploi/') && !url.includes('/our-offers/') && !url.includes('/our-offres/')) return;
  const { taleos_pending_offer } = await chrome.storage.local.get('taleos_pending_offer');
  if (!taleos_pending_offer?.profile) return;
  const age = Date.now() - (taleos_pending_offer.timestamp || 0);
  if (age > 3 * 60 * 1000) {
    chrome.storage.local.remove(['taleos_pending_offer', 'taleos_redirect_fallback']);
    return;
  }
  if (caLastInject.get(tabId) && Date.now() - caLastInject.get(tabId) < 5000) return;
  caLastInject.set(tabId, Date.now());
  const { profile } = taleos_pending_offer;
  chrome.storage.local.remove('taleos_pending_offer');
  console.log('[Taleos CA] Injection page offre (listener persistant)');
  await new Promise(r => setTimeout(r, 2000));
  try {
    await chrome.scripting.executeScript({
      target: { tabId },
      files: injectBankFiles('credit_agricole', [BANK_SCRIPT_MAP.credit_agricole])
    });
    await chrome.scripting.executeScript({
      target: { tabId },
      func: (data) => { if (window.__taleosRun) window.__taleosRun(data); },
      args: [{ ...profile, __phase: 2 }]
    });
  } catch (e) {
    console.error('[Taleos CA] Injection:', e);
  }
});

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (msg.action === 'ping') {
    sendResponse({ ok: true });
    return true;
  }
  if (msg.action === 'taleos_get_current_tab_id') {
    sendResponse({ tabId: sender.tab?.id || null });
    return true;
  }
  if (msg.action === 'ca_offer_page_ready') {
    const tabId = sender.tab?.id;
    if (!tabId) return;
    chrome.storage.local.get('taleos_pending_offer').then(async (s) => {
      const { taleos_pending_offer } = s;
      if (!taleos_pending_offer?.profile) return;
      const age = Date.now() - (taleos_pending_offer.timestamp || 0);
      if (age > 3 * 60 * 1000) {
        chrome.storage.local.remove(['taleos_pending_offer', 'taleos_redirect_fallback']);
        return;
      }
      if (caLastInject.get(tabId) && Date.now() - caLastInject.get(tabId) < 5000) return;
      caLastInject.set(tabId, Date.now());
      const { profile } = taleos_pending_offer;
      chrome.storage.local.remove('taleos_pending_offer');
      console.log('[Taleos CA] Injection page offre (message ca_offer_page_ready)');
      await new Promise(r => setTimeout(r, 1500));
      try {
        await chrome.scripting.executeScript({
          target: { tabId },
          files: injectBankFiles('credit_agricole', [BANK_SCRIPT_MAP.credit_agricole])
        });
        await chrome.scripting.executeScript({
          target: { tabId },
          func: (data) => { if (window.__taleosRun) window.__taleosRun(data); },
          args: [{ ...profile, __phase: 2 }]
        });
      } catch (e) {
        console.error('[Taleos CA] Injection:', e);
      }
    });
    sendResponse({ ok: true });
    return true;
  }
  if (msg.action === 'sg_page_loaded') {
    const tabId = sender.tab?.id;
    console.log('[Taleos SG] sg_page_loaded reçu, tabId:', tabId);
    if (tabId) {
      chrome.storage.local.get(['taleos_pending_sg', 'taleos_sg_tab_id']).then(({ taleos_pending_sg, taleos_sg_tab_id }) => {
        if (!taleos_pending_sg?.profile) return;
        if (tabId !== taleos_sg_tab_id) return;
        const age = Date.now() - (taleos_pending_sg.timestamp || 0);
        if (age > 3 * 60 * 1000) {
          chrome.storage.local.remove(['taleos_pending_sg', 'taleos_sg_tab_id']);
          return;
        }
        injectSgAutomation(tabId, taleos_pending_sg.profile);
      });
    }
    return;
  }
  if (msg.action === 'after_login_submit') {
      const { offerUrl, bankId, profile } = msg;
    chrome.storage.local.set({ taleos_redirect_fallback: offerUrl });
    chrome.storage.local.remove('taleos_pending_offer');
    const tabId = sender.tab?.id;
    if (tabId) {
      const scriptPath = BANK_SCRIPT_MAP[bankId] || BANK_SCRIPT_MAP.credit_agricole;
      const injectAndRun = (phase) => {
        const p = { ...profile, __phase: phase, __jobId: profile.__jobId, __jobTitle: profile.__jobTitle, __companyName: profile.__companyName, __offerUrl: offerUrl };
        chrome.scripting.executeScript({ target: { tabId }, files: injectBankFiles(bankId, [scriptPath]) }).then(() =>
          chrome.scripting.executeScript({
            target: { tabId },
            func: (data) => { if (window.__taleosRun) window.__taleosRun(data); },
            args: [p]
          })
        ).catch(e => console.error('[Taleos] Inject après login:', e));
      };
      let done = false;
      const handleUrl = (url) => {
        if (done) return;
        const u = (url || '').toLowerCase();
        if (u.includes('/candidature/') || u.includes('/application/') || u.includes('/apply/')) { done = true; chrome.storage.local.remove('taleos_redirect_fallback'); injectAndRun(3); return; }
        if (u.includes('/nos-offres-emploi/') || u.includes('/our-offers/') || u.includes('/our-offres/')) { done = true; chrome.storage.local.remove('taleos_redirect_fallback'); injectAndRun(2); return; }
        if (offerUrl && !done) {
          done = true;
          chrome.tabs.update(tabId, { url: offerUrl });
          chrome.tabs.onUpdated.addListener(function rel(id, inf) {
            if (id !== tabId || inf.status !== 'complete') return;
            chrome.tabs.onUpdated.removeListener(rel);
            injectAndRun(2);
          });
        }
      };
      chrome.tabs.get(tabId).then(t => {
        const url = (t?.url || '').toLowerCase();
        if (!url.includes('/connexion') && !url.includes('/login') && !url.includes('/connection')) handleUrl(t?.url || '');
      }).catch(() => {});
      const listener = async (id, info) => {
        if (id !== tabId || info.status !== 'complete') return;
        try {
          const t = await chrome.tabs.get(tabId);
          const url = (t?.url || '').toLowerCase();
          if (url.includes('/connexion') || url.includes('/login') || url.includes('/connection')) return;
          if (url.includes('admin-ajax')) {
            chrome.tabs.onUpdated.removeListener(listener);
            setTimeout(() => {
              chrome.tabs.get(tabId).then(t => {
                if (t?.url?.toLowerCase().includes('admin-ajax')) {
                  chrome.tabs.update(tabId, { url: offerUrl });
                }
              }).catch(() => {});
            }, 8000);
            return;
          }
          chrome.tabs.onUpdated.removeListener(listener);
          handleUrl(url);
        } catch (_) {}
      };
      chrome.tabs.onUpdated.addListener(listener);
      setTimeout(() => {
        chrome.tabs.onUpdated.removeListener(listener);
        if (!done) chrome.tabs.get(tabId).then(t => handleUrl(t?.url || '')).catch(() => {});
      }, 35000);
      setTimeout(() => {
        if (done) return;
        chrome.tabs.get(tabId).then(async (t) => {
          const url = (t?.url || '').toLowerCase();
          if (url.includes('/connexion') || url.includes('/login') || url.includes('/connection') || url.includes('admin-ajax')) {
            chrome.tabs.update(tabId, { url: offerUrl });
          }
        }).catch(() => {});
      }, 10000);
    }
    sendResponse({ ok: true });
    return true;
  }
  if (msg.action === 'inject_auth_sync') {
    const tabId = sender.tab?.id;
    if (tabId) {
      const forceRefresh = !!msg.forceRefresh;
      chrome.scripting.executeScript({
        target: { tabId },
        world: 'MAIN',
        func: function(doRefresh) {
          if (typeof firebase === 'undefined' || !firebase.auth) return;
          function sendToken(u, r) {
            if (!u) return;
            u.getIdToken(!!r).then(function(t) {
              window.dispatchEvent(new CustomEvent('__TALEOS_AUTH_SYNC__', {
                detail: { token: t, uid: u.uid, email: u.email || '' }
              }));
            });
          }
          var u = firebase.auth().currentUser;
          if (u) sendToken(u, doRefresh);
          else firebase.auth().onAuthStateChanged(function(user) { if (user) sendToken(user, doRefresh); });
        },
        args: [forceRefresh]
      }).catch(function() {});
    }
    sendResponse({ ok: true });
    return true;
  }
  if (msg.action === 'sync_auth_from_site') {
    const { taleosUserId, taleosIdToken, taleosUserEmail } = msg;
    if (taleosUserId && taleosIdToken) {
      chrome.storage.local.set({
        taleosUserId,
        taleosIdToken,
        taleosUserEmail: taleosUserEmail || ''
      });
      if (authSyncResolve) {
        authSyncResolve();
        authSyncResolve = null;
      }
    }
    sendResponse({ ok: true });
    return true;
  }
  if (msg.action === 'gmail_get_link_status') {
    (async () => {
      try {
        const { taleosUserId, taleosIdToken } = await chrome.storage.local.get(['taleosUserId', 'taleosIdToken']);
        if (!taleosUserId) {
          sendResponse({ ok: false, message: 'Utilisateur non connecté' });
          return;
        }
        const status = await getGmailAuthState(taleosUserId, taleosIdToken);
        sendResponse({ ok: true, ...status });
      } catch (e) {
        sendResponse({ ok: false, message: e.message || 'Erreur statut Gmail' });
      }
    })();
    return true;
  }
  if (msg.action === 'gmail_link_save_token') {
    (async () => {
      try {
        const { taleosUserId, taleosIdToken } = await chrome.storage.local.get(['taleosUserId', 'taleosIdToken']);
        if (!taleosUserId || !taleosIdToken) {
          sendResponse({ ok: false, message: 'Session Taleos manquante' });
          return;
        }
        const accessToken = String(msg.accessToken || '').trim();
        if (!accessToken) {
          sendResponse({ ok: false, message: 'Token Gmail manquant' });
          return;
        }
        const ttl = Number(msg.expiresInSec || 3600);
        const authObj = {
          access_token: accessToken,
          gmail_email: String(msg.gmailEmail || '').trim(),
          scope: GMAIL_REQUIRED_SCOPE,
          created_at: Date.now(),
          expires_at: Date.now() + Math.max(300, ttl) * 1000
        };
        await chrome.storage.local.set({ [getGmailStorageKey(taleosUserId)]: authObj });
        await saveGmailIntegrationToFirestore(taleosUserId, taleosIdToken, {
          status: 'connected',
          gmail_email: authObj.gmail_email
        });
        sendResponse({ ok: true });
      } catch (e) {
        sendResponse({ ok: false, message: e.message || 'Erreur liaison Gmail' });
      }
    })();
    return true;
  }
  if (msg.action === 'gmail_unlink') {
    (async () => {
      try {
        const { taleosUserId, taleosIdToken } = await chrome.storage.local.get(['taleosUserId', 'taleosIdToken']);
        if (!taleosUserId || !taleosIdToken) {
          sendResponse({ ok: false, message: 'Session Taleos manquante' });
          return;
        }
        const key = getGmailStorageKey(taleosUserId);
        const oldAuth = (await chrome.storage.local.get(key))[key] || null;
        await chrome.storage.local.remove(key);
        if (oldAuth && oldAuth.access_token) {
          fetch(`https://oauth2.googleapis.com/revoke?token=${encodeURIComponent(oldAuth.access_token)}`, { method: 'POST' }).catch(() => {});
        }
        await saveGmailIntegrationToFirestore(taleosUserId, taleosIdToken, { status: 'disconnected', gmail_email: '' });
        sendResponse({ ok: true });
      } catch (e) {
        sendResponse({ ok: false, message: e.message || 'Erreur déliaison Gmail' });
      }
    })();
    return true;
  }

    if (msg.action === 'gmail_link_direct') {
    (async () => {
      try {
        const { taleosUserId, taleosIdToken } = await chrome.storage.local.get(['taleosUserId', 'taleosIdToken']);
        if (!taleosUserId || !taleosIdToken) {
          sendResponse({ ok: false, message: 'Session Taleos manquante. Reconnectez-vous.' });
          return;
        }
        const redirectUrl = chrome.identity.getRedirectURL('gmail');
        // ⚠️ REMPLACER PAR VOTRE CLIENT ID OAuth 2.0
        // Google Cloud Console → APIs & Services → Credentials → votre app OAuth
        // Format : 747525128323-XXXXXXXXXXXXXXXX.apps.googleusercontent.com
        // Puis ajouter chrome.identity.getRedirectURL('gmail') dans les URI de redirection autorisées
        const clientId = '747525128323-REMPLACER_PAR_VOTRE_CLIENT_ID.apps.googleusercontent.com';
        const scope = encodeURIComponent(GMAIL_REQUIRED_SCOPE);
        const authUrl =
          `https://accounts.google.com/o/oauth2/v2/auth` +
          `?client_id=${encodeURIComponent(clientId)}` +
          `&redirect_uri=${encodeURIComponent(redirectUrl)}` +
          `&response_type=token` +
          `&scope=${scope}` +
          `&prompt=select_account`;

        const redirected = await chrome.identity.launchWebAuthFlow({
          url: authUrl,
          interactive: true
        });

        if (!redirected) throw new Error('Authentification Gmail annulée ou fenêtre fermée');

        const hashPart = redirected.includes('#') ? redirected.split('#')[1] : redirected.split('?')[1] || '';
        const params = new URLSearchParams(hashPart);
        const accessToken = params.get('access_token');
        const expiresIn = parseInt(params.get('expires_in') || '3600', 10);

        if (!accessToken) throw new Error('Token Gmail non reçu. Vérifiez le Client ID OAuth et les URI autorisées.');

        // Récupérer l'email Gmail via l'API Google
        const userRes = await fetch('https://www.googleapis.com/gmail/v1/users/me/profile', {
          headers: { Authorization: `Bearer ${accessToken}` }
        });
        const userJson = userRes.ok ? await userRes.json() : {};
        const gmailEmail = userJson.emailAddress || '';

        const authObj = {
          access_token: accessToken,
          gmail_email: gmailEmail,
          scope: GMAIL_REQUIRED_SCOPE,
          created_at: Date.now(),
          expires_at: Date.now() + Math.max(300, expiresIn) * 1000
        };
        await chrome.storage.local.set({ [getGmailStorageKey(taleosUserId)]: authObj });
        await saveGmailIntegrationToFirestore(taleosUserId, taleosIdToken, {
          status: 'connected',
          gmail_email: gmailEmail
        });
        sendResponse({ ok: true, gmail_email: gmailEmail });
      } catch (e) {
        sendResponse({ ok: false, message: e.message || 'Erreur liaison Gmail' });
      }
    })();
    return true;
  }

  
  if (msg.action === 'outlook_get_link_status') {
    (async () => {
      try {
        const { taleosUserId, taleosIdToken } = await chrome.storage.local.get(['taleosUserId', 'taleosIdToken']);
        if (!taleosUserId || !taleosIdToken) {
          sendResponse({ ok: false, message: 'Session Taleos manquante' });
          return;
        }
        const st = await getOutlookIntegrationState(taleosUserId, taleosIdToken);
        sendResponse({ ok: true, ...st });
      } catch (e) {
        sendResponse({ ok: false, message: e.message || 'Erreur statut Outlook' });
      }
    })();
    return true;
  }
  if (msg.action === 'outlook_link') {
    (async () => {
      try {
        const { taleosUserId, taleosIdToken } = await chrome.storage.local.get(['taleosUserId', 'taleosIdToken']);
        if (!taleosUserId || !taleosIdToken) {
          sendResponse({ ok: false, message: 'Session Taleos manquante' });
          return;
        }
        const { verifier, challenge } = await buildPkce();
        const outlookClientId = await getOutlookOAuthClientId();
        const redirectUri = chrome.identity.getRedirectURL('microsoft');
        const authUrl = `https://login.microsoftonline.com/common/oauth2/v2.0/authorize?client_id=${encodeURIComponent(outlookClientId)}&response_type=code&redirect_uri=${encodeURIComponent(redirectUri)}&response_mode=query&scope=${encodeURIComponent(OUTLOOK_OAUTH_SCOPE)}&code_challenge=${encodeURIComponent(challenge)}&code_challenge_method=S256&prompt=select_account`;
        const redirected = await chrome.identity.launchWebAuthFlow({ url: authUrl, interactive: true });
        if (!redirected) throw new Error('Redirection OAuth Outlook absente');
        const u = new URL(redirected);
        const code = u.searchParams.get('code');
        if (!code) throw new Error('Code OAuth Outlook introuvable');
        await exchangeOutlookCodeWithBackend(code, verifier, redirectUri);
        await setOutlookLocalState(taleosUserId, { connected: true, outlook_email: '' });
        try {
          await saveOutlookIntegrationToFirestore(taleosUserId, taleosIdToken, { status: 'connected', outlook_email: '' });
        } catch (_) {
          // Le lien OAuth est déjà actif côté backend; on garde un état local si Firestore refuse.
        }
        sendResponse({ ok: true });
      } catch (e) {
        sendResponse({ ok: false, message: e.message || 'Erreur liaison Outlook' });
      }
    })();
    return true;
  }
  if (msg.action === 'outlook_unlink') {
    (async () => {
      try {
        const { taleosUserId, taleosIdToken } = await chrome.storage.local.get(['taleosUserId', 'taleosIdToken']);
        if (!taleosUserId || !taleosIdToken) {
          sendResponse({ ok: false, message: 'Session Taleos manquante' });
          return;
        }
        fetch(OUTLOOK_UNLINK_CF_URL, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${taleosIdToken}` },
          body: JSON.stringify({})
        }).catch(() => {});
        await setOutlookLocalState(taleosUserId, { connected: false, outlook_email: '' });
        try {
          await saveOutlookIntegrationToFirestore(taleosUserId, taleosIdToken, {
            status: 'disconnected',
            outlook_email: ''
          });
        } catch (_) {
          // Non bloquant: la déliaison backend est demandée et l'état local est vidé.
        }
        sendResponse({ ok: true });
      } catch (e) {
        sendResponse({ ok: false, message: e.message || 'Erreur déliaison Outlook' });
      }
    })();
    return true;
  }
  if (msg.action === 'bpce_pin_code') {
    const pinCode = String(msg.pinCode || '').trim();
    if (/^\d{6}$/.test(pinCode)) {
      chrome.storage.local.set({ taleos_bpce_pin_code: pinCode });
      sendResponse({ ok: true });
    } else {
      sendResponse({ ok: false, message: 'PIN invalide' });
    }
    return true;
  }
  if (msg.action === 'test_credentials') {
    testCredentials(msg.bankId).then(sendResponse).catch(e => sendResponse({ ok: false, error: e.message }));
    return true;
  }
  if (msg.action === 'test_connection') {
    runTestConnection(msg).then(sendResponse).catch(e => sendResponse({ success: false, message: e.message || 'Erreur' }));
    return true;
  }
  if (msg.action === 'taleos_check_profile_complete') {
    checkProfileCompletenessFromFirestore(msg.bankId)
      .then((res) => sendResponse(typeof res === 'object' ? res : { complete: !!res }))
      .catch(e => sendResponse({ complete: false, error: e.message, missingFields: [] }));
    return true;
  }
  if (msg.action === 'taleos_apply') {
    const taleosTabId = sender.tab?.id;
    if (String(msg.bankId || '').toLowerCase() === 'credit_mutuel') {
      chrome.storage.local.set({
        [CREDIT_MUTUEL_LAST_APPLY_KEY]: {
          offerUrl: msg.offerUrl || '',
          jobId: msg.jobId || '',
          jobTitle: msg.jobTitle || '',
          companyName: msg.companyName || 'Crédit Mutuel',
          offerMeta: msg.offerMeta || null,
          taleosTabId: taleosTabId || null,
          timestamp: Date.now()
        }
      }).catch(() => {});
    }
    // Tracking non bloquant du démarrage de candidature.
    trackApplyStart(msg.bankId, msg.jobTitle, msg.jobId, msg.offerUrl).catch(() => {});
    handleApply(msg.offerUrl, msg.bankId, msg.jobId, msg.jobTitle, msg.companyName, taleosTabId, msg.offerMeta || null)
      .then((result) => {
        if (result?.error) sendResponse({ error: result.error, openUrl: true });
        else {
          sendResponse({
            ok: true,
            pilotTier: result.pilotTier,
            pilotLabel: result.pilotLabel,
            routingSource: result.routingSource,
            automationSource: result.automationSource,
          });
        }
      })
      .catch(e => sendResponse({ error: e.message || 'Erreur', openUrl: true }));
    return true;
  }
  if (msg.action === 'taleos_rehydrate_credit_mutuel_pending') {
    (async () => {
      try {
        const tabId = sender.tab?.id;
        const pageUrl = String(sender.tab?.url || msg.offerUrl || '').trim();
        if (!tabId || !pageUrl) {
          sendResponse({ ok: false, error: 'Onglet Crédit Mutuel introuvable' });
          return;
        }

        const {
          taleosUserId,
          taleosIdToken,
          [CREDIT_MUTUEL_LAST_APPLY_KEY]: lastApply
        } = await chrome.storage.local.get(['taleosUserId', 'taleosIdToken', CREDIT_MUTUEL_LAST_APPLY_KEY]);

        if (!taleosUserId || !taleosIdToken) {
          sendResponse({ ok: false, error: 'Utilisateur non connecté' });
          return;
        }
        if (!lastApply?.offerUrl || Date.now() - Number(lastApply.timestamp || 0) > 10 * 60 * 1000) {
          sendResponse({ ok: false, error: 'Contexte Crédit Mutuel trop ancien' });
          return;
        }
        if (String(lastApply.offerUrl).trim() !== pageUrl) {
          sendResponse({ ok: false, error: 'Offre Crédit Mutuel non correspondante' });
          return;
        }

        const profileCheck = await checkProfileCompletenessFromFirestore('credit_mutuel');
        if (!profileCheck?.complete) {
          sendResponse({ ok: false, error: 'Profil incomplet', missingFields: profileCheck?.missingFields || [] });
          return;
        }

        const profile = await fetchProfile(taleosUserId, 'credit_mutuel', taleosIdToken);
        profile.__jobId = lastApply.jobId || '';
        profile.__jobTitle = lastApply.jobTitle || '';
        profile.__companyName = lastApply.companyName || 'Crédit Mutuel';
        profile.__offerUrl = lastApply.offerUrl || pageUrl;
        profile.__offerMeta = lastApply.offerMeta || {};

        await chrome.storage.local.set({
          taleos_pending_credit_mutuel: {
            profile: { ...profile },
            offerUrl: profile.__offerUrl,
            jobId: profile.__jobId,
            jobTitle: profile.__jobTitle,
            companyName: profile.__companyName,
            tabId,
            timestamp: Date.now()
          },
          taleos_credit_mutuel_tab_id: tabId
        });
        sendResponse({ ok: true });
      } catch (e) {
        sendResponse({ ok: false, error: e.message || 'Réhydratation Crédit Mutuel impossible' });
      }
    })();
    return true;
  }
  if (msg.action === 'taleos_setup_for_open_tab') {
    const careersTabId = sender.tab?.id;
    if (!careersTabId) {
      sendResponse({ error: 'Onglet introuvable' });
      return false;
    }
    const { offerUrl, bankId, jobId, jobTitle, companyName } = msg;
    chrome.storage.local.remove('taleos_apply_fallback');
    (async () => {
      try {
        const { taleosUserId, taleosIdToken } = await chrome.storage.local.get(['taleosUserId', 'taleosIdToken']);
        if (!taleosUserId) {
          sendResponse({ error: 'Utilisateur non connecté' });
          return;
        }
        const profileCheck = await checkProfileCompletenessFromFirestore(bankId || 'societe_generale');
        if (!profileCheck?.complete) {
          sendResponse({
            error: 'Profil incomplet. Complétez toutes les informations requises dans Mon profil sur Taleos avant de candidater.',
            missingFields: profileCheck?.missingFields || []
          });
          return;
        }
        const profile = await fetchProfile(taleosUserId, bankId || 'societe_generale', taleosIdToken);
        profile.__jobId = jobId;
        profile.__jobTitle = jobTitle || '';
        profile.__companyName = companyName || 'Société Générale';
        profile.__offerUrl = offerUrl;
        chrome.storage.local.remove('taleos_sg_navigate_profile_attempted');
        chrome.storage.local.set({
          taleos_pending_sg: {
            profile: { ...profile, __jobId: jobId, __jobTitle: jobTitle, __companyName: companyName, __offerUrl: offerUrl },
            offerUrl, jobId, jobTitle, companyName,
            timestamp: Date.now()
          },
          taleos_sg_tab_id: careersTabId
        });
        scheduleApplyStuckWatchdog();
        sendResponse({ ok: true });
      } catch (e) {
        sendResponse({ error: e.message || 'Erreur profil' });
      }
    })();
    return true;
  }
  if (msg.action === 'candidature_success') {
    (async () => {
      const tabIdToClose = sender.tab?.id;
      const runInfo = await finalizeApplyRunForTab(tabIdToClose, 'success', {
        successType: msg.successType,
        successMessage: msg.successMessage || msg.message
      }).catch(() => null);
      clearPendingStateForBank(msg.bankId, sender.tab?.id).catch(() => {});
      trackApplySuccess(msg.bankId, msg.jobTitle, msg.jobId, msg.offerUrl).catch(() => {});
      saveCandidatureAndNotifyTaleos({
        ...msg,
        applyRunId: runInfo?.runId || '',
        extensionVersion: runInfo?.extensionVersion || getExtensionVersionForGa4().extension_version,
        extensionVersionName: runInfo?.extensionVersionName || getExtensionVersionForGa4().extension_version_name
      }, tabIdToClose).then(sendResponse).catch(e => sendResponse({ error: e.message }));
    })();
    return true;
  }
  if (msg.action === 'candidature_failure') {
    (async () => {
      const { offerExpired, jobId, jobTitle, error } = msg;
      const isExpired = !!offerExpired || /404|non disponible|expirée|n'est plus en ligne/i.test(error || '');
      await finalizeApplyRunForTab(sender.tab?.id, 'failed', {
        failureType: isExpired ? 'offer_expired' : 'failure',
        failureMessage: error || (isExpired ? 'Offre expirée' : 'Erreur candidature')
      }).catch(() => null);
      if (isExpired) {
        trackApplyExpired(msg.bankId, jobTitle, jobId, msg.offerUrl, error).catch(() => {});
        upsertGlobalExpiredJobSignal({
          jobId,
          jobTitle,
          offerUrl: msg.offerUrl || '',
          source: msg.bankId || ''
        }).catch(() => {});
      } else {
        trackError('apply_failure', error || 'Erreur candidature', msg.bankId, jobId, msg.offerUrl).catch(() => {});
      }
      clearPendingStateForBank(msg.bankId, sender.tab?.id).catch(() => {});
      if (sender.tab?.id && isExpired) chrome.tabs.remove(sender.tab.id).catch(() => {});
      if (isExpired && jobId) {
        notifyTaleosOfferUnavailable({ jobId, jobTitle: jobTitle || '' }).then(() => sendResponse({ ok: true })).catch(e => sendResponse({ error: e.message }));
      } else {
        notifyTaleosCandidatureFailure(msg).then(() => sendResponse({ ok: true })).catch(e => sendResponse({ error: e.message }));
      }
    })();
    return true;
  }
  if (msg.action === 'extension_run_log') {
    (async () => {
      const tabId = msg.tabId || sender.tab?.id;
      await appendApplyRunLogForTab(tabId, {
        ts: msg.ts || nowIso(),
        source: msg.source || sender.tab?.url || 'extension',
        level: msg.level || 'info',
        message: msg.message || ''
      }).catch(() => null);
      sendResponse({ ok: true });
    })();
    return true;
  }
  if (msg.action === 'reload_and_continue') {
    reloadAndContinue(sender.tab.id, msg.offerUrl, msg.bankId, msg.profile)
      .then(() => sendResponse({ ok: true }))
      .catch(e => sendResponse({ error: e.message }));
    return true;
  }
  if (msg.action === 'fetch_storage_file') {
    fetchStorageFileAsBase64(msg.storagePath).then(sendResponse).catch(e => sendResponse({ error: e.message }));
    return true;
  }
});

async function reloadAndContinue(tabId, offerUrl, bankId, profile) {
  await chrome.tabs.update(tabId, { url: offerUrl });
  const listener = (id, info) => {
    if (id !== tabId || info.status !== 'complete') return;
    chrome.tabs.onUpdated.removeListener(listener);
    const scriptPath = BANK_SCRIPT_MAP[bankId] || BANK_SCRIPT_MAP.credit_agricole;
    chrome.scripting.executeScript({ target: { tabId }, files: injectBankFiles(bankId, [scriptPath]) }).then(() =>
      chrome.scripting.executeScript({
        target: { tabId },
        func: (data) => { if (window.__taleosRun) window.__taleosRun(data); },
        args: [{ ...profile, __phase: 2 }]
      })
    ).catch(e => console.error('[Taleos] Re-inject:', e));
  };
  chrome.tabs.onUpdated.addListener(listener);
}

const CA_CONNEXION_URL = 'https://groupecreditagricole.jobs/fr/connexion/';
function buildAxaApplyUrl(jobUrl, localeHint = '') {
  const match = String(jobUrl || '').match(/\/jobs\/(\d+)(?:[/?#]|$)/i);
  if (!match) return jobUrl;
  const jobId = match[1];
  const normalizedLocale = String(localeHint || '').toLowerCase();
  if (jobId === '16638') {
    return `https://careers-en-axa.icims.com/jobs/${jobId}/login?mobile=false&width=1331&height=500&bga=true&needsRedirect=false&jan1offset=60&jun1offset=120`;
  }
  if (normalizedLocale.includes('en')) {
    return `https://careers-en-axa.icims.com/jobs/${jobId}/login?mobile=false&width=1331&height=500&bga=true&needsRedirect=false&jan1offset=60&jun1offset=120`;
  }
  return `https://careers-fr-axa.icims.com/jobs/${jobId}/login?loginOnly=1&in_iframe=1`;
}

async function resolveAxaApplyUrl(jobUrl, companyName = '', jobTitle = '', offerMeta = null) {
  const normalizedUrl = String(jobUrl || '').toLowerCase();
  const normalizedCompany = String(companyName || '').toLowerCase();
  const normalizedTitle = String(jobTitle || '').toLowerCase();
  const normalizedLocation = String(offerMeta?.location || '').toLowerCase();

  if (normalizedCompany.includes('axa xl')) return buildAxaApplyUrl(jobUrl, 'en');
  if (normalizedTitle.includes('underwriter') || normalizedLocation.includes('paris - france')) {
    if (normalizedCompany.includes('axa xl')) return buildAxaApplyUrl(jobUrl, 'en');
  }
  if (normalizedUrl.includes('lang=en')) return buildAxaApplyUrl(jobUrl, 'en');
  if (normalizedUrl.includes('lang=fr')) return buildAxaApplyUrl(jobUrl, 'fr');

  try {
    const res = await fetch(jobUrl, { credentials: 'omit', redirect: 'follow' });
    const html = await res.text();
    const lc = html.toLowerCase();
    if (
      lc.includes('"language":"en-us"') ||
      lc.includes('lang="en"') ||
      lc.includes('lang="en-us"') ||
      lc.includes('hreflang="en-us"')
    ) {
      return buildAxaApplyUrl(jobUrl, 'en');
    }
    if (
      lc.includes('"language":"fr-fr"') ||
      lc.includes('lang="fr"') ||
      lc.includes('lang="fr-fr"') ||
      lc.includes('hreflang="fr-fr"')
    ) {
      return buildAxaApplyUrl(jobUrl, 'fr');
    }
  } catch (e) {
    console.warn('[Taleos AXA] Impossible de résoudre la langue de l’offre, fallback fr:', e?.message || e);
  }

  return buildAxaApplyUrl(jobUrl, 'fr');
}

const CONNECTION_TEST_URLS = {
  credit_agricole: 'https://groupecreditagricole.jobs/fr/connexion/',
  bnp_paribas: 'https://bwelcome.hr.bnpparibas/fr_FR/externalcareers/Login',
  societe_generale: 'https://socgen.taleo.net/careersection/iam/accessmanagement/login.jsf?lang=fr-FR&redirectionURI=https%3A%2F%2Fsocgen.taleo.net%2Fcareersection%2Fsgcareers%2Fprofile.ftl%3Flang%3Dfr-FR%26src%3DCWS-1%26pcid%3Dmjlsx8hz6i4vn92z&TARGET=https%3A%2F%2Fsocgen.taleo.net%2Fcareersection%2Fsgcareers%2Fprofile.ftl%3Flang%3Dfr-FR%26src%3DCWS-1%26pcid%3Dmjlsx8hz6i4vn92z',
  deloitte: 'https://fina.wd103.myworkdayjobs.com/fr-FR/DeloitteRecrute',
  bpifrance: 'https://bpi.tzportal.io//fr/login',
  allianz: 'https://career5.successfactors.eu/career?company=AZGROUPPROD&site=&lang=en_GB&login_ns=login&loginFlowRequired=true&showLogOutMsg=true&brandUrl=&_s.crb=vGbBbLMSiPxDaedOIn8tTt8WApNMjWQcgDbELe1OyzA%253d',
  axa: 'https://careers.axa.com/careers-home/auth/1/verify-login-type'
};

async function saveCareerConnectionToFirestore(uid, token, bankId, bankName, email, passwordEncoded) {
  const base = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;
  const docPath = `profiles/${uid}/career_connections/${bankId}`;
  const body = {
    fields: {
      bankName: { stringValue: bankName || '' },
      bankId: { stringValue: bankId || '' },
      email: { stringValue: email || '' },
      password: { stringValue: passwordEncoded || '' },
      status: { stringValue: 'connected' },
      timestamp: { timestampValue: new Date().toISOString() }
    }
  };
  let res = await fetch(`${base}/${docPath}`, {
    method: 'PATCH',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${token}`
    },
    body: JSON.stringify(body)
  });
  if (res.status === 404) {
    const parentPath = `profiles/${uid}/career_connections`;
    res = await fetch(`${base}/${parentPath}?documentId=${encodeURIComponent(bankId)}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`
      },
      body: JSON.stringify(body)
    });
  }
  if (!res.ok) throw new Error(await res.text());
}

function getGmailStorageKey(uid) {
  return `${GMAIL_STORAGE_KEY_PREFIX}${uid}`;
}

function getOfferMetaUrlKey(url) {
  return String(url || '').trim().toLowerCase().replace(/#.*$/, '');
}

function chromeGetAuthToken({ interactive }) {
  return new Promise((resolve, reject) => {
    chrome.identity.getAuthToken({ interactive: !!interactive }, (token) => {
      if (chrome.runtime.lastError) {
        reject(new Error(chrome.runtime.lastError.message || 'getAuthToken a échoué'));
        return;
      }
      if (!token) {
        reject(new Error('Aucun jeton OAuth retourné'));
        return;
      }
      resolve(token);
    });
  });
}

function chromeRemoveCachedAuthToken(token) {
  return new Promise((resolve) => {
    chrome.identity.removeCachedAuthToken({ token }, () => resolve());
  });
}

async function fetchGmailData(token) {
  const res = await fetch(
    'https://www.googleapis.com/gmail/v1/users/me/messages?maxResults=10',
    { headers: { Authorization: `Bearer ${token}` } }
  );
  if (res.status === 401) {
    // Jeton expiré/corrompu côté cache identity.
    await chromeRemoveCachedAuthToken(token);
    throw new Error('Jeton OAuth expiré (401), veuillez réessayer.');
  }
  if (!res.ok) {
    throw new Error(`Gmail API ${res.status}`);
  }
  const data = await res.json();
  return Array.isArray(data.messages) ? data.messages : [];
}

async function connectGmailWithIdentity() {
  // 1) Tentative silencieuse d'abord.
  try {
    const silentToken = await chromeGetAuthToken({ interactive: false });
    const messages = await fetchGmailData(silentToken);
    return { ok: true, token: silentToken, messagesCount: messages.length, interactiveUsed: false };
  } catch (_) {
    // 2) Fallback interactif (popup Google).
  }
  const interactiveToken = await chromeGetAuthToken({ interactive: true });
  const messages = await fetchGmailData(interactiveToken);
  return { ok: true, token: interactiveToken, messagesCount: messages.length, interactiveUsed: true };
}
function getOutlookStorageKey(uid) {
  return `${OUTLOOK_LINK_STATE_KEY_PREFIX}${uid}`;
}

async function setOutlookLocalState(uid, state) {
  if (!uid) return;
  const key = getOutlookStorageKey(uid);
  await chrome.storage.local.set({
    [key]: {
      connected: !!(state && state.connected),
      outlook_email: String((state && state.outlook_email) || ''),
      updated_at: Date.now()
    }
  });
}

async function getOutlookLocalState(uid) {
  if (!uid) return { connected: false, outlook_email: '' };
  const key = getOutlookStorageKey(uid);
  const local = (await chrome.storage.local.get(key))[key] || null;
  if (!local) return { connected: false, outlook_email: '' };
  return {
    connected: !!local.connected,
    outlook_email: String(local.outlook_email || '')
  };
}

async function saveGmailIntegrationToFirestore(uid, idToken, data) {
  const base = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;
  const docPath = `profiles/${uid}/integrations/gmail`;
  const body = {
    fields: {
      provider: { stringValue: 'gmail' },
      status: { stringValue: data.status || 'connected' },
      gmail_email: { stringValue: String(data.gmail_email || '') },
      scope: { stringValue: GMAIL_REQUIRED_SCOPE },
      linked_at: { timestampValue: new Date().toISOString() },
      updated_at: { timestampValue: new Date().toISOString() }
    }
  };
  const res = await fetch(`${base}/${docPath}`, {
    method: 'PATCH',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${idToken}`
    },
    body: JSON.stringify(body)
  });
  if (!res.ok) throw new Error('Erreur sauvegarde intégration Gmail');
}

async function saveOutlookIntegrationToFirestore(uid, idToken, data) {
  const base = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;
  const docPaths = [
    `profiles/${uid}/career_connections/outlook`,
    `profiles/${uid}/integrations/outlook`,
    `profiles/${uid}/mail_connections/outlook`
  ];
  const body = {
    fields: {
      bankName: { stringValue: 'Outlook' },
      bankId: { stringValue: 'outlook' },
      provider: { stringValue: 'outlook' },
      status: { stringValue: data.status || 'connected' },
      outlook_email: { stringValue: String(data.outlook_email || '') },
      email: { stringValue: String(data.outlook_email || '') },
      timestamp: { timestampValue: new Date().toISOString() },
      linked_at: { timestampValue: new Date().toISOString() },
      updated_at: { timestampValue: new Date().toISOString() }
    }
  };
  let lastStatus = 0;
  let lastText = '';
  for (const docPath of docPaths) {
    const res = await fetch(`${base}/${docPath}`, {
      method: 'PATCH',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${idToken}`
      },
      body: JSON.stringify(body)
    });
    if (res.ok) return true;
    lastStatus = res.status || 0;
    lastText = await res.text().catch(() => '');
  }
  if (lastStatus === 401) {
    throw new Error('Session expirée. Déconnectez/reconnectez Taleos puis réessayez.');
  }
  if (lastStatus === 403) {
    throw new Error('Permissions Firestore insuffisantes pour enregistrer Outlook.');
  }
  throw new Error(`Erreur sauvegarde intégration Outlook (${lastStatus || 'inconnue'})${lastText ? `: ${lastText}` : ''}`);
}

async function getOutlookIntegrationState(uid, idToken) {
  const base = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;
  const paths = [`profiles/${uid}/career_connections/outlook`, `profiles/${uid}/mail_connections/outlook`, `profiles/${uid}/integrations/outlook`];
  for (const docPath of paths) {
    const res = await fetch(`${base}/${docPath}`, { headers: { Authorization: `Bearer ${idToken}` } });
    if (!res.ok) continue;
    const data = parseFirestoreDoc(await res.json());
    return {
      connected: (data.status || '') === 'connected',
      outlook_email: data.outlook_email || data.email || ''
    };
  }
  return getOutlookLocalState(uid);
}

function b64UrlEncode(bytes) {
  return btoa(String.fromCharCode(...new Uint8Array(bytes)))
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/g, '');
}

async function buildPkce() {
  const verifierArr = crypto.getRandomValues(new Uint8Array(32));
  const verifier = b64UrlEncode(verifierArr);
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(verifier));
  const challenge = b64UrlEncode(new Uint8Array(digest));
  return { verifier, challenge };
}

async function exchangeOutlookCodeWithBackend(code, verifier, redirectUri) {
  const { taleosIdToken } = await chrome.storage.local.get(['taleosIdToken']);
  if (!taleosIdToken) throw new Error('Session Taleos manquante');
  const res = await fetch(OUTLOOK_EXCHANGE_CF_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${taleosIdToken}`
    },
    body: JSON.stringify({ code, codeVerifier: verifier, redirectUri })
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok || json.ok !== true) throw new Error(json.error || 'Échec exchange Outlook OAuth');
  return true;
}

async function getOutlookOAuthClientId() {
  let res;
  try {
    res = await fetch(OUTLOOK_CONFIG_CF_URL, { method: 'GET' });
  } catch (e) {
    throw new Error(
      `Impossible de joindre outlookOAuthConfig (réseau ou extension). Vérifiez la connexion et que l’URL est autorisée dans le manifest. Détails : ${e?.message || e}`
    );
  }
  const text = await res.text();
  let json = {};
  try {
    json = text ? JSON.parse(text) : {};
  } catch (_) {
    json = {};
  }
  if (!res.ok || json.ok !== true || !json.clientId) {
    if (res.status === 404) {
      throw new Error(
        'Outlook OAuth : la Cloud Function outlookOAuthConfig est introuvable (HTTP 404). ' +
        'Déployez les fonctions sur Firebase : firebase deploy --only functions --project project-taleos ' +
        '(ou lancez le workflow GitHub « Deploy Firebase Functions »). ' +
        'Sans déploiement, l’URL europe-west1-project-taleos.cloudfunctions.net/outlookOAuthConfig ne répond pas.'
      );
    }
    const looksHtml = /<html[\s>]/i.test(text || '') || /<title>.*404/i.test(text || '');
    const serverMsg = json.error
      || (looksHtml ? 'réponse HTML inattendue (serveur)' : (text || '').slice(0, 180).trim())
      || 'réponse invalide';
    if (res.status === 500 && /OUTLOOK_CLIENT_ID/i.test(serverMsg)) {
      throw new Error(
        `${serverMsg} — À faire côté prod : Firebase Console → Functions → outlookOAuthConfig / variables d’environnement, définir OUTLOOK_CLIENT_ID (ID d’application Azure AD), puis redéployer les fonctions.`
      );
    }
    throw new Error(
      serverMsg && serverMsg !== 'réponse invalide'
        ? `Configuration Outlook OAuth : ${serverMsg} (HTTP ${res.status})`
        : `Configuration Outlook OAuth indisponible (HTTP ${res.status || '?'})`
    );
  }
  return String(json.clientId);
}

async function getGmailAuthState(uid, idToken) {
  const key = getGmailStorageKey(uid);
  const local = (await chrome.storage.local.get(key))[key] || null;
  let firestoreState = null;
  if (idToken) {
    const base = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;
    const docPath = `profiles/${uid}/integrations/gmail`;
    const res = await fetch(`${base}/${docPath}`, { headers: { Authorization: `Bearer ${idToken}` } });
    if (res.ok) {
      const data = parseFirestoreDoc(await res.json());
      firestoreState = {
        status: data.status || 'connected',
        gmail_email: data.gmail_email || ''
      };
    }
  }
  const now = Date.now();
  const tokenValid = !!(local && local.access_token && local.expires_at && local.expires_at > now + 60 * 1000);
  return {
    connected: !!((firestoreState && firestoreState.status === 'connected') || tokenValid),
    tokenValid,
    gmail_email: (local && local.gmail_email) || (firestoreState && firestoreState.gmail_email) || '',
    expires_at: local && local.expires_at ? local.expires_at : null
  };
}

async function runTestConnection(msg) {
  const { bankId, email, password, firebaseUserId, taleosTabId, bankName } = msg;

  // BPCE utilise l'OTP Oracle — pas de mot de passe ni d'URL de connexion traditionnelle
  if (bankId === 'bpce') {
    if (!email || !firebaseUserId) {
      return { success: false, message: 'Email BPCE manquant.' };
    }
    const { taleosIdToken } = await chrome.storage.local.get(['taleosIdToken']);
    if (!taleosIdToken) {
      return { success: false, message: 'Vous devez être connecté à Taleos' };
    }
    await saveCareerConnectionToFirestore(firebaseUserId, taleosIdToken, bankId, bankName || 'BPCE', email, '');
    return { success: true };
  }

  // J.P. Morgan utilise un code OTP envoyé par email pendant la candidature.
  if (bankId === 'jp_morgan') {
    if (!email || !firebaseUserId) {
      return { success: false, message: 'Email J.P. Morgan manquant.' };
    }
    const { taleosIdToken } = await chrome.storage.local.get(['taleosIdToken']);
    if (!taleosIdToken) {
      return { success: false, message: 'Vous devez être connecté à Taleos' };
    }
    await saveCareerConnectionToFirestore(firebaseUserId, taleosIdToken, bankId, bankName || 'J.P. Morgan', email, '');
    return { success: true, message: 'Email J.P. Morgan enregistré. Le code OTP sera demandé pendant la candidature.' };
  }

  const loginUrl = CONNECTION_TEST_URLS[bankId] || String(msg.loginUrl || '').trim();
  if (!loginUrl || !email || !password || !firebaseUserId) {
    const missing = [];
    if (!loginUrl) missing.push('URL de connexion');
    if (!email) missing.push('email');
    if (!password) missing.push('mot de passe');
    if (!firebaseUserId) missing.push('session Taleos');
    return { success: false, message: `Paramètres manquants: ${missing.join(', ')}` };
  }

  const { taleosIdToken } = await chrome.storage.local.get(['taleosIdToken']);
  if (!taleosIdToken) {
    return { success: false, message: 'Vous devez être connecté à Taleos' };
  }

  const tab = await chrome.tabs.create({ url: loginUrl, active: false });
  const tabId = tab.id;

  async function restoreTaleosTabIfNeeded() {
    if (!taleosTabId) return;
    try {
      await chrome.tabs.update(taleosTabId, { active: true });
    } catch (_) {}
  }

  await chrome.storage.local.set({
    taleos_connection_test: { bankId, tabId, firebaseUserId, taleosTabId, bankName, timestamp: Date.now() }
  });

  const params = { bankId, email, password }
  const runFill = (phase) => chrome.scripting.executeScript({
    target: { tabId },
    func: (p, ph) => {
      window.__taleosConnectionTestParams = { ...p, phase: ph };
      if (typeof window.__taleosConnectionTestFill === 'function') {
        return window.__taleosConnectionTestFill();
      }
      return { done: false, error: 'Script non chargé' };
    },
    args: [params, phase || 0]
  });

  const runCheck = () => chrome.scripting.executeScript({
    target: { tabId },
    func: (bkId) => {
      window.__taleosConnectionTestParams = { bankId: bkId };
      if (typeof window.__taleosConnectionTestCheck === 'function') {
        return window.__taleosConnectionTestCheck();
      }
      return null;
    },
    args: [bankId]
  });

  const waitForLoad = () => new Promise((resolve) => {
    const listener = (id, info) => {
      if (id === tabId && info.status === 'complete') {
        chrome.tabs.onUpdated.removeListener(listener);
        resolve();
      }
    };
    chrome.tabs.onUpdated.addListener(listener);
    setTimeout(() => {
      chrome.tabs.onUpdated.removeListener(listener);
      resolve();
    }, 15000);
  });

  try {
    await waitForLoad();
    await new Promise(r => setTimeout(r, 1500));

    await chrome.scripting.executeScript({
      target: { tabId },
      files: ['scripts/connection-test-runner.js']
    });

    if (bankId === 'deloitte') {
      const r1 = await runFill(1);
      if (r1?.[0]?.result?.needPhase2) {
        await new Promise(r => setTimeout(r, 2500));
      }
    }

    if (bankId === 'axa') {
      const r1 = await runFill(1);
      if (r1?.[0]?.result?.needPhase2) {
        await new Promise(r => setTimeout(r, 3000));
        try {
          await chrome.scripting.executeScript({
            target: { tabId },
            files: ['scripts/connection-test-runner.js']
          });
        } catch (_) {}
      }
      const r2 = await runFill(2);
      if (r2?.[0]?.result?.error && !r2?.[0]?.result?.submitted) {
        await chrome.tabs.remove(tabId).catch(() => {});
        chrome.storage.local.remove('taleos_connection_test');
        await restoreTaleosTabIfNeeded();
        return { success: false, message: r2[0].result.error };
      }
      await new Promise(r => setTimeout(r, 7000));
    }

    const fillRes = bankId === 'axa' ? null : await runFill(bankId === 'deloitte' ? 2 : 0);
    if (fillRes?.[0]?.result?.error && !fillRes[0].result?.submitted) {
      await chrome.tabs.remove(tabId).catch(() => {});
      chrome.storage.local.remove('taleos_connection_test');
      await restoreTaleosTabIfNeeded();
      return { success: false, message: fillRes[0].result.error };
    }

    if (bankId !== 'axa') {
      await new Promise(r => setTimeout(r, 8000));
    }

    try {
      await chrome.scripting.executeScript({
        target: { tabId },
        files: ['scripts/connection-test-runner.js']
      });
    } catch (_) {}

    const checkRes = await runCheck();
    const result = checkRes?.[0]?.result?.success !== undefined ? checkRes[0].result : null;

    await chrome.tabs.remove(tabId).catch(() => {});
    chrome.storage.local.remove('taleos_connection_test');
    await restoreTaleosTabIfNeeded();

    if (result && result.success) {
      const encryptedPassword = btoa(password);
      await saveCareerConnectionToFirestore(
        firebaseUserId,
        taleosIdToken,
        bankId,
        bankName || bankId,
        email,
        encryptedPassword
      );
      return { success: true, message: result.message || 'Connexion réussie' };
    }

    return {
      success: false,
      message: (result && result.message) || 'Échec de connexion (état inconnu).'
    };
  } catch (e) {
    await chrome.tabs.remove(tabId).catch(() => {});
    chrome.storage.local.remove('taleos_connection_test');
    await restoreTaleosTabIfNeeded();
    return { success: false, message: e.message || 'Erreur technique' };
  }
}

/** Routage local selon banque / URL d’offre */
function computeLegacyRouteAs(bankId, offerUrl) {
  const url = String(offerUrl || '').toLowerCase();
  const bid = String(bankId || '').toLowerCase();
  if (bid === 'credit_agricole' || url.includes('groupecreditagricole.jobs')) return 'ca';
  if (bid === 'credit_mutuel' || url.includes('recrutement.creditmutuel.fr')) return 'credit_mutuel';
  if (bid === 'bpifrance' || url.includes('talents.bpifrance.fr') || url.includes('bpi.tzportal.io')) return 'bpifrance';
  if (bid === 'jp_morgan' || bid.includes('jp morgan') || bid.includes('jpmorgan') || url.includes('jpmc.fa.oraclecloud.com')) return 'jp_morgan';
  if (bid === 'axa' || url.includes('careers.axa.com') || url.includes('careers-fr-axa.icims.com') || url.includes('careers-en-axa.icims.com') || url.includes('candidature-recrutement.axa.fr')) return 'axa';
  if (bid === 'deloitte' || (url.includes('myworkdayjobs.com') && url.includes('deloitte'))) return 'deloitte';
  if (bid === 'societe_generale' || url.includes('careers.societegenerale.com') || url.includes('socgen.taleo.net')) return 'sg';
  if (bid === 'bpce' || url.includes('recrutement.bpce.fr') || url.includes('recruitmentplatform.com')) return 'bpce';
  if (bid === 'bnp_paribas' || url.includes('group.bnpparibas') || url.includes('bwelcome.hr.bnpparibas')) return 'bnp';
  return 'other';
}

/** Pilotage local uniquement : pas d’appel Cloud Function pour le plan de candidature. */
function buildLocalPilotExecution(scriptKey, scriptPath) {
  return {
    scriptKey,
    scriptPath,
    planVersion: null,
    tier: 'local_only',
    label: 'Scripts embarqués (extension)',
    detail: '',
    routingSource: 'local',
    automationSource: 'bundled',
    useRemote: false,
    remoteSource: null
  };
}

async function persistLastPilot(exec, meta) {
  const record = {
    tier: exec.tier,
    label: exec.label,
    detail: exec.detail || '',
    routingSource: exec.routingSource,
    automationSource: exec.automationSource,
    scriptKey: meta.scriptKey,
    planVersion: exec.planVersion ?? null,
    routeAs: meta.routeAs,
    bankId: meta.bankId,
    jobId: meta.jobId,
    jobTitle: (meta.jobTitle || '').slice(0, 120),
    at: Date.now(),
    offerUrlPreview: (meta.offerUrl || '').slice(0, 160)
  };
  await chrome.storage.local.set({ taleos_last_pilot: record });
  try {
    await chrome.storage.session.remove('taleos_instruction_plan');
  } catch (_) {}
  try {
    await chrome.storage.session.remove('taleos_remote_automation');
  } catch (_) {}
  console.warn('[Taleos Pilot]', exec.tier, '|', exec.label, exec.detail ? '| ' + exec.detail : '');
}

async function injectAutomationTab(tabId, profile, scriptPath, pilotExec, bankId = '') {
  const target = bankId === 'axa' ? { tabId, allFrames: true } : { tabId };
  if (pilotExec.useRemote && pilotExec.remoteSource) {
    await chrome.scripting.executeScript({ target, files: injectFilesWithBanner(['scripts/remote-loader.js']) });
    await chrome.scripting.executeScript({
      target,
      func: (payload) => {
        if (window.__taleosInjectRemote) window.__taleosInjectRemote(payload.source, payload.data);
      },
      args: [{ source: pilotExec.remoteSource, data: profile }]
    });
    return;
  }
  await chrome.scripting.executeScript({ target, files: injectBankFiles(bankId, [scriptPath]) });
  await chrome.scripting.executeScript({
    target,
    func: (data) => { if (window.__taleosRun) window.__taleosRun(data); },
    args: [profile]
  });
}

async function handleApply(offerUrl, bankId, jobId, jobTitle, companyName, taleosTabId, offerMeta = null) {
  if (bankId && !hasBankAutomation(bankId)) {
    const createOpts = { url: offerUrl, active: false };
    if (taleosTabId) {
      try {
        const taleosTab = await chrome.tabs.get(taleosTabId);
        if (taleosTab?.index != null) createOpts.index = taleosTab.index + 1;
      } catch (_) {}
    }
    await chrome.tabs.create(createOpts);
    if (taleosTabId) {
      chrome.tabs.update(taleosTabId, { active: true }).catch(() => {});
    }
    return { success: true, message: 'Ouverture directe du portail carrière.' };
  }
  let { taleosUserId, taleosIdToken } = await chrome.storage.local.get(['taleosUserId', 'taleosIdToken']);
  if (!taleosUserId && taleosTabId) {
    try {
      await chrome.tabs.sendMessage(taleosTabId, { action: 'taleos_request_auth' });
      await new Promise((resolve) => {
        authSyncResolve = () => { authSyncResolve = null; resolve(); };
        setTimeout(() => { if (authSyncResolve) authSyncResolve(); }, 5000);
      });
      const stored = await chrome.storage.local.get(['taleosUserId', 'taleosIdToken']);
      taleosUserId = stored.taleosUserId;
      taleosIdToken = stored.taleosIdToken;
    } catch (_) {}
  }
  if (!taleosUserId) {
    console.warn('[Taleos] Utilisateur non connecté');
    try {
      await chrome.tabs.sendMessage(taleosTabId, { action: 'taleos_auth_required' });
    } catch (_) {}
    return { error: 'Utilisateur non connecté' };
  }

  const profileCheck = await checkProfileCompletenessFromFirestore(bankId);
  if (!profileCheck?.complete) {
    const missing = profileCheck?.missingFields?.length ? profileCheck.missingFields.join(', ') : 'informations manquantes';
    return { error: `Profil incomplet. Complétez toutes les informations requises dans Mon profil avant de lancer une candidature : ${missing}` };
  }

  let profile;
  try {
    profile = await fetchProfile(taleosUserId, bankId, taleosIdToken);
  } catch (e) {
    console.error('[Taleos] Profil:', e);
    return { error: e.message || 'Profil introuvable' };
  }
  profile.__jobId = jobId;
  profile.__jobTitle = jobTitle || '';
  const normalizedBankId = String(bankId || '').toLowerCase();
  profile.__companyName = companyName || (
    normalizedBankId === 'credit_mutuel' ? 'Crédit Mutuel'
      : normalizedBankId === 'bpifrance' ? 'Bpifrance'
        : 'Crédit Agricole'
  );
  profile.__offerUrl = offerUrl;
  profile.__offerMeta = offerMeta || {};

  // Conserver les métadonnées d'offre pour enrichir l'enregistrement final de candidature
  if (jobId) {
    try {
      const key = String(jobId).trim();
      const urlKey = getOfferMetaUrlKey(offerUrl);
      const { taleos_offer_meta_by_job = {}, taleos_offer_meta_by_url = {} } = await chrome.storage.local.get(['taleos_offer_meta_by_job', 'taleos_offer_meta_by_url']);
      const mergedMeta = {
        ...(offerMeta || {}),
        offerUrl: offerUrl || '',
        companyName: companyName || '',
        updatedAt: Date.now()
      };
      taleos_offer_meta_by_job[key] = {
        ...(taleos_offer_meta_by_job[key] || {}),
        ...mergedMeta
      };
      if (urlKey) {
        taleos_offer_meta_by_url[urlKey] = {
          ...(taleos_offer_meta_by_url[urlKey] || {}),
          ...mergedMeta
        };
      }
      const entries = Object.entries(taleos_offer_meta_by_job).sort((a, b) => (b[1]?.updatedAt || 0) - (a[1]?.updatedAt || 0)).slice(0, 300);
      const urlEntries = Object.entries(taleos_offer_meta_by_url).sort((a, b) => (b[1]?.updatedAt || 0) - (a[1]?.updatedAt || 0)).slice(0, 500);
      await chrome.storage.local.set({
        taleos_offer_meta_by_job: Object.fromEntries(entries),
        taleos_offer_meta_by_url: Object.fromEntries(urlEntries)
      });
    } catch (_) {}
  }

  const routeAs = computeLegacyRouteAs(bankId, offerUrl);
  const normalizedBankKey = normalizeSite(bankId, offerUrl);
  const scriptKey = Object.prototype.hasOwnProperty.call(BANK_SCRIPT_MAP, normalizedBankKey)
    ? normalizedBankKey
    : (Object.prototype.hasOwnProperty.call(BANK_SCRIPT_MAP, bankId) ? bankId : 'credit_agricole');
  const scriptPath = BANK_SCRIPT_MAP[scriptKey] || BANK_SCRIPT_MAP.credit_agricole;
  const pilotExec = buildLocalPilotExecution(scriptKey, scriptPath);
  await persistLastPilot(pilotExec, { bankId, jobId, jobTitle, routeAs, offerUrl, scriptKey });
  chrome.storage.local.set({ taleos_pending_tab: taleosTabId });
  const runMeta = {
    bankId,
    companyName: companyName || '',
    jobId,
    jobTitle,
    offerUrl,
    taleosTabId,
    location: offerMeta?.location || '',
    contractType: offerMeta?.contractType || '',
    experienceLevel: offerMeta?.experienceLevel || '',
    jobFamily: offerMeta?.jobFamily || '',
    publicationDate: offerMeta?.publicationDate || '',
    routeAs,
    routingSource: pilotExec.routingSource,
    automationSource: pilotExec.automationSource
  };

  if (routeAs === 'ca') {
    chrome.storage.local.set({
      taleos_pending_offer: {
        offerUrl,
        bankId,
        profile: { ...profile, __phase: 2, __jobId: jobId, __jobTitle: jobTitle, __companyName: companyName, __offerUrl: offerUrl },
        timestamp: Date.now()
      }
    });
    // Ouvrir la candidature dans un sous-onglet, jamais dans la page Taleos
    const caCreateOpts = { url: CA_CONNEXION_URL, active: false };
    if (taleosTabId) {
      try {
        const taleosTab = await chrome.tabs.get(taleosTabId);
        if (taleosTab?.index != null) caCreateOpts.index = taleosTab.index + 1;
      } catch (_) {}
    }
    const tab = await chrome.tabs.create(caCreateOpts);
    const tabId = tab.id;
    chrome.storage.local.set({ taleos_ca_apply_tab_id: tabId });
    await registerApplyRunForTab(tabId, runMeta);
    scheduleApplyStuckWatchdog();
    chrome.storage.local.remove(['taleos_ca_candidature_reloaded', 'taleos_ca_candidature_pending']);
    if (taleosTabId) {
      chrome.tabs.update(taleosTabId, { active: true }).catch(() => {});
      [100, 300, 600].forEach(ms => setTimeout(() => {
        chrome.tabs.update(taleosTabId, { active: true }).catch(() => {});
      }, ms));
    }

    const injectAndRun = (phase) => {
      const ph = phase ?? 2;
      const p = { ...profile, __phase: ph };
      injectAutomationTab(tabId, p, scriptPath, pilotExec, bankId).catch(e => console.error('[Taleos] Injection:', e));
    };

    const listener = async (id, info) => {
      if (id !== tabId || info.status !== 'complete') return;
      try {
        const t = await chrome.tabs.get(tabId);
        const url = (t?.url || '').toLowerCase();
        if (url.includes('/connexion') || url.includes('/login') || url.includes('/connection')) return;
        if (url.includes('admin-ajax')) return;
        if (url.includes('/candidature-validee')) {
          chrome.tabs.onUpdated.removeListener(listener);
          chrome.storage.local.remove('taleos_pending_offer');
          await new Promise(r => setTimeout(r, 2000));
          injectAndRun(3);
          return;
        }
        if (url.includes('/candidature/') || url.includes('/application/') || url.includes('/apply/')) {
          chrome.storage.local.remove('taleos_pending_offer');
          const { taleos_ca_candidature_reloaded } = await chrome.storage.local.get('taleos_ca_candidature_reloaded');
          if (taleos_ca_candidature_reloaded !== tabId) {
            chrome.storage.local.set({
              taleos_ca_candidature_reloaded: tabId,
              taleos_ca_candidature_pending: { tabId, profile: { ...profile, __phase: 3, __jobId: jobId, __jobTitle: jobTitle, __companyName: companyName, __offerUrl: offerUrl }, timestamp: Date.now() }
            });
            chrome.tabs.reload(tabId);
            return;
          }
          chrome.storage.local.remove(['taleos_ca_candidature_reloaded', 'taleos_ca_candidature_pending']);
          if (caLastInject.get(tabId) && Date.now() - caLastInject.get(tabId) < 8000) return;
          caLastInject.set(tabId, Date.now());
          await new Promise(r => setTimeout(r, 5000));
          injectAndRun(3);
          return;
        }
        if (url.includes('/nos-offres-emploi/') || url.includes('/our-offers/') || url.includes('/our-offres/')) {
          if (caLastInject.get(tabId) && Date.now() - caLastInject.get(tabId) < 5000) return;
          caLastInject.set(tabId, Date.now());
          chrome.storage.local.remove('taleos_pending_offer');
          await new Promise(r => setTimeout(r, 2000));
          injectAndRun(2);
        }
      } catch (_) {}
    };
    chrome.tabs.onUpdated.addListener(listener);
    setTimeout(() => chrome.tabs.onUpdated.removeListener(listener), 120000);
  } else if (routeAs === 'deloitte') {
    chrome.storage.local.set({ taleos_pending_tab: taleosTabId });
    const deloitteCreateOpts = { url: offerUrl, active: false };
    if (taleosTabId) {
      try {
        const taleosTab = await chrome.tabs.get(taleosTabId);
        if (taleosTab?.index != null) deloitteCreateOpts.index = taleosTab.index + 1;
      } catch (_) {}
    }
    const tab = await chrome.tabs.create(deloitteCreateOpts);
    await registerApplyRunForTab(tab.id, runMeta);
    chrome.storage.local.set({
      taleos_pending_deloitte: {
        profile: { ...profile, auth_email: profile.auth_email || profile.email, auth_password: profile.auth_password },
        tabId: tab.id,
        jobId,
        jobTitle,
        companyName,
        offerUrl,
        timestamp: Date.now()
      }
    });
    scheduleApplyStuckWatchdog();
    if (taleosTabId) {
      chrome.tabs.update(taleosTabId, { active: true }).catch(() => {});
    }
  } else if (routeAs === 'sg') {
    chrome.storage.local.set({ taleos_pending_tab: taleosTabId });
    const createOpts = { url: offerUrl, active: false };
    if (taleosTabId) {
      try {
        const taleosTab = await chrome.tabs.get(taleosTabId);
        if (taleosTab?.index != null) createOpts.index = taleosTab.index + 1;
      } catch (_) {}
    }
    const tab = await chrome.tabs.create(createOpts);
    await registerApplyRunForTab(tab.id, runMeta);
    if (taleosTabId) {
      chrome.tabs.update(taleosTabId, { active: true }).catch(() => {});
      [100, 300, 600].forEach(ms => setTimeout(() => {
        chrome.tabs.update(taleosTabId, { active: true }).catch(() => {});
      }, ms));
    }
    chrome.storage.local.remove('taleos_sg_navigate_profile_attempted');
    chrome.storage.local.set({
      taleos_pending_sg: {
        profile: { ...profile, __jobId: jobId, __jobTitle: jobTitle, __companyName: companyName, __offerUrl: offerUrl },
        offerUrl, jobId, jobTitle, companyName,
        timestamp: Date.now()
      },
      taleos_sg_tab_id: tab.id
    });
    scheduleApplyStuckWatchdog();
  } else if (routeAs === 'bpce') {
    chrome.storage.local.set({ taleos_pending_tab: taleosTabId });
    const createOpts = { url: offerUrl, active: false };
    if (taleosTabId) {
      try {
        const taleosTab = await chrome.tabs.get(taleosTabId);
        if (taleosTab?.index != null) createOpts.index = taleosTab.index + 1;
      } catch (_) {}
    }
    const tab = await chrome.tabs.create(createOpts);
    await registerApplyRunForTab(tab.id, runMeta);
    if (taleosTabId) {
      chrome.tabs.update(taleosTabId, { active: true }).catch(() => {});
      [100, 300, 600].forEach(ms => setTimeout(() => {
        chrome.tabs.update(taleosTabId, { active: true }).catch(() => {});
      }, ms));
    }
    chrome.storage.local.set({
      taleos_pending_bpce: {
        profile: { ...profile, __jobId: jobId, __jobTitle: jobTitle, __companyName: companyName || 'BPCE', __offerUrl: offerUrl },
        offerUrl, jobId, jobTitle, companyName: companyName || 'BPCE',
        tabId: tab.id,
        timestamp: Date.now()
      },
      taleos_bpce_tab_id: tab.id
    });
    scheduleApplyStuckWatchdog();
  } else if (routeAs === 'axa') {
    chrome.storage.local.set({ taleos_pending_tab: taleosTabId });
    const applyUrl = await resolveAxaApplyUrl(offerUrl, companyName, jobTitle, offerMeta);
    const createOpts = { url: applyUrl, active: false };
    if (taleosTabId) {
      try {
        const taleosTab = await chrome.tabs.get(taleosTabId);
        if (taleosTab?.index != null) createOpts.index = taleosTab.index + 1;
      } catch (_) {}
    }
    const tab = await chrome.tabs.create(createOpts);
    await registerApplyRunForTab(tab.id, runMeta);
    if (taleosTabId) {
      chrome.tabs.update(taleosTabId, { active: true }).catch(() => {});
      [100, 300, 600].forEach((ms) => setTimeout(() => {
        chrome.tabs.update(taleosTabId, { active: true }).catch(() => {});
      }, ms));
    }
    await chrome.storage.local.set({
      taleos_pending_axa: {
        profile: { ...profile, __jobId: jobId, __jobTitle: jobTitle, __companyName: companyName || 'AXA', __offerUrl: offerUrl },
        offerUrl,
        applyUrl,
        jobId,
        jobTitle,
        companyName: companyName || 'AXA',
        tabId: tab.id,
        timestamp: Date.now()
      },
      taleos_axa_tab_id: tab.id
    });
    await scheduleApplyStuckWatchdog();
  } else if (routeAs === 'bnp') {
    chrome.storage.local.set({ taleos_pending_tab: taleosTabId });
    const createOpts = { url: offerUrl, active: false };
    if (taleosTabId) {
      try {
        const taleosTab = await chrome.tabs.get(taleosTabId);
        if (taleosTab?.index != null) createOpts.index = taleosTab.index + 1;
      } catch (_) {}
    }
    const tab = await chrome.tabs.create(createOpts);
    await registerApplyRunForTab(tab.id, runMeta);
    if (taleosTabId) {
      chrome.tabs.update(taleosTabId, { active: true }).catch(() => {});
      [100, 300, 600].forEach(ms => setTimeout(() => {
        chrome.tabs.update(taleosTabId, { active: true }).catch(() => {});
      }, ms));
    }
    chrome.storage.local.set({
      taleos_pending_bnp: {
        profile: { ...profile, __jobId: jobId, __jobTitle: jobTitle, __companyName: companyName || 'BNP Paribas', __offerUrl: offerUrl },
        offerUrl,
        jobId,
        jobTitle,
        companyName: companyName || 'BNP Paribas',
        tabId: tab.id,
        timestamp: Date.now()
      },
      taleos_bnp_tab_id: tab.id
    });
    scheduleApplyStuckWatchdog();
  } else if (routeAs === 'credit_mutuel') {
    await chrome.storage.local.set({ taleos_pending_tab: taleosTabId });
    const createOpts = { url: offerUrl, active: false };
    if (taleosTabId) {
      try {
        const taleosTab = await chrome.tabs.get(taleosTabId);
        if (taleosTab?.index != null) createOpts.index = taleosTab.index + 1;
      } catch (_) {}
    }
    const tab = await chrome.tabs.create(createOpts);
    await registerApplyRunForTab(tab.id, runMeta);
    if (taleosTabId) {
      chrome.tabs.update(taleosTabId, { active: true }).catch(() => {});
      [100, 300, 600].forEach(ms => setTimeout(() => {
        chrome.tabs.update(taleosTabId, { active: true }).catch(() => {});
      }, ms));
    }
    await chrome.storage.local.set({
      taleos_pending_credit_mutuel: {
        profile: { ...profile, __jobId: jobId, __jobTitle: jobTitle, __companyName: companyName || 'Crédit Mutuel', __offerUrl: offerUrl },
        offerUrl,
        jobId,
        jobTitle,
        companyName: companyName || 'Crédit Mutuel',
        tabId: tab.id,
        timestamp: Date.now()
      },
      taleos_credit_mutuel_tab_id: tab.id
    });
    await scheduleApplyStuckWatchdog();
  } else if (routeAs === 'bpifrance') {
    await chrome.storage.local.set({ taleos_pending_tab: taleosTabId });
    const createOpts = { url: offerUrl, active: false };
    if (taleosTabId) {
      try {
        const taleosTab = await chrome.tabs.get(taleosTabId);
        if (taleosTab?.index != null) createOpts.index = taleosTab.index + 1;
      } catch (_) {}
    }
    const tab = await chrome.tabs.create(createOpts);
    await registerApplyRunForTab(tab.id, runMeta);
    if (taleosTabId) {
      chrome.tabs.update(taleosTabId, { active: true }).catch(() => {});
      [100, 300, 600].forEach((ms) => setTimeout(() => {
        chrome.tabs.update(taleosTabId, { active: true }).catch(() => {});
      }, ms));
    }

    const injectBpifrance = async () => {
      try {
        const openedTab = await chrome.tabs.get(tab.id).catch(() => null);
        const currentUrl = String(openedTab?.url || '').toLowerCase();
        if (!currentUrl.includes('talents.bpifrance.fr') && !currentUrl.includes('bpi.tzportal.io')) return;
        const lastInject = bpifranceLastInject.get(tab.id) || 0;
        if (Date.now() - lastInject < 2500) return;
        bpifranceLastInject.set(tab.id, Date.now());
        await injectAutomationTab(tab.id, profile, scriptPath, pilotExec, bankId);
      } catch (e) {
        console.error('[Taleos] Injection Bpifrance:', e);
      }
    };

    const listener = async (id, info) => {
      if (id !== tab.id || info.status !== 'complete') return;
      await new Promise((resolve) => setTimeout(resolve, 1200));
      await injectBpifrance();
    };
    chrome.tabs.onUpdated.addListener(listener);
    setTimeout(() => chrome.tabs.onUpdated.removeListener(listener), 180000);
    setTimeout(() => { injectBpifrance().catch(() => {}); }, 2500);
    await scheduleApplyStuckWatchdog();
  } else if (routeAs === 'goldman_sachs') {
    await chrome.storage.local.set({ taleos_pending_tab: taleosTabId });
    const createOpts = { url: offerUrl, active: false };
    if (taleosTabId) {
      try {
        const taleosTab = await chrome.tabs.get(taleosTabId);
        if (taleosTab?.index != null) createOpts.index = taleosTab.index + 1;
      } catch (_) {}
    }
    const tab = await chrome.tabs.create(createOpts);
    await registerApplyRunForTab(tab.id, runMeta);
    if (taleosTabId) {
      chrome.tabs.update(taleosTabId, { active: true }).catch(() => {});
      [100, 300, 600].forEach((ms) => setTimeout(() => {
        chrome.tabs.update(taleosTabId, { active: true }).catch(() => {});
      }, ms));
    }
    await chrome.storage.local.set({
      taleos_pending_goldman_sachs: {
        profile: { ...profile, __jobId: jobId, __jobTitle: jobTitle, __companyName: 'Goldman Sachs', __offerUrl: offerUrl },
        offerUrl,
        jobId,
        jobTitle,
        companyName: 'Goldman Sachs',
        location: offerMeta?.location || '',
        contractType: offerMeta?.contractType || '',
        tabId: tab.id,
        timestamp: Date.now()
      },
      taleos_gs_tab_id: tab.id
    });
    await scheduleApplyStuckWatchdog();
  } else if (routeAs === 'jp_morgan') {
    await chrome.storage.local.set({ taleos_pending_tab: taleosTabId });
    const createOpts = { url: offerUrl, active: false };
    if (taleosTabId) {
      try {
        const taleosTab = await chrome.tabs.get(taleosTabId);
        if (taleosTab?.index != null) createOpts.index = taleosTab.index + 1;
      } catch (_) {}
    }
    const tab = await chrome.tabs.create(createOpts);
    await registerApplyRunForTab(tab.id, runMeta);
    if (taleosTabId) {
      chrome.tabs.update(taleosTabId, { active: true }).catch(() => {});
      [100, 300, 600].forEach((ms) => setTimeout(() => {
        chrome.tabs.update(taleosTabId, { active: true }).catch(() => {});
      }, ms));
    }
    await chrome.storage.local.set({
      taleos_pending_jp_morgan: {
        profile: { ...profile, __jobId: jobId, __jobTitle: jobTitle, __companyName: companyName || 'J.P. Morgan', __offerUrl: offerUrl },
        offerUrl,
        jobId,
        jobTitle,
        companyName: companyName || 'J.P. Morgan',
        location: offerMeta?.location || '',
        contractType: offerMeta?.contractType || '',
        tabId: tab.id,
        timestamp: Date.now()
      },
      taleos_jp_morgan_tab_id: tab.id
    });
    await scheduleApplyStuckWatchdog();
  } else {
    // Ouvrir la candidature dans un sous-onglet, jamais dans la page Taleos
    const otherCreateOpts = { url: offerUrl, active: false };
    if (taleosTabId) {
      try {
        const taleosTab = await chrome.tabs.get(taleosTabId);
        if (taleosTab?.index != null) otherCreateOpts.index = taleosTab.index + 1;
      } catch (_) {}
    }
    const tab = await chrome.tabs.create(otherCreateOpts);
    const tabId = tab.id;
    await registerApplyRunForTab(tabId, runMeta);
    if (taleosTabId) {
      chrome.tabs.update(taleosTabId, { active: true }).catch(() => {});
    }
    const listener = async (id, info) => {
      if (id !== tabId || info.status !== 'complete') return;
      chrome.tabs.onUpdated.removeListener(listener);
      await new Promise(r => setTimeout(r, 1500));
      try {
        await injectAutomationTab(tabId, profile, scriptPath, pilotExec, bankId);
      } catch (e) {
        console.error('[Taleos] Injection:', e);
      }
    };
    chrome.tabs.onUpdated.addListener(listener);
  }
  return {
    ok: true,
    pilotTier: pilotExec.tier,
    pilotLabel: pilotExec.label,
    routingSource: pilotExec.routingSource,
    automationSource: pilotExec.automationSource
  };
}

async function saveCandidatureAndNotifyTaleos(msg, tabIdToClose) {
  const { jobId, jobTitle, companyName, offerUrl } = msg;
  try {
    const { taleosUserId, taleosIdToken, taleos_pending_tab, taleos_offer_meta_by_job = {}, taleos_offer_meta_by_url = {} } = await chrome.storage.local.get(['taleosUserId', 'taleosIdToken', 'taleos_pending_tab', 'taleos_offer_meta_by_job', 'taleos_offer_meta_by_url']);
    chrome.storage.local.remove('taleos_pending_tab');
    if (!taleosUserId || !taleosIdToken) return;

    const safe = (s) => (s || '').trim().replace(/[/\\.]/g, '_').replace(/\s+/g, '_').slice(0, 150) || 'inconnu';
    const now = new Date();
    const dd = String(now.getDate()).padStart(2, '0');
    const mm = String(now.getMonth() + 1).padStart(2, '0');
    const yyyy = now.getFullYear();
    const datePart = dd + '\uFF0F' + mm + '\uFF0F' + yyyy;
    const docId = (datePart + ' \u203A ' + safe(companyName) + ' \u203A ' + safe(jobTitle) + ' \u203A ' + (jobId || 'unknown')).slice(0, 1500);

    const metaFromStore = taleos_offer_meta_by_job[String(jobId || '').trim()] || {};
    const metaFromUrl = taleos_offer_meta_by_url[getOfferMetaUrlKey(offerUrl)] || {};
    const mergedMeta = { ...metaFromUrl, ...metaFromStore };
    const location = (msg.location || mergedMeta.location || '').trim();
    const contractType = (msg.contractType || mergedMeta.contractType || '').trim();
    const experienceLevel = (msg.experienceLevel || mergedMeta.experienceLevel || '').trim();
    const jobFamily = (msg.jobFamily || mergedMeta.jobFamily || '').trim();
    const publicationDate = (msg.publicationDate || mergedMeta.publicationDate || '').trim();
    const extensionVersion = String(msg.extensionVersion || '').trim();
    const extensionVersionName = String(msg.extensionVersionName || '').trim();
    const applyRunId = String(msg.applyRunId || '').trim();
    const status = String(msg.status || 'envoyée').trim() || 'envoyée';
    const successType = String(msg.successType || '').trim();
    const successMessage = String(msg.successMessage || msg.message || '').trim();

    const doc = {
      jobId: String(jobId || '').trim(),
      jobTitle: (jobTitle || '').trim(),
      jobUrl: offerUrl || '',
      companyName: companyName || 'Non spécifié',
      location: location || 'Non spécifié',
      contractType: contractType || 'Non spécifié',
      experienceLevel: experienceLevel || 'Non spécifié',
      jobFamily: jobFamily || 'Non spécifié',
      publicationDate: publicationDate || 'Non spécifié',
      appliedDate: { seconds: Math.floor(Date.now() / 1000), nanoseconds: 0 },
      status,
      extensionVersion: extensionVersion || 'unknown',
      extensionVersionName: extensionVersionName || '',
      applyRunId: applyRunId || '',
      extensionSuccessType: successType || '',
      extensionSuccessMessage: successMessage || ''
    };

    const base = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;
    const docPath = `profiles/${taleosUserId}/job_applications/${encodeURIComponent(docId)}`;
    const fields = {};
    for (const [k, v] of Object.entries(doc)) {
      if (typeof v === 'string') fields[k] = { stringValue: v };
      else if (typeof v === 'number') fields[k] = { integerValue: String(v) };
      else if (v && typeof v === 'object' && 'seconds' in v) fields[k] = { timestampValue: new Date(v.seconds * 1000).toISOString() };
    }
    const res = await fetch(`${base}/${docPath}`, {
      method: 'PATCH',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${taleosIdToken}`
      },
      body: JSON.stringify({ fields })
    });
    if (!res.ok) console.error('[Taleos] Firestore save:', await res.text());

    let taleosTab = taleos_pending_tab;
    if (!taleosTab) {
      const taleosTabs = await chrome.tabs.query({ url: '*://*.taleos.co/*' });
      taleosTab = taleosTabs[0]?.id;
    }
    if (!taleosTab) {
      const ghTabs = await chrome.tabs.query({ url: '*://*.github.io/*' });
      taleosTab = ghTabs[0]?.id;
    }
    if (taleosTab) {
      try {
        await chrome.tabs.sendMessage(taleosTab, {
          action: 'taleos_candidature_success',
          jobId,
          status,
          successType,
          successMessage
        });
        chrome.tabs.update(taleosTab, { active: true }).catch(() => {});
      } catch (_) {}
    }
  } finally {
    if (tabIdToClose) {
      setTimeout(() => {
        chrome.tabs.remove(tabIdToClose).catch(() => {});
      }, 3000);
    }
  }
}

async function notifyTaleosCandidatureFailure(msg) {
  const { jobId, error } = msg;
  const { taleos_pending_tab } = await chrome.storage.local.get(['taleos_pending_tab']);
  let taleosTab = taleos_pending_tab;
  if (!taleosTab) {
    const taleosTabs = await chrome.tabs.query({ url: '*://*.taleos.co/*' });
    taleosTab = taleosTabs[0]?.id;
  }
  if (!taleosTab) {
    const ghTabs = await chrome.tabs.query({ url: '*://*.github.io/*' });
    taleosTab = ghTabs[0]?.id;
  }
  if (taleosTab) {
    try {
      await chrome.tabs.sendMessage(taleosTab, { action: 'taleos_candidature_failure', jobId, error: error || 'Erreur' });
    } catch (_) {}
  }
}

async function notifyTaleosOfferUnavailable(msg) {
  const { jobId, jobTitle } = msg;
  const { taleos_pending_tab } = await chrome.storage.local.get(['taleos_pending_tab']);
  let taleosTab = taleos_pending_tab;
  if (!taleosTab) {
    const taleosTabs = await chrome.tabs.query({ url: '*://*.taleos.co/*' });
    taleosTab = taleosTabs[0]?.id;
  }
  if (!taleosTab) {
    const ghTabs = await chrome.tabs.query({ url: '*://*.github.io/*' });
    taleosTab = ghTabs[0]?.id;
  }
  if (taleosTab) {
    try {
      await chrome.tabs.sendMessage(taleosTab, { action: 'taleos_offer_unavailable', jobId, jobTitle: jobTitle || '' });
    } catch (_) {}
  }
}

async function upsertGlobalExpiredJobSignal(msg) {
  const jobId = String(msg?.jobId || '').trim();
  if (!jobId) return;
  const { taleosIdToken, taleosUserId } = await chrome.storage.local.get(['taleosIdToken', 'taleosUserId']);
  if (!taleosIdToken) return;
  const nowIso = new Date().toISOString();
  const doc = {
    jobId,
    jobTitle: String(msg?.jobTitle || '').trim(),
    offerUrl: String(msg?.offerUrl || '').trim(),
    source: String(msg?.source || '').trim(),
    status: 'expired',
    detectedBy: String(taleosUserId || 'unknown'),
    lastDetectedAt: nowIso
  };
  const fields = {};
  for (const [k, v] of Object.entries(doc)) fields[k] = { stringValue: String(v || '') };
  const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/expired_jobs/${encodeURIComponent(jobId)}`;
  await fetch(url, {
    method: 'PATCH',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${taleosIdToken}`
    },
    body: JSON.stringify({ fields })
  }).catch(() => {});
}

async function testCredentials(bankId) {
  const { taleosUserId, taleosIdToken } = await chrome.storage.local.get(['taleosUserId', 'taleosIdToken']);
  if (!taleosUserId || !taleosIdToken) throw new Error('Non connecté. Connectez-vous d\'abord.');
  const profile = await fetchProfile(taleosUserId, bankId, taleosIdToken);
  return { ok: true, email: profile.auth_email || '(vide)' };
}

const PROFILE_FIELD_LABELS = {
  civility: 'Civilité',
  firstName: 'Prénom',
  lastName: 'Nom',
  phoneCountryCode: 'Indicatif pays',
  phone: 'Téléphone',
  address: 'Adresse',
  postalCode: 'Code postal',
  city: 'Ville',
  country: 'Pays',
  jobs: 'Métiers qui m\'intéressent',
  contractType: 'Type de contrat',
  availableFrom: 'Disponible à partir de',
  salaryExpectations: 'Prétentions salariales',
  continents: 'Continents',
  preferredCountries: 'Pays préférés',
  experienceLevel: 'Niveau d\'expérience',
  educationLevel: 'Niveau d\'études',
  institutionType: 'Type d\'établissement',
  diplomaStatus: 'Statut du diplôme',
  deloitteWorked: 'Avez-vous déjà travaillé pour Deloitte ?',
  sg_eu_work_authorization: 'Autorisation de travail dans l’UE',
  sg_notice_period: 'Préavis de départ',
  cv: 'CV (Documents)',
  bpcePreferences: 'Préférences BPCE'
};

const EU_WORK_AUTH_LABEL = 'Union européenne';
const EU_WORK_AUTH_FRENCH_COUNTRIES = new Set([
  'Allemagne', 'Autriche', 'Belgique', 'Bulgarie', 'Chypre', 'Croatie', 'Danemark',
  'Espagne', 'Estonie', 'Finlande', 'France', 'Grèce', 'Hongrie', 'Irlande', 'Italie',
  'Lettonie', 'Lituanie', 'Luxembourg', 'Malte', 'Pays-Bas', 'Pologne', 'Portugal',
  'République tchèque', 'Roumanie', 'Slovaquie', 'Slovénie', 'Suède'
]);

function deriveEuWorkAuthorizationFromProfile(profile) {
  const direct = String(profile?.sg_eu_work_authorization || '').trim().toLowerCase();
  if (direct === 'yes' || direct === 'no') return direct;
  const rows = Array.isArray(profile?.jp_morgan_work_authorizations) ? profile.jp_morgan_work_authorizations : [];
  const normalizedRows = rows.filter(Boolean);
  const euRow = normalizedRows.find((row) => String(row?.country || '').trim() === EU_WORK_AUTH_LABEL);
  if (euRow) {
    const value = String(euRow.work_authorized || '').trim();
    if (value === 'Yes') return 'yes';
    if (value === 'No') return 'no';
  }
  const legacyFranceRow = normalizedRows.find((row) => EU_WORK_AUTH_FRENCH_COUNTRIES.has(String(row?.country || '').trim()));
  if (legacyFranceRow) {
    const value = String(legacyFranceRow.work_authorized || '').trim();
    if (value === 'Yes') return 'yes';
    if (value === 'No') return 'no';
  }
  return '';
}

/** Vérifie si le profil utilisateur est complet (même logique que offres.html) */
async function checkProfileCompletenessFromFirestore(bankId) {
  const { taleosUserId, taleosIdToken } = await chrome.storage.local.get(['taleosUserId', 'taleosIdToken']);
  if (!taleosUserId || !taleosIdToken) return { complete: false, missingFields: ['Connexion'] };
  const base = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;
  const profileRes = await fetch(`${base}/profiles/${taleosUserId}`, { headers: { Authorization: `Bearer ${taleosIdToken}` } });
  if (!profileRes.ok) return { complete: false, missingFields: ['Profil'] };
  const profile = parseFirestoreDoc(await profileRes.json());
  const sgEuWorkAuthorization = deriveEuWorkAuthorizationFromProfile(profile);
  const isBpce = bankId === 'bpce' || (typeof bankId === 'string' && bankId.toLowerCase().includes('bpce'));
  const bpceHasContent = !!((profile.bpce_handicap || '').trim() || (profile.bpce_vivier_natixis || '').trim() || (profile.bpce_application_source || '').trim() || (profile.linkedin_url || '').trim() || profile.bpce_job_alerts);
  const required = {
    civility: profile.civility,
    firstName: profile.first_name,
    lastName: profile.last_name,
    phoneCountryCode: profile.phone_country_code,
    phone: profile.phone,
    address: profile.address,
    postalCode: profile.postal_code,
    city: profile.city,
    country: profile.country,
    jobs: profile.jobs && Array.isArray(profile.jobs) && profile.jobs.length > 0,
    contractType: profile.contract_type,
    availableFrom: profile.available_from || profile.available_from_raw,
    continents: profile.continents && Array.isArray(profile.continents) && profile.continents.length > 0,
    preferredCountries: profile.preferred_countries && Array.isArray(profile.preferred_countries) && profile.preferred_countries.length > 0,
    experienceLevel: profile.experience_level,
    educationLevel: profile.education_level,
    institutionType: profile.institution_type,
    diplomaStatus: profile.diploma_status,
    deloitteWorked: profile.deloitte_worked === 'yes' || profile.deloitte_worked === 'no',
    sg_eu_work_authorization: sgEuWorkAuthorization === 'yes' || sgEuWorkAuthorization === 'no',
    sg_notice_period: ['none', '1_month', '2_months', '3_months', 'more_than_3_months'].includes(
      String(profile.sg_notice_period || '').trim()
    ),
    cv: !!((profile.cv_storage_path || profile.cv_url || '').trim())
  };
  if (isBpce) {
    required.bpcePreferences = bpceHasContent;
  }
  const missingFields = [];
  for (const [k, v] of Object.entries(required)) {
    if (v === undefined || v === null || v === '' || v === false || (typeof v === 'string' && v.trim() === '')) {
      missingFields.push(PROFILE_FIELD_LABELS[k] || k);
    }
  }
  // BPCE : vérifier que l'email de connexion a été configuré dans la page Connexions
  if (isBpce) {
    let bpceEmailConfigured = false;
    try {
      const connRes = await fetch(`${base}/profiles/${taleosUserId}/career_connections/bpce`, {
        headers: { Authorization: `Bearer ${taleosIdToken}` }
      });
      if (connRes.ok) {
        const connData = parseFirestoreDoc(await connRes.json());
        bpceEmailConfigured = !!(connData.email || '').trim();
      }
    } catch (_) {}
    if (!bpceEmailConfigured) {
      missingFields.push('Email de connexion BPCE (page Connexions)');
    }
  }
  return { complete: missingFields.length === 0, missingFields };
}

async function fetchProfile(uid, bankId, token) {
  const base = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;
  const headers = { Authorization: `Bearer ${token}` };
  const normalizedBankId = String(bankId || '').toLowerCase().trim();
  const requiresCareerCredentials = !['credit_mutuel', 'bpifrance', 'jp_morgan', 'goldman_sachs'].includes(normalizedBankId);

  const profileRes = await fetch(`${base}/profiles/${uid}`, { headers });
  if (!profileRes.ok) throw new Error('Profil introuvable');
  const profile = parseFirestoreDoc(await profileRes.json());
  const sgEuWorkAuthorization = deriveEuWorkAuthorizationFromProfile(profile);

  let creds = null;
  const directRes = await fetch(`${base}/profiles/${uid}/career_connections/${bankId}`, { headers });
  if (directRes.ok) {
    creds = parseFirestoreDoc(await directRes.json());
  } else {
    const listRes = await fetch(`${base}/profiles/${uid}/career_connections`, { headers });
    if (listRes.ok) {
      const listJson = await listRes.json();
      const docs = listJson.documents || [];
      for (const d of docs) {
        const data = parseFirestoreDoc(d);
        if ((data.bankId || '').toLowerCase() === bankId.toLowerCase()) {
          creds = data;
          break;
        }
      }
    }
  }
  if (requiresCareerCredentials && (!creds || !creds.email)) {
    throw new Error(`Identifiants ${bankId} introuvables. Configurez-les sur la page Connexions.`);
  }
  if (!creds) creds = {};

  const authPassword = creds.password ? decodeBase64(creds.password) : '';

  const cvResolved = await resolveLatestProfileAsset(uid, 'cv', profile.cv_storage_path || null, profile.cv_filename || null);
  const lmResolved = await resolveLatestProfileAsset(uid, 'letter', profile.letter_storage_path || null, profile.letter_filename || null);
  const cvStoragePath = cvResolved.storagePath || null;
  const lmStoragePath = lmResolved.storagePath || null;
  const cvFilename = cvResolved.filename || (cvStoragePath ? cvStoragePath.split('/').pop() : null);
  const lmFilename = lmResolved.filename || (lmStoragePath ? lmStoragePath.split('/').pop() : null);

  const cType = profile.contract_type;
  const contractList = Array.isArray(cType) ? cType : (cType ? [cType] : []);
  const languages = (profile.languages || []).map(l => ({
    name: l.language || l.name || '',
    level: l.level || ''
  }));

  const phone = String(profile.phone || '').trim().replace(/\s/g, '');
  // Indicatif pays : priorité à Firebase (phone_country_code ou phoneCountryCode), pas de défaut +33 si l'utilisateur a mis +44
  let phoneCountryCode = (profile.phone_country_code || profile.phoneCountryCode || '').trim().replace(/\s/g, '');
  let phoneNumber = phone;
  if (!phoneCountryCode && phone) {
    if (phone.startsWith('+')) {
      const match = phone.match(/^(\+\d{1,4})(.*)$/);
      if (match) {
        phoneCountryCode = match[1];
        phoneNumber = (match[2] || '').replace(/\D/g, '') || phone;
      }
    } else if (phone.startsWith('0') && phone.length >= 10) {
      phoneCountryCode = '+33';
      phoneNumber = phone.slice(1).replace(/\D/g, '');
    }
  }
  if (!phoneCountryCode) phoneCountryCode = '+33';
  const normalizedAvailableDate = normalizeAvailableDateForAutomation(
    profile.available_from || profile.available_from_raw || profile.availableFrom || ''
  );

  return {
    civility: profile.civility || '',
    firstname: profile.first_name || '',
    lastname: profile.last_name || '',
    email: profile.email || creds.email || '',
    address: profile.address || '',
    zipcode: String(profile.postal_code || ''),
    city: profile.city || '',
    country: profile.country || '',
    phone_country_code: phoneCountryCode,
    phone_number: phoneNumber || phone,
    'phone-number': profile.phone || '',
    job_families: profile.jobs || [],
    contract_types: contractList,
    available_date: normalizedAvailableDate,
    available_from_raw: normalizedAvailableDate,
    salary_expectations: (profile.salary_expectations || '').trim(),
    continents: profile.continents || [],
    target_countries: profile.preferred_countries || [],
    target_regions: profile.regions || [],
    experience_level: profile.experience_level || '',
    education_level: profile.education_level || '',
    establishment: (profile.establishment || profile.institution_name || '').trim(),
    school_type: profile.institution_type || '',
    diploma_status: profile.diploma_status || '',
    diploma_year: String(profile.graduation_year ?? profile.graduationYear ?? ''),
    languages,
    cv_storage_path: cvStoragePath,
    lm_storage_path: lmStoragePath,
    cv_filename: cvFilename,
    lm_filename: lmFilename,
    auth_email: (creds.email || '').trim(),
    auth_password: authPassword,
    deloitte_worked: profile.deloitte_worked || 'no',
    deloitte_old_office: profile.deloitte_old_office || '',
    deloitte_old_email: profile.deloitte_old_email || '',
    deloitte_country: profile.deloitte_country || '',
    bpce_handicap: profile.bpce_handicap || '',
    bpce_vivier_natixis: profile.bpce_vivier_natixis || '',
    bpce_application_source: (profile.bpce_application_source || '').trim(),
    linkedin_url: (profile.linkedin_url || '').trim(),
    bpce_job_alerts: !!profile.bpce_job_alerts,
    bpifrance_talent_pool: (profile.bpifrance_talent_pool || '').trim(),
    axa_talent_pool: (profile.axa_talent_pool || '').trim(),
    group_data_sharing_scope: (profile.group_data_sharing_scope || profile.bnp_data_sharing_scope || '').trim(),
    sg_eu_work_authorization: sgEuWorkAuthorization,
    sg_notice_period: profile.sg_notice_period || '',
    sg_handicap: profile.sg_handicap || '',
    sg_handicap_accommodation: profile.sg_handicap_accommodation || '',
    jp_morgan_military_service: profile.jp_morgan_military_service || '',
    jp_morgan_work_authorizations: Array.isArray(profile.jp_morgan_work_authorizations) ? profile.jp_morgan_work_authorizations : [],
    // Goldman Sachs — diversité & identité
    gender: (profile.gender || '').trim(),
    pronouns: (profile.pronouns || '').trim(),
    work_authorization_type: Array.isArray(profile.work_authorization_type) ? profile.work_authorization_type : [],
    gs_diversity_consent: (profile.gs_diversity_consent || '').trim(),
    gs_transgender: (profile.gs_transgender || '').trim(),
    gs_sexual_orientation: (profile.gs_sexual_orientation || '').trim(),
    gs_race_ethnicity: (profile.gs_race_ethnicity || '').trim(),
    gs_race_additional_origins: Array.isArray(profile.gs_race_additional_origins) ? profile.gs_race_additional_origins : [],
    gs_disability: (profile.gs_disability || '').trim()
  };
}

function parseFirestoreDoc(json) {
  const fields = json.fields || {};
  const out = {};
  for (const [k, v] of Object.entries(fields)) {
    if (v.stringValue !== undefined) out[k] = v.stringValue;
    else if (v.integerValue !== undefined) out[k] = parseInt(v.integerValue, 10);
    else if (v.booleanValue !== undefined) out[k] = v.booleanValue;
    else if (v.doubleValue !== undefined) out[k] = v.doubleValue;
    else if (v.arrayValue?.values) {
      out[k] = v.arrayValue.values.map(x => {
        if (x.mapValue?.fields) return parseFirestoreDoc({ fields: x.mapValue.fields });
        if (x.stringValue !== undefined) return x.stringValue;
        return null;
      });
    } else if (v.mapValue?.fields) out[k] = parseFirestoreDoc({ fields: v.mapValue.fields });
  }
  return out;
}

function decodeBase64(str) {
  try {
    str = String(str).trim();
    let pad = str.length % 4;
    if (pad) str += '='.repeat(4 - pad);
    return atob(str);
  } catch {
    return str;
  }
}

async function getStorageDownloadUrl(storagePath, token) {
  const url = `https://firebasestorage.googleapis.com/v0/b/project-taleos.firebasestorage.app/o/${encodeURIComponent(storagePath)}?alt=media`;
  try {
    const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    if (!res.ok) return null;
    return res.url;
  } catch {
    return null;
  }
}

function parseFilenameFromContentDisposition(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const utf8Match = raw.match(/filename\\*=UTF-8''([^;]+)/i);
  if (utf8Match?.[1]) {
    try { return decodeURIComponent(utf8Match[1]).trim(); } catch (_) {}
  }
  const quotedMatch = raw.match(/filename=\"([^\"]+)\"/i);
  if (quotedMatch?.[1]) return quotedMatch[1].trim();
  const plainMatch = raw.match(/filename=([^;]+)/i);
  if (plainMatch?.[1]) return plainMatch[1].trim().replace(/^\"|\"$/g, '');
  return '';
}

async function fetchStorageObjectMetadata(storagePath) {
  const { taleosIdToken } = await chrome.storage.local.get(['taleosIdToken']);
  if (!taleosIdToken || !storagePath) return null;
  const url = `https://firebasestorage.googleapis.com/v0/b/project-taleos.firebasestorage.app/o/${encodeURIComponent(storagePath)}`;
  try {
    const res = await fetch(url, { headers: { Authorization: `Bearer ${taleosIdToken}` } });
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

async function listStorageObjects(prefix) {
  const { taleosIdToken } = await chrome.storage.local.get(['taleosIdToken']);
  if (!taleosIdToken || !prefix) return [];
  const url = `https://firebasestorage.googleapis.com/v0/b/project-taleos.firebasestorage.app/o?prefix=${encodeURIComponent(prefix)}`;
  try {
    const res = await fetch(url, { headers: { Authorization: `Bearer ${taleosIdToken}` } });
    if (!res.ok) return [];
    const json = await res.json();
    return Array.isArray(json?.items) ? json.items : [];
  } catch {
    return [];
  }
}

function extractTimestampFromStorageName(name) {
  const match = String(name || '').match(/_(\\d{10,})\\.[a-z0-9]+$/i);
  return match ? Number(match[1]) : 0;
}

function isGeneratedProfileAssetFilename(name, kind) {
  const raw = String(name || '').trim();
  if (!raw) return false;
  const escapedKind = kind === 'letter' ? 'letter' : 'cv';
  return new RegExp(`^${escapedKind}_[A-Za-z0-9]+_\\d{10,}\\.[a-z0-9]+$`, 'i').test(raw);
}

async function resolveLatestProfileAsset(uid, kind, currentPath, currentFilename) {
  if (!uid) {
    return { storagePath: currentPath || null, filename: currentFilename || null };
  }

  const prefix = `users/${uid}/`;
  const objectPrefix = kind === 'letter' ? `letter_${uid}_` : `cv_${uid}_`;
  const items = await listStorageObjects(prefix);
  const candidates = items
    .filter((item) => String(item?.name || '').startsWith(`${prefix}${objectPrefix}`))
    .sort((a, b) => {
      const aTs = extractTimestampFromStorageName(a?.name) || Date.parse(a?.updated || 0) || 0;
      const bTs = extractTimestampFromStorageName(b?.name) || Date.parse(b?.updated || 0) || 0;
      return bTs - aTs;
    });

  const chosen = candidates[0] || null;
  const chosenPath = chosen?.name || currentPath || null;
  let filename = currentFilename || null;

  const meta = chosenPath ? await fetchStorageObjectMetadata(chosenPath) : null;
  const metaFilename = parseFilenameFromContentDisposition(meta?.contentDisposition)
    || meta?.metadata?.originalName
    || meta?.metadata?.filename
    || null;

  if (metaFilename && (!filename || isGeneratedProfileAssetFilename(filename, kind))) {
    filename = metaFilename;
  } else if (!filename && chosenPath) {
    filename = chosenPath.split('/').pop() || null;
  }

  return {
    storagePath: chosenPath,
    filename
  };
}

async function fetchStorageFileAsBase64(storagePath) {
  const { taleosIdToken } = await chrome.storage.local.get(['taleosIdToken']);
  if (!taleosIdToken) throw new Error('Non connecté');
  const url = `https://firebasestorage.googleapis.com/v0/b/project-taleos.firebasestorage.app/o/${encodeURIComponent(storagePath)}?alt=media`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${taleosIdToken}` } });
  if (!res.ok) throw new Error(`Storage ${res.status}`);
  const blob = await res.blob();
  const meta = await fetchStorageObjectMetadata(storagePath).catch(() => null);
  const filename = parseFilenameFromContentDisposition(meta?.contentDisposition)
    || meta?.metadata?.originalName
    || meta?.metadata?.filename
    || (storagePath ? storagePath.split('/').pop() : '')
    || 'document.pdf';
  return new Promise((resolve, reject) => {
    const r = new FileReader();
    r.onload = () => resolve({ base64: r.result.split(',')[1], type: blob.type || 'application/pdf', filename });
    r.onerror = () => reject(new Error('Lecture fichier'));
    r.readAsDataURL(blob);
  });
}

/**
 * Interception automatique du code PIN BPCE/Oracle via l'API Gmail
 */
async function checkGmailForBpcePin(tabId) {
  try {
    const { taleosUserId, taleosIdToken } = await chrome.storage.local.get(['taleosUserId', 'taleosIdToken']);
    if (!taleosUserId) return;
    const key = getGmailStorageKey(taleosUserId);
    const gmailAuth = (await chrome.storage.local.get(key))[key] || null;
    const hasGmailToken = !!(gmailAuth && gmailAuth.access_token && gmailAuth.expires_at > Date.now() + 30 * 1000);
    const bearerToken = hasGmailToken ? gmailAuth.access_token : taleosIdToken;
    if (!bearerToken) return;
    if (!hasGmailToken) {
      console.warn('[Taleos BPCE] Gmail non lié ou token expiré - liaison Gmail recommandée dans Connexions.');
    }

    console.log('[Taleos BPCE] Recherche du code PIN dans Gmail...');
    
    // Requête Gmail : chercher les emails de l'expéditeur Oracle BPCE reçus récemment
    const q = encodeURIComponent('from:ekez.fa.sender@workflow.mail.em2.cloud.oracle.com "Confirmer votre identité"');
    const res = await fetch(`https://www.googleapis.com/gmail/v1/users/me/messages?q=${q}&maxResults=1`, {
      headers: { Authorization: `Bearer ${bearerToken}` }
    });
    
    if (!res.ok) return;
    const data = await res.json();
    
    if (data.messages && data.messages.length > 0) {
      const msgId = data.messages[0].id;
      const msgRes = await fetch(`https://www.googleapis.com/gmail/v1/users/me/messages/${msgId}`, {
        headers: { Authorization: `Bearer ${bearerToken}` }
      });
      const msgData = await msgRes.json();
      
      // Extraction du corps du message (snippet ou body)
      const snippet = msgData.snippet || '';
      const pinMatch = snippet.match(/\b(\d{6})\b/);
      
      if (pinMatch) {
        const pinCode = pinMatch[1];
        console.log('[Taleos BPCE] Code PIN intercepté :', pinCode);
        chrome.tabs.sendMessage(tabId, { action: 'bpce_pin_code', pinCode });
        // Stockage temporaire pour le content script
        chrome.storage.local.set({ taleos_bpce_pin_code: pinCode });
      }
    }
  } catch (e) {
    console.error('[Taleos BPCE] Erreur interception Gmail:', e);
  }
}

async function checkOutlookForBpcePin(tabId) {
  try {
    const { taleosIdToken } = await chrome.storage.local.get(['taleosIdToken']);
    if (!taleosIdToken) return;
    const res = await fetch(OUTLOOK_FETCH_OTP_CF_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${taleosIdToken}`
      },
      body: JSON.stringify({})
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok || json.ok !== true) return;
    const pinCode = String(json.pinCode || '').trim();
    if (!/^\d{6}$/.test(pinCode)) return;
    chrome.tabs.sendMessage(tabId, { action: 'bpce_pin_code', pinCode });
    chrome.storage.local.set({ taleos_bpce_pin_code: pinCode });
  } catch (_) {}
}

// Surveillance des onglets pour déclencher la recherche du PIN
chrome.tabs.onUpdated.addListener((tabId, info, tab) => {
  if (info.status === 'complete' && tab.url?.includes('oraclecloud.com') && tab.url?.includes('/apply/email')) {
    // On lance une recherche toutes les 5 secondes pendant 2 minutes max
    let attempts = 0;
    const interval = setInterval(() => {
      checkGmailForBpcePin(tabId);
      checkOutlookForBpcePin(tabId);
      if (++attempts > 24) clearInterval(interval);
    }, 5000);
  }
});


/**
 * === GA4 TRACKING VIA MEASUREMENT PROTOCOL ===
 * Version 1.1.0 : Intégration du suivi analytique pour les candidatures
 */

const GA4_CONFIG = {
  MEASUREMENT_ID: 'G-4PZJ4QXMJ0',
  API_SECRET: 'S_nZvZMxQ1Kv9w_80lWorw'
};

/** Versions manifest (MP GA4 : à déclarer en dimensions personnalisées « événement » : extension_version, extension_version_name). */
function getExtensionVersionForGa4() {
  try {
    const m = chrome.runtime.getManifest();
    const v = String(m.version || 'unknown');
    const vn = String(m.version_name || m.version || '');
    return {
      extension_version: v.length > 100 ? v.slice(0, 100) : v,
      extension_version_name: vn.length > 100 ? vn.slice(0, 100) : vn
    };
  } catch (_) {
    return { extension_version: 'unknown', extension_version_name: '' };
  }
}

async function appendGa4EventLog(entry) {
  try {
    const { taleos_ga4_event_log = [] } = await chrome.storage.local.get('taleos_ga4_event_log');
    const next = [entry, ...taleos_ga4_event_log].slice(0, 20);
    await chrome.storage.local.set({ taleos_ga4_event_log: next });
  } catch (_) {}
}

async function getTrackingUserContext() {
  try {
    const { taleosUser, taleosUserId, taleosUserEmail } = await chrome.storage.local.get([
      'taleosUser',
      'taleosUserId',
      'taleosUserEmail'
    ]);
    const uid = taleosUser?.uid || taleosUserId || 'anonymous';
    return {
      uid: String(uid || 'anonymous'),
      email: String(taleosUserEmail || '').trim().toLowerCase()
    };
  } catch (_) {
    return { uid: 'anonymous', email: '' };
  }
}

const GA4_SESSION_KEYS = { id: 'ga4_mp_session_id', at: 'ga4_mp_session_at' };
const GA4_SESSION_TTL_MS = 30 * 60 * 1000;

/** session_id GA4 MP : entier (secondes), stable ~30 min — évite des sessions fantômes par événement. */
async function getGa4SessionIdForPayload() {
  const now = Date.now();
  const sid = Math.floor(now / 1000);
  try {
    if (chrome.storage?.session) {
      const o = await chrome.storage.session.get([GA4_SESSION_KEYS.id, GA4_SESSION_KEYS.at]);
      if (o[GA4_SESSION_KEYS.id] != null && o[GA4_SESSION_KEYS.at] != null && now - o[GA4_SESSION_KEYS.at] < GA4_SESSION_TTL_MS) {
        return Number(o[GA4_SESSION_KEYS.id]);
      }
      await chrome.storage.session.set({
        [GA4_SESSION_KEYS.id]: sid,
        [GA4_SESSION_KEYS.at]: now
      });
      return sid;
    }
  } catch (_) {}
  try {
    const o = await chrome.storage.local.get([GA4_SESSION_KEYS.id, GA4_SESSION_KEYS.at]);
    if (o[GA4_SESSION_KEYS.id] != null && o[GA4_SESSION_KEYS.at] != null && now - o[GA4_SESSION_KEYS.at] < GA4_SESSION_TTL_MS) {
      return Number(o[GA4_SESSION_KEYS.id]);
    }
    await chrome.storage.local.set({
      [GA4_SESSION_KEYS.id]: sid,
      [GA4_SESSION_KEYS.at]: now
    });
    return sid;
  } catch (_) {
    return sid;
  }
}

/**
 * Envoie un événement à Google Analytics 4 via le Measurement Protocol
 * @param {string} eventName - Nom de l'événement (ex: 'apply_start', 'apply_success')
 * @param {object} params - Paramètres additionnels (ex: {site: 'bpce', job_title: 'Risk Analyst'})
 * @param {string} userId - ID utilisateur Firebase (optionnel)
 */
async function sendGA4Event(eventName, params = {}, userId = null) {
  let userUid = String(userId || 'anonymous');
  try {
    // Récupération du user_id depuis Firebase si non fourni
    if (!userId) {
      const userCtx = await getTrackingUserContext();
      userId = userCtx.uid || 'anonymous';
    }
    const userCtx = await getTrackingUserContext();
    userUid = userCtx.uid || String(userId || 'anonymous');

    const extVer = getExtensionVersionForGa4();
    const sessionIdNum = await getGa4SessionIdForPayload();

    // MP GA4 : engagement_time_msec + session_id numérique requis pour la prise en compte fiable des rapports.
    // Ne pas envoyer timestamp_micros dans params (réservé / rejets possibles côté validation).
    const payload = {
      client_id: userUid,
      user_id: userUid,
      events: [
        {
          name: eventName,
          params: {
            ...params,
            ...extVer,
            user_uid: userUid,
            engagement_time_msec: 100,
            session_id: sessionIdNum
          }
        }
      ]
    };

    const collectUrl = `https://www.google-analytics.com/mp/collect?measurement_id=${GA4_CONFIG.MEASUREMENT_ID}&api_secret=${GA4_CONFIG.API_SECRET}`;
    const debugUrl = `https://www.google-analytics.com/debug/mp/collect?measurement_id=${GA4_CONFIG.MEASUREMENT_ID}&api_secret=${GA4_CONFIG.API_SECRET}`;

    // Envoi réel + validation debug pour diagnostiquer la qualité des événements.
    const response = await fetch(collectUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });

    let validationMessages = [];
    try {
      const debugRes = await fetch(debugUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      const debugJson = await debugRes.json().catch(() => ({}));
      validationMessages = Array.isArray(debugJson?.validationMessages) ? debugJson.validationMessages : [];
    } catch (_) {
      validationMessages = [{ description: 'Validation debug GA4 indisponible' }];
    }

    const debugValid = validationMessages.length === 0;
    const firstValidationIssue = validationMessages[0]?.description || '';

    if (response.ok) {
      console.log(`[Taleos Analytics] Événement "${eventName}" envoyé à GA4`);
      await appendGa4EventLog({
        at: Date.now(),
        name: eventName,
        ok: true,
        status: response.status,
        debug_valid: debugValid,
        debug_issue: firstValidationIssue || '',
        site: params?.site || 'unknown',
        job_id: params?.job_id || '',
        user_uid: userUid,
        extension_version: extVer.extension_version,
        error_type: params?.error_type || ''
      });
      await chrome.storage.local.set({
        taleos_ga4_last_event: {
          name: eventName,
          at: Date.now(),
          userId: userId || 'anonymous',
          user_uid: userUid,
          params: params || {},
          ok: true,
          debug_valid: debugValid,
          debug_issue: firstValidationIssue
        }
      });
    } else {
      console.warn(`[Taleos Analytics] Erreur envoi GA4:`, response.status);
      await appendGa4EventLog({
        at: Date.now(),
        name: eventName,
        ok: false,
        status: response.status,
        debug_valid: debugValid,
        debug_issue: firstValidationIssue || '',
        site: params?.site || 'unknown',
        job_id: params?.job_id || '',
        user_uid: userUid,
        extension_version: extVer.extension_version,
        error_type: params?.error_type || ''
      });
      await chrome.storage.local.set({
        taleos_ga4_last_event: {
          name: eventName,
          at: Date.now(),
          userId: userId || 'anonymous',
          user_uid: userUid,
          params: params || {},
          ok: false,
          status: response.status,
          debug_valid: debugValid,
          debug_issue: firstValidationIssue
        }
      });
    }
  } catch (e) {
    console.error('[Taleos Analytics] Erreur:', e);
    await appendGa4EventLog({
      at: Date.now(),
      name: eventName,
      ok: false,
      status: 0,
      debug_valid: false,
      debug_issue: '',
      site: params?.site || 'unknown',
      job_id: params?.job_id || '',
      user_uid: userUid,
      extension_version: getExtensionVersionForGa4().extension_version,
      error_type: params?.error_type || '',
      error: e?.message || String(e)
    });
    await chrome.storage.local.set({
      taleos_ga4_last_event: {
        name: eventName,
        at: Date.now(),
        userId: userId || 'anonymous',
        params: params || {},
        ok: false,
        error: e?.message || String(e)
      }
    });
  }
}

/**
 * Envoie un événement de candidature au démarrage
 */
function normalizeSite(site, offerUrl) {
  const raw = (site || '').toLowerCase();
  if (raw.includes('credit') || raw.includes('agricole')) return 'credit_agricole';
  if (raw.includes('mutuel')) return 'credit_mutuel';
  if (raw.includes('bpifrance') || raw.includes('bpi')) return 'bpifrance';
  if (raw.includes('jp morgan') || raw.includes('jpmorgan') || raw.includes('jp_morgan')) return 'jp_morgan';
  if (raw.includes('goldman') || raw.includes('goldman sachs') || raw.includes('goldman_sachs')) return 'goldman_sachs';
  if (raw.includes('axa')) return 'axa';
  if (raw.includes('societe') || raw.includes('socgen')) return 'societe_generale';
  if (raw.includes('bpce')) return 'bpce';
  if (raw.includes('deloitte')) return 'deloitte';
  const url = (offerUrl || '').toLowerCase();
  if (url.includes('groupecreditagricole.jobs')) return 'credit_agricole';
  if (url.includes('recrutement.creditmutuel.fr')) return 'credit_mutuel';
  if (url.includes('talents.bpifrance.fr') || url.includes('bpi.tzportal.io')) return 'bpifrance';
  if (url.includes('jpmc.fa.oraclecloud.com')) return 'jp_morgan';
  if (url.includes('higher.gs.com') || url.includes('hdpc.fa.us2.oraclecloud.com')) return 'goldman_sachs';
  if (url.includes('careers.axa.com') || url.includes('careers-fr-axa.icims.com') || url.includes('careers-en-axa.icims.com') || url.includes('candidature-recrutement.axa.fr')) return 'axa';
  if (url.includes('societegenerale') || url.includes('socgen.taleo.net')) return 'societe_generale';
  if (url.includes('recrutement.bpce.fr') || url.includes('oraclecloud.com') || url.includes('recruitmentplatform.com')) return 'bpce';
  if (url.includes('myworkdayjobs.com') || url.includes('deloitte.com')) return 'deloitte';
  return 'unknown';
}

async function resolveTrackingContext(bankId, jobId, offerUrl) {
  const directSite = normalizeSite(bankId, offerUrl);
  if (directSite !== 'unknown') {
    return { site: directSite, offerUrl: offerUrl || '' };
  }
  try {
    const key = String(jobId || '').trim();
    if (!key) return { site: directSite, offerUrl: offerUrl || '' };
    const { taleos_offer_meta_by_job = {} } = await chrome.storage.local.get(['taleos_offer_meta_by_job']);
    const meta = taleos_offer_meta_by_job[key] || {};
    const resolvedOfferUrl = offerUrl || meta.offerUrl || '';
    return { site: normalizeSite(bankId, resolvedOfferUrl), offerUrl: resolvedOfferUrl };
  } catch (_) {
    return { site: directSite, offerUrl: offerUrl || '' };
  }
}

function getLocalDateTimeParts() {
  const now = new Date();
  const pad = (n) => String(n).padStart(2, '0');
  const date = `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())}`;
  const time = `${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;
  const tz = Intl.DateTimeFormat().resolvedOptions().timeZone || 'Europe/Paris';
  return { event_local_date: date, event_local_time: time, event_local_datetime: `${date} ${time}`, event_timezone: tz };
}

function normalizeAvailableDateForAutomation(rawValue) {
  const raw = String(rawValue || '').trim();
  const today = getLocalDateTimeParts().event_local_date;
  if (!raw) return today;

  let year = '';
  let month = '';
  let day = '';

  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    [year, month, day] = raw.split('-');
  } else if (/^\d{2}\/\d{2}\/\d{4}$/.test(raw)) {
    [day, month, year] = raw.split('/');
  } else {
    return today;
  }

  const normalized = `${year}-${month}-${day}`;
  return normalized < today ? today : normalized;
}

async function getOfferMetaForTracking(jobId) {
  try {
    const key = String(jobId || '').trim();
    if (!key) return {};
    const { taleos_offer_meta_by_job = {} } = await chrome.storage.local.get(['taleos_offer_meta_by_job']);
    return taleos_offer_meta_by_job[key] || {};
  } catch (_) {
    return {};
  }
}

async function trackApplyStart(site, jobTitle, jobId, offerUrl) {
  const ctx = await resolveTrackingContext(site, jobId, offerUrl);
  const meta = await getOfferMetaForTracking(jobId);
  const dt = getLocalDateTimeParts();
  await sendGA4Event('apply_start', {
    site: ctx.site,
    job_title: jobTitle || 'Unknown Position',
    job_id: jobId || 'unknown',
    offer_url: ctx.offerUrl || '',
    company_name: meta.companyName || '',
    location: meta.location || '',
    contract_type: meta.contractType || '',
    experience_level: meta.experienceLevel || '',
    job_family: meta.jobFamily || '',
    publication_date: meta.publicationDate || '',
    ...dt
  });
}

/**
 * Envoie un événement quand le code PIN est reçu
 */
async function trackPinReceived(site, offerUrl) {
  await sendGA4Event('pin_received', {
    site: normalizeSite(site, offerUrl)
  });
}

/**
 * Envoie un événement quand le formulaire est rempli
 */
async function trackFormFilled(site, jobTitle, jobId, offerUrl) {
  const ctx = await resolveTrackingContext(site, jobId, offerUrl);
  const meta = await getOfferMetaForTracking(jobId);
  const dt = getLocalDateTimeParts();
  await sendGA4Event('form_filled', {
    site: ctx.site,
    job_title: jobTitle || 'Unknown Position',
    job_id: jobId || 'unknown',
    offer_url: ctx.offerUrl || '',
    company_name: meta.companyName || '',
    location: meta.location || '',
    contract_type: meta.contractType || '',
    experience_level: meta.experienceLevel || '',
    job_family: meta.jobFamily || '',
    publication_date: meta.publicationDate || '',
    ...dt
  });
}

/**
 * Envoie un événement quand la candidature est soumise
 */
async function trackApplySuccess(site, jobTitle, jobId, offerUrl) {
  const ctx = await resolveTrackingContext(site, jobId, offerUrl);
  const meta = await getOfferMetaForTracking(jobId);
  const dt = getLocalDateTimeParts();
  await sendGA4Event('apply_success', {
    site: ctx.site,
    job_title: jobTitle || 'Unknown Position',
    job_id: jobId || 'unknown',
    offer_url: ctx.offerUrl || '',
    company_name: meta.companyName || '',
    location: meta.location || '',
    contract_type: meta.contractType || '',
    experience_level: meta.experienceLevel || '',
    job_family: meta.jobFamily || '',
    publication_date: meta.publicationDate || '',
    ...dt
  });
}

function classifyApplyError(errorMessage) {
  const raw = String(errorMessage || '');
  const msg = raw.toLowerCase();
  if (!raw.trim()) return { code: 'unknown', hint: 'Erreur non renseignée' };
  if (/404|introuvable|non disponible|n'est plus en ligne|expired|no longer online/.test(msg)) {
    return { code: 'offer_expired', hint: 'Offre expirée ou retirée' };
  }
  if (/question|mapping|mapp|non gér|non pris en charge|unsupported/.test(msg)) {
    return { code: 'unmapped_question', hint: 'Question non mappée dans le formulaire cible' };
  }
  if (/obligatoire|required|champ manquant|missing field|validation/.test(msg)) {
    return { code: 'required_field', hint: 'Champ obligatoire non complété ou validation échouée' };
  }
  if (/login|connexion|mot de passe|password|auth/.test(msg)) {
    return { code: 'auth', hint: 'Échec d’authentification sur le site carrière' };
  }
  if (/timeout|timed out|délai|attente/.test(msg)) {
    return { code: 'timeout', hint: 'Timeout pendant le parcours automatisé' };
  }
  if (/captcha|robot|verification/.test(msg)) {
    return { code: 'anti_bot', hint: 'Blocage anti-bot/captcha détecté' };
  }
  if (/network|fetch|net::|cors/.test(msg)) {
    return { code: 'network', hint: 'Erreur réseau/API pendant l’automatisation' };
  }
  return { code: 'other', hint: 'Erreur non catégorisée' };
}

async function trackApplyExpired(site, jobTitle, jobId, offerUrl, errorMessage) {
  const ctx = await resolveTrackingContext(site, jobId, offerUrl);
  const meta = await getOfferMetaForTracking(jobId);
  const dt = getLocalDateTimeParts();
  await sendGA4Event('apply_expired', {
    site: ctx.site,
    job_title: jobTitle || 'Unknown Position',
    job_id: jobId || 'unknown',
    offer_url: ctx.offerUrl || '',
    reason: String(errorMessage || 'Offre expirée').slice(0, 300),
    company_name: meta.companyName || '',
    location: meta.location || '',
    contract_type: meta.contractType || '',
    experience_level: meta.experienceLevel || '',
    job_family: meta.jobFamily || '',
    publication_date: meta.publicationDate || '',
    ...dt
  });
}

/**
 * Envoie un événement d'erreur
 */
async function trackError(errorType, errorMessage, site, jobId, offerUrl) {
  const ctx = await resolveTrackingContext(site, jobId, offerUrl);
  const meta = await getOfferMetaForTracking(jobId);
  const dt = getLocalDateTimeParts();
  const classified = classifyApplyError(errorMessage);
  await sendGA4Event('apply_error', {
    error_type: errorType || classified.code || 'unknown',
    error_code: classified.code || 'unknown',
    error_hint: classified.hint || 'Erreur inconnue',
    error_message: String(errorMessage || 'Unknown error').slice(0, 300),
    site: ctx.site,
    job_id: jobId || 'unknown',
    offer_url: ctx.offerUrl || '',
    company_name: meta.companyName || '',
    location: meta.location || '',
    contract_type: meta.contractType || '',
    experience_level: meta.experienceLevel || '',
    job_family: meta.jobFamily || '',
    publication_date: meta.publicationDate || '',
    ...dt
  });
}

// Exposition des fonctions GA4 pour les content scripts
chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (msg.action === 'track_event') {
    sendGA4Event(msg.eventName, msg.params, msg.userId).then(() => {
      sendResponse({ ok: true });
    }).catch(e => {
      console.error('[Taleos Analytics] Erreur tracking:', e);
      sendResponse({ ok: false, error: e.message });
    });
    return true; // Indique que la réponse sera asynchrone
  }
});
