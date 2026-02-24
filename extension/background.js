/**
 * Taleos Extension - Background Service Worker
 * Orchestre : ouverture onglet, récupération profil Firestore, injection du script banque
 */

const BANK_SCRIPT_MAP = {
  credit_agricole: 'scripts/credit_agricole.js',
  societe_generale: 'scripts/societe_generale.js',
  deloitte: 'scripts/credit_agricole.js'
};

const PROJECT_ID = 'project-taleos';

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (msg.action === 'test_credentials') {
    testCredentials(msg.bankId).then(sendResponse).catch(e => sendResponse({ ok: false, error: e.message }));
    return true;
  }
  if (msg.action === 'taleos_apply') {
    handleApply(msg.offerUrl, msg.bankId, msg.jobId)
      .then(() => sendResponse({ ok: true }))
      .catch(e => sendResponse({ error: e.message }));
    return true;
  }
  if (msg.action === 'reload_and_continue') {
    reloadAndContinue(sender.tab.id, msg.offerUrl, msg.bankId, msg.profile)
      .then(() => sendResponse({ ok: true }))
      .catch(e => sendResponse({ error: e.message }));
    return true;
  }
});

async function reloadAndContinue(tabId, offerUrl, bankId, profile) {
  await chrome.tabs.update(tabId, { url: offerUrl });
  const listener = (id, info) => {
    if (id !== tabId || info.status !== 'complete') return;
    chrome.tabs.onUpdated.removeListener(listener);
    const scriptPath = BANK_SCRIPT_MAP[bankId] || BANK_SCRIPT_MAP.credit_agricole;
    chrome.scripting.executeScript({ target: { tabId }, files: [scriptPath] }).then(() =>
      chrome.scripting.executeScript({
        target: { tabId },
        func: (data) => { if (window.__taleosRun) window.__taleosRun(data); },
        args: [{ ...profile, __phase: 2 }]
      })
    ).catch(e => console.error('[Taleos] Re-inject:', e));
  };
  chrome.tabs.onUpdated.addListener(listener);
}

async function handleApply(offerUrl, bankId, jobId) {
  const tab = await chrome.tabs.create({ url: offerUrl, active: true });
  const tabId = tab.id;

  const listener = async (id, info) => {
    if (id !== tabId || info.status !== 'complete') return;
    chrome.tabs.onUpdated.removeListener(listener);

    const { taleosUserId, taleosIdToken } = await chrome.storage.local.get(['taleosUserId', 'taleosIdToken']);
    if (!taleosUserId) {
      console.warn('[Taleos] Utilisateur non connecté');
      return;
    }

    let profile;
    try {
      profile = await fetchProfile(taleosUserId, bankId, taleosIdToken);
    } catch (e) {
      console.error('[Taleos] Profil:', e);
      return;
    }

    const scriptPath = BANK_SCRIPT_MAP[bankId] || BANK_SCRIPT_MAP.credit_agricole;

    try {
      await chrome.scripting.executeScript({ target: { tabId: id }, files: [scriptPath] });
      await chrome.scripting.executeScript({
        target: { tabId: id },
        func: (data) => { if (window.__taleosRun) window.__taleosRun(data); },
        args: [profile]
      });
    } catch (e) {
      console.error('[Taleos] Injection:', e);
    }
  };

  chrome.tabs.onUpdated.addListener(listener);
}

async function testCredentials(bankId) {
  const { taleosUserId, taleosIdToken } = await chrome.storage.local.get(['taleosUserId', 'taleosIdToken']);
  if (!taleosUserId || !taleosIdToken) throw new Error('Non connecté. Connectez-vous d\'abord.');
  const profile = await fetchProfile(taleosUserId, bankId, taleosIdToken);
  return { ok: true, email: profile.auth_email || '(vide)' };
}

async function fetchProfile(uid, bankId, token) {
  const base = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;
  const headers = { Authorization: `Bearer ${token}` };

  const profileRes = await fetch(`${base}/profiles/${uid}`, { headers });
  if (!profileRes.ok) throw new Error('Profil introuvable');
  const profile = parseFirestoreDoc(await profileRes.json());

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
  if (!creds || !creds.email) throw new Error(`Identifiants ${bankId} introuvables. Configurez-les sur la page Connexions.`);

  const authPassword = creds.password ? decodeBase64(creds.password) : '';

  const cvUrl = profile.cv_storage_path
    ? await getStorageDownloadUrl(profile.cv_storage_path, token)
    : null;
  const lmUrl = profile.letter_storage_path
    ? await getStorageDownloadUrl(profile.letter_storage_path, token)
    : null;

  const cType = profile.contract_type;
  const contractList = Array.isArray(cType) ? cType : (cType ? [cType] : []);
  const languages = (profile.languages || []).map(l => ({
    name: l.language || l.name || '',
    level: l.level || ''
  }));

  return {
    civility: profile.civility || '',
    firstname: profile.first_name || '',
    lastname: profile.last_name || '',
    address: profile.address || '',
    zipcode: String(profile.postal_code || ''),
    city: profile.city || '',
    country: profile.country || '',
    'phone-number': profile.phone || '',
    job_families: profile.jobs || [],
    contract_types: contractList,
    available_date: profile.available_from || '',
    continents: profile.continents || [],
    target_countries: profile.preferred_countries || [],
    target_regions: profile.regions || [],
    experience_level: profile.experience_level || '',
    education_level: profile.education_level || '',
    school_type: profile.institution_type || '',
    diploma_status: profile.diploma_status || '',
    diploma_year: String(profile.graduation_year || ''),
    languages,
    cv_url: cvUrl,
    lm_url: lmUrl,
    auth_email: (creds.email || '').trim(),
    auth_password: authPassword
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
