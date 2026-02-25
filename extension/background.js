/**
 * Taleos Extension - Background Service Worker
 * Orchestre : ouverture onglet, récupération profil Firestore, injection du script banque
 */

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
  deloitte: 'scripts/credit_agricole.js'
};

const PROJECT_ID = 'project-taleos';

let authSyncResolve = null;

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (msg.action === 'after_login_submit') {
      const { offerUrl, bankId, profile } = msg;
    chrome.storage.local.set({ taleos_redirect_fallback: offerUrl });
    chrome.storage.local.remove('taleos_pending_offer');
    const tabId = sender.tab?.id;
    if (tabId) {
      const scriptPath = BANK_SCRIPT_MAP[bankId] || BANK_SCRIPT_MAP.credit_agricole;
      const injectAndRun = (phase) => {
        const p = { ...profile, __phase: phase, __jobId: profile.__jobId, __jobTitle: profile.__jobTitle, __companyName: profile.__companyName, __offerUrl: offerUrl };
        chrome.scripting.executeScript({ target: { tabId }, files: [scriptPath] }).then(() =>
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
  if (msg.action === 'test_credentials') {
    testCredentials(msg.bankId).then(sendResponse).catch(e => sendResponse({ ok: false, error: e.message }));
    return true;
  }
  if (msg.action === 'taleos_apply') {
    const taleosTabId = sender.tab?.id;
    handleApply(msg.offerUrl, msg.bankId, msg.jobId, msg.jobTitle, msg.companyName, taleosTabId)
      .then(() => sendResponse({ ok: true }))
      .catch(e => sendResponse({ error: e.message }));
    return true;
  }
  if (msg.action === 'candidature_success') {
    const tabIdToClose = sender.tab?.id;
    saveCandidatureAndNotifyTaleos(msg, tabIdToClose).then(sendResponse).catch(e => sendResponse({ error: e.message }));
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

const CA_CONNEXION_URL = 'https://groupecreditagricole.jobs/fr/connexion/';

async function handleApply(offerUrl, bankId, jobId, jobTitle, companyName, taleosTabId) {
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
    return;
  }

  let profile;
  try {
    profile = await fetchProfile(taleosUserId, bankId, taleosIdToken);
  } catch (e) {
    console.error('[Taleos] Profil:', e);
    return;
  }
  profile.__jobId = jobId;
  profile.__jobTitle = jobTitle || '';
  profile.__companyName = companyName || 'Crédit Agricole';
  profile.__offerUrl = offerUrl;

  const scriptPath = BANK_SCRIPT_MAP[bankId] || BANK_SCRIPT_MAP.credit_agricole;
  chrome.storage.local.set({ taleos_pending_tab: taleosTabId });

  const isCreditAgricole = bankId === 'credit_agricole' || (offerUrl && String(offerUrl).toLowerCase().includes('groupecreditagricole.jobs'));
  if (isCreditAgricole) {
    chrome.storage.local.set({
      taleos_pending_offer: {
        offerUrl,
        bankId,
        profile: { ...profile, __phase: 2, __jobId: jobId, __jobTitle: jobTitle, __companyName: companyName, __offerUrl: offerUrl },
        timestamp: Date.now()
      }
    });
    const tab = await chrome.tabs.create({ url: CA_CONNEXION_URL, active: false });
    const tabId = tab.id;
    if (taleosTabId) {
      chrome.tabs.update(taleosTabId, { active: true }).catch(() => {});
      [100, 300, 600].forEach(ms => setTimeout(() => {
        chrome.tabs.update(taleosTabId, { active: true }).catch(() => {});
      }, ms));
    }

    const injectAndRun = (phase) => {
      const p = { ...profile, __phase: phase ?? 2 };
      chrome.scripting.executeScript({ target: { tabId }, files: [scriptPath] }).then(() =>
        chrome.scripting.executeScript({
          target: { tabId },
          func: (data) => { if (window.__taleosRun) window.__taleosRun(data); },
          args: [p]
        })
      ).catch(e => console.error('[Taleos] Injection:', e));
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
          await new Promise(r => setTimeout(r, 5000));
          injectAndRun(3);
          return;
        }
        if (url.includes('/nos-offres-emploi/') || url.includes('/our-offers/') || url.includes('/our-offres/')) {
          chrome.storage.local.remove('taleos_pending_offer');
          await new Promise(r => setTimeout(r, 2000));
          injectAndRun(2);
        }
      } catch (_) {}
    };
    chrome.tabs.onUpdated.addListener(listener);
    setTimeout(() => chrome.tabs.onUpdated.removeListener(listener), 120000);
  } else {
    const tab = await chrome.tabs.create({ url: offerUrl, active: false });
    const tabId = tab.id;
    if (taleosTabId) {
      chrome.tabs.update(taleosTabId, { active: true }).catch(() => {});
      [100, 300, 600].forEach(ms => setTimeout(() => {
        chrome.tabs.update(taleosTabId, { active: true }).catch(() => {});
      }, ms));
    }
    const listener = async (id, info) => {
      if (id !== tabId || info.status !== 'complete') return;
      chrome.tabs.onUpdated.removeListener(listener);
      await new Promise(r => setTimeout(r, 1500));
      try {
        await chrome.scripting.executeScript({ target: { tabId }, files: [scriptPath] });
        await chrome.scripting.executeScript({
          target: { tabId },
          func: (data) => { if (window.__taleosRun) window.__taleosRun(data); },
          args: [profile]
        });
      } catch (e) {
        console.error('[Taleos] Injection:', e);
      }
    };
    chrome.tabs.onUpdated.addListener(listener);
  }
}

async function saveCandidatureAndNotifyTaleos(msg, tabIdToClose) {
  const { jobId, jobTitle, companyName, offerUrl } = msg;
  const { taleosUserId, taleosIdToken, taleos_pending_tab } = await chrome.storage.local.get(['taleosUserId', 'taleosIdToken', 'taleos_pending_tab']);
  chrome.storage.local.remove('taleos_pending_tab');
  if (!taleosUserId || !taleosIdToken) return;

  const safe = (s) => (s || '').trim().replace(/[/\\.]/g, '_').replace(/\s+/g, '_').slice(0, 150) || 'inconnu';
  const now = new Date();
  const dd = String(now.getDate()).padStart(2, '0');
  const mm = String(now.getMonth() + 1).padStart(2, '0');
  const yyyy = now.getFullYear();
  const datePart = dd + '\uFF0F' + mm + '\uFF0F' + yyyy;
  const docId = (datePart + ' \u203A ' + safe(companyName) + ' \u203A ' + safe(jobTitle) + ' \u203A ' + (jobId || 'unknown')).slice(0, 1500);

  const doc = {
    jobId: String(jobId || '').trim(),
    jobTitle: (jobTitle || '').trim(),
    jobUrl: offerUrl || '',
    companyName: companyName || 'Non spécifié',
    location: 'Non spécifié',
    contractType: 'Non spécifié',
    experienceLevel: 'Non spécifié',
    jobFamily: 'Non spécifié',
    publicationDate: 'Non spécifié',
    appliedDate: { seconds: Math.floor(Date.now() / 1000), nanoseconds: 0 },
    status: 'envoyée'
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
      await chrome.tabs.sendMessage(taleosTab, { action: 'taleos_candidature_success', jobId, status: 'envoyée' });
    } catch (_) {}
  }
  if (tabIdToClose) {
    setTimeout(() => {
      chrome.tabs.remove(tabIdToClose).catch(() => {});
    }, 5000);
  }
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

  const cvStoragePath = profile.cv_storage_path || null;
  const lmStoragePath = profile.letter_storage_path || null;

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
    cv_storage_path: cvStoragePath,
    lm_storage_path: lmStoragePath,
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

async function fetchStorageFileAsBase64(storagePath) {
  const { taleosIdToken } = await chrome.storage.local.get(['taleosIdToken']);
  if (!taleosIdToken) throw new Error('Non connecté');
  const url = `https://firebasestorage.googleapis.com/v0/b/project-taleos.firebasestorage.app/o/${encodeURIComponent(storagePath)}?alt=media`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${taleosIdToken}` } });
  if (!res.ok) throw new Error(`Storage ${res.status}`);
  const blob = await res.blob();
  return new Promise((resolve, reject) => {
    const r = new FileReader();
    r.onload = () => resolve({ base64: r.result.split(',')[1], type: blob.type || 'application/pdf' });
    r.onerror = () => reject(new Error('Lecture fichier'));
    r.readAsDataURL(blob);
  });
}
