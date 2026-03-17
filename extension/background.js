/**
 * Taleos Extension - Background Service Worker
 * Orchestre : ouverture onglet, récupération profil Firestore, injection du script banque
 */

chrome.alarms.create('taleos-keepalive', { periodInMinutes: 4 });
chrome.alarms.onAlarm.addListener((alarm) => {
  if (alarm.name === 'taleos-keepalive') { /* keep service worker warm */ }
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
  deloitte: 'scripts/credit_agricole.js',
  bpce: 'content/bpce-careers-filler.js'
};

const PROJECT_ID = 'project-taleos';

let authSyncResolve = null;
const sgLastInject = new Map();
const caLastInject = new Map();

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
    await chrome.scripting.executeScript({ target, files: ['scripts/job-family-mapping.js', BANK_SCRIPT_MAP.societe_generale] });
    await chrome.scripting.executeScript({
      target,
      func: (data) => { if (window.__taleosRun) window.__taleosRun(data); },
      args: [profile]
    });
    console.log('[Taleos SG] Injection OK');
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
    await chrome.scripting.executeScript({ target: { tabId }, files: [BANK_SCRIPT_MAP.credit_agricole] });
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
    await chrome.scripting.executeScript({ target: { tabId }, files: [BANK_SCRIPT_MAP.credit_agricole] });
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
        await chrome.scripting.executeScript({ target: { tabId }, files: [BANK_SCRIPT_MAP.credit_agricole] });
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
    handleApply(msg.offerUrl, msg.bankId, msg.jobId, msg.jobTitle, msg.companyName, taleosTabId)
      .then((result) => sendResponse(result?.error ? { error: result.error, openUrl: true } : { ok: true }))
      .catch(e => sendResponse({ error: e.message || 'Erreur', openUrl: true }));
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
        sendResponse({ ok: true });
      } catch (e) {
        sendResponse({ error: e.message || 'Erreur profil' });
      }
    })();
    return true;
  }
  if (msg.action === 'candidature_success') {
    chrome.storage.local.remove(['taleos_pending_sg', 'taleos_sg_tab_id']);
    if (sender.tab?.id) sgLastInject.delete(sender.tab.id);
    const tabIdToClose = sender.tab?.id;
    saveCandidatureAndNotifyTaleos(msg, tabIdToClose).then(sendResponse).catch(e => sendResponse({ error: e.message }));
    return true;
  }
  if (msg.action === 'candidature_failure') {
    const { offerExpired, jobId, jobTitle, error } = msg;
    const isExpired = !!offerExpired || /404|non disponible|expirée|n'est plus en ligne/i.test(error || '');
    chrome.storage.local.remove(['taleos_pending_sg', 'taleos_sg_tab_id']);
    if (sender.tab?.id) {
      sgLastInject.delete(sender.tab.id);
      if (isExpired) chrome.tabs.remove(sender.tab.id).catch(() => {});
    }
    if (isExpired && jobId) {
      notifyTaleosOfferUnavailable({ jobId, jobTitle: jobTitle || '' }).then(() => sendResponse({ ok: true })).catch(e => sendResponse({ error: e.message }));
    } else {
      notifyTaleosCandidatureFailure(msg).then(() => sendResponse({ ok: true })).catch(e => sendResponse({ error: e.message }));
    }
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

const CONNECTION_TEST_URLS = {
  credit_agricole: 'https://groupecreditagricole.jobs/fr/connexion/',
  societe_generale: 'https://socgen.taleo.net/careersection/iam/accessmanagement/login.jsf?lang=fr-FR&redirectionURI=https%3A%2F%2Fsocgen.taleo.net%2Fcareersection%2Fsgcareers%2Fprofile.ftl%3Flang%3Dfr-FR%26src%3DCWS-1%26pcid%3Dmjlsx8hz6i4vn92z&TARGET=https%3A%2F%2Fsocgen.taleo.net%2Fcareersection%2Fsgcareers%2Fprofile.ftl%3Flang%3Dfr-FR%26src%3DCWS-1%26pcid%3Dmjlsx8hz6i4vn92z',
  deloitte: 'https://fina.wd103.myworkdayjobs.com/fr-FR/DeloitteRecrute'
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

async function runTestConnection(msg) {
  const { bankId, email, password, firebaseUserId, taleosTabId, bankName } = msg;
  const loginUrl = CONNECTION_TEST_URLS[bankId];
  if (!loginUrl || !email || !password || !firebaseUserId) {
    return { success: false, message: 'Paramètres manquants' };
  }

  const { taleosIdToken } = await chrome.storage.local.get(['taleosIdToken']);
  if (!taleosIdToken) {
    return { success: false, message: 'Vous devez être connecté à Taleos' };
  }

  const tab = await chrome.tabs.create({ url: loginUrl, active: true });
  const tabId = tab.id;

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

    const fillRes = await runFill(bankId === 'deloitte' ? 2 : 0);
    if (fillRes?.[0]?.result?.error && !fillRes[0].result?.submitted) {
      await chrome.tabs.remove(tabId).catch(() => {});
      chrome.storage.local.remove('taleos_connection_test');
      return { success: false, message: fillRes[0].result.error };
    }

    await new Promise(r => setTimeout(r, 8000));

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
    return { success: false, message: e.message || 'Erreur technique' };
  }
}

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
    chrome.storage.local.remove(['taleos_ca_candidature_reloaded', 'taleos_ca_candidature_pending']);
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
  } else if (bankId === 'deloitte' || (offerUrl && (String(offerUrl).includes('myworkdayjobs.com') && String(offerUrl).toLowerCase().includes('deloitte')))) {
    chrome.storage.local.set({ taleos_pending_tab: taleosTabId });
    const deloitteCreateOpts = { url: offerUrl, active: false };
    if (taleosTabId) {
      try {
        const taleosTab = await chrome.tabs.get(taleosTabId);
        if (taleosTab?.index != null) deloitteCreateOpts.index = taleosTab.index + 1;
      } catch (_) {}
    }
    const tab = await chrome.tabs.create(deloitteCreateOpts);
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
    if (taleosTabId) {
      chrome.tabs.update(taleosTabId, { active: true }).catch(() => {});
    }
  } else if (bankId === 'societe_generale' || (offerUrl && String(offerUrl).toLowerCase().includes('careers.societegenerale.com')) || (offerUrl && String(offerUrl).toLowerCase().includes('socgen.taleo.net'))) {
    chrome.storage.local.set({ taleos_pending_tab: taleosTabId });
    const createOpts = { url: offerUrl, active: true };
    if (taleosTabId) {
      try {
        const taleosTab = await chrome.tabs.get(taleosTabId);
        if (taleosTab?.index != null) createOpts.index = taleosTab.index + 1;
      } catch (_) {}
    }
    const tab = await chrome.tabs.create(createOpts);
    chrome.storage.local.remove('taleos_sg_navigate_profile_attempted');
    chrome.storage.local.set({
      taleos_pending_sg: {
        profile: { ...profile, __jobId: jobId, __jobTitle: jobTitle, __companyName: companyName, __offerUrl: offerUrl },
        offerUrl, jobId, jobTitle, companyName,
        timestamp: Date.now()
      },
      taleos_sg_tab_id: tab.id
    });
  } else if (bankId === 'bpce' || (offerUrl && String(offerUrl).toLowerCase().includes('recrutement.bpce.fr'))) {
    chrome.storage.local.set({ taleos_pending_tab: taleosTabId });
    const createOpts = { url: offerUrl, active: true };
    if (taleosTabId) {
      try {
        const taleosTab = await chrome.tabs.get(taleosTabId);
        if (taleosTab?.index != null) createOpts.index = taleosTab.index + 1;
      } catch (_) {}
    }
    const tab = await chrome.tabs.create(createOpts);
    chrome.storage.local.set({
      taleos_pending_bpce: {
        profile: { ...profile, __jobId: jobId, __jobTitle: jobTitle, __companyName: companyName || 'BPCE', __offerUrl: offerUrl },
        offerUrl, jobId, jobTitle, companyName: companyName || 'BPCE',
        tabId: tab.id,
        timestamp: Date.now()
      },
      taleos_bpce_tab_id: tab.id
    });
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
    if (taleosTabId) {
      chrome.tabs.update(taleosTabId, { active: true }).catch(() => {});
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
      chrome.tabs.update(taleosTab, { active: true }).catch(() => {});
    } catch (_) {}
  }
  if (tabIdToClose) {
    setTimeout(() => {
      chrome.tabs.remove(tabIdToClose).catch(() => {});
    }, 3000);
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
  continents: 'Continents',
  preferredCountries: 'Pays préférés',
  experienceLevel: 'Niveau d\'expérience',
  educationLevel: 'Niveau d\'études',
  institutionType: 'Type d\'établissement',
  diplomaStatus: 'Statut du diplôme',
  deloitteWorked: 'Avez-vous déjà travaillé pour Deloitte ?',
  cv: 'CV (Documents)',
  bpcePreferences: 'Préférences BPCE'
};

/** Vérifie si le profil utilisateur est complet (même logique que offres.html) */
async function checkProfileCompletenessFromFirestore(bankId) {
  const { taleosUserId, taleosIdToken } = await chrome.storage.local.get(['taleosUserId', 'taleosIdToken']);
  if (!taleosUserId || !taleosIdToken) return { complete: false, missingFields: ['Connexion'] };
  const base = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;
  const profileRes = await fetch(`${base}/profiles/${taleosUserId}`, { headers: { Authorization: `Bearer ${taleosIdToken}` } });
  if (!profileRes.ok) return { complete: false, missingFields: ['Profil'] };
  const profile = parseFirestoreDoc(await profileRes.json());
  const isBpce = bankId === 'bpce' || (typeof bankId === 'string' && bankId.toLowerCase().includes('bpce'));
  const bpceHasContent = !!((profile.bpce_handicap || '').trim() || (profile.bpce_vivier_natixis || '').trim() || (profile.linkedin_url || '').trim() || profile.bpce_job_alerts);
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
  return { complete: missingFields.length === 0, missingFields };
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
  // BPCE : fallback sur email du profil ou taleosUserEmail si pas de career_connection
  if (bankId === 'bpce' && (!creds || !creds.email)) {
    const { taleosUserEmail } = await chrome.storage.local.get(['taleosUserEmail']);
    const fallbackEmail = (profile.email || taleosUserEmail || '').trim();
    if (fallbackEmail) creds = { email: fallbackEmail };
  }
  if (!creds || !creds.email) throw new Error(`Identifiants ${bankId} introuvables. Configurez-les sur la page Connexions.`);

  const authPassword = creds.password ? decodeBase64(creds.password) : '';

  const cvStoragePath = profile.cv_storage_path || null;
  const lmStoragePath = profile.letter_storage_path || null;
  const cvFilename = profile.cv_filename || (cvStoragePath ? cvStoragePath.split('/').pop() : null);
  const lmFilename = profile.letter_filename || (lmStoragePath ? lmStoragePath.split('/').pop() : null);

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
    available_date: profile.available_from || '',
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
    linkedin_url: (profile.linkedin_url || '').trim(),
    bpce_job_alerts: !!profile.bpce_job_alerts
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
