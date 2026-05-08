/**
 * Taleos - Content Script (site Taleos)
 * Intercepte le clic sur "Candidater" et envoie à l'extension pour ouverture + automatisation
 * Synchronise aussi l'auth depuis le site vers l'extension (connexion automatique)
 */

(function () {
  'use strict';
  if (window.__taleosInjectorLoaded) return;
  window.__taleosInjectorLoaded = true;
  try { document.documentElement.setAttribute('data-taleos-injector', 'ready'); } catch (_) { }
  try {
    const manifestVersion = chrome?.runtime?.getManifest?.()?.version || '';
    if (manifestVersion) document.documentElement.setAttribute('data-taleos-injector-version', manifestVersion);
  } catch (_) { }

  function syncAuthFromPage(forceRefresh) {
    try {
      if (!chrome?.runtime?.id) return;
      chrome.runtime.sendMessage({ action: 'inject_auth_sync', forceRefresh: !!forceRefresh }).catch(function () { });
    } catch (_) { }
  }

  function isExtensionValid() {
    try { return !!chrome?.runtime?.id; } catch (_) { return false; }
  }

  // Masque le module Outlook dans l'onglet Connexions.
  // Le site peut rendre ce bloc côté serveur/front, donc on le supprime côté DOM.
  function removeOutlookConnectionsBlock() {
    try {
      const candidates = Array.from(document.querySelectorAll('h1, h2, h3, h4, p, span, button, a'));
      for (const el of candidates) {
        const txt = String(el.textContent || '').toLowerCase();
        if (!txt) continue;
        if (!txt.includes('outlook')) continue;
        const card = el.closest('[class*="card"], [class*="module"], [class*="bank"], [class*="connection"], section, article, div');
        if (card && card.parentElement) {
          // Ne retire que les zones connexions/liaison pour éviter les faux positifs ailleurs.
          const zoneTxt = String(card.textContent || '').toLowerCase();
          if (zoneTxt.includes('liaison') || zoneTxt.includes('connexion') || zoneTxt.includes('lier mon compte') || zoneTxt.includes('otp')) {
            card.style.display = 'none';
          }
        }
      }
    } catch (_) { }
  }

  window.addEventListener('__TALEOS_AUTH_SYNC__', function (e) {
    const { token, uid, email } = e.detail || {};
    if (token && uid) {
      chrome.runtime.sendMessage({
        action: 'sync_auth_from_site',
        taleosUserId: uid,
        taleosIdToken: token,
        taleosUserEmail: email || ''
      }).catch(function () { });
    }
  });

  function scheduleSync() {
    syncAuthFromPage(false);
    setTimeout(function () { syncAuthFromPage(true); }, 2500);
    setTimeout(function () { syncAuthFromPage(true); }, 6000);
    setTimeout(removeOutlookConnectionsBlock, 600);
    setTimeout(removeOutlookConnectionsBlock, 1800);
    setTimeout(removeOutlookConnectionsBlock, 3500);
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', scheduleSync);
  } else {
    scheduleSync();
  }
  setTimeout(resumePendingApplyAfterReload, 1200);
  // Le DOM Connexions peut être re-rendu dynamiquement.
  const removeOutlookObserver = new MutationObserver(function () {
    removeOutlookConnectionsBlock();
  });
  try {
    removeOutlookObserver.observe(document.documentElement, { childList: true, subtree: true });
  } catch (_) { }
  window.addEventListener('pageshow', function (ev) {
    if (ev.persisted) {
      scheduleSync();
    }
    setTimeout(resumePendingApplyAfterReload, 800);
  });

  let lastHealthCheck = 0;
  const HEALTH_CHECK_INTERVAL = 90000;
  const HEALTH_CHECK_RETRY_DELAY = 1500;
  const HEALTH_CHECK_RETRIES = 3;
  function isContextInvalidated(err) {
    const msg = (err?.message || String(err)).toLowerCase();
    return /context invalidated|extension.*invalid/i.test(msg);
  }

  function isReceivingEndMissing(err) {
    const msg = (err?.message || String(err)).toLowerCase();
    return /receiving end does not exist/i.test(msg);
  }
  const PENDING_APPLY_INTENT_KEY = 'taleos_pending_apply_intent_v1';

  function savePendingApplyIntent(intent) {
    try {
      sessionStorage.setItem(PENDING_APPLY_INTENT_KEY, JSON.stringify({
        jobId: String(intent?.jobId || '').trim(),
        jobTitle: String(intent?.jobTitle || '').trim(),
        bankId: String(intent?.bankId || '').trim(),
        jobUrl: String(intent?.jobUrl || '').trim(),
        ts: Date.now(),
        attempts: Number(intent?.attempts || 0)
      }));
    } catch (_) { }
  }

  function loadPendingApplyIntent() {
    try {
      const raw = sessionStorage.getItem(PENDING_APPLY_INTENT_KEY);
      if (!raw) return null;
      const parsed = JSON.parse(raw);
      if (!parsed?.jobId) return null;
      if (Date.now() - Number(parsed.ts || 0) > 2 * 60 * 1000) {
        sessionStorage.removeItem(PENDING_APPLY_INTENT_KEY);
        return null;
      }
      return parsed;
    } catch (_) {
      return null;
    }
  }

  function clearPendingApplyIntent() {
    try { sessionStorage.removeItem(PENDING_APPLY_INTENT_KEY); } catch (_) { }
  }

  function findApplyButtonForJobId(jobId) {
    const normalized = String(jobId || '').trim();
    if (!normalized) return null;
    const direct = Array.from(document.querySelectorAll('.job-apply-btn, button[onclick*="applyToJob"]'))
      .find((el) => String(el.getAttribute('onclick') || '').includes(normalized));
    if (direct) return direct;
    const cards = Array.from(document.querySelectorAll('.job-card'));
    for (const card of cards) {
      const cardJobId = String(card.querySelector('.job-id')?.textContent || '').trim();
      if (cardJobId === normalized) {
        return card.querySelector('.job-apply-btn, button[onclick*="applyToJob"]');
      }
    }
    return null;
  }

  function reloadPageAndResumeApply(intent) {
    const current = loadPendingApplyIntent();
    const attempts = Number(current?.attempts || intent?.attempts || 0);
    if (attempts >= 2) {
      clearPendingApplyIntent();
      showReloadToast();
      return;
    }
    savePendingApplyIntent({ ...intent, attempts: attempts + 1 });
    showReloadToast();
  }

  function showReloadToast() {
    const toast = document.createElement('div');
    toast.textContent = 'Extension déconnectée. Rechargement de la page...';
    Object.assign(toast.style, {
      position: 'fixed', top: '20px', left: '50%', transform: 'translateX(-50%)', zIndex: 2147483647,
      background: '#dc2626', color: '#fff', padding: '12px 24px', borderRadius: '8px', fontSize: '14px',
      fontFamily: 'sans-serif', boxShadow: '0 4px 12px rgba(0,0,0,0.3)'
    });
    document.body.appendChild(toast);
    setTimeout(function () { window.location.reload(); }, 2000);
  }
  async function resumePendingApplyAfterReload() {
    const intent = loadPendingApplyIntent();
    if (!intent) return;
    if (!isExtensionValid()) return;
    try {
      await chrome.runtime.sendMessage({ action: 'ping' });
    } catch (_) {
      return;
    }
    const btn = findApplyButtonForJobId(intent.jobId);
    if (!btn) return;
    clearPendingApplyIntent();
    setTimeout(function () {
      try { btn.click(); } catch (_) { }
    }, 500);
  }
  async function healthCheck() {
    if (Date.now() - lastHealthCheck < HEALTH_CHECK_INTERVAL) return;
    lastHealthCheck = Date.now();
    for (let i = 0; i < HEALTH_CHECK_RETRIES; i++) {
      try {
        await chrome.runtime.sendMessage({ action: 'ping' });
        syncAuthFromPage(true);
        return;
      } catch (e) {
        const invalidated = isContextInvalidated(e);
        const receivingEnd = isReceivingEndMissing(e);
        if (invalidated || receivingEnd) {
          showReloadToast();
          return;
        }
        if (i < HEALTH_CHECK_RETRIES - 1) {
          await new Promise(function (r) { setTimeout(r, HEALTH_CHECK_RETRY_DELAY); });
          continue;
        }
      }
    }
  }
  document.addEventListener('visibilitychange', function () {
    if (document.visibilityState === 'visible') {
      setTimeout(healthCheck, 2000);
    }
  });
  setInterval(healthCheck, HEALTH_CHECK_INTERVAL);

  function getBankIdFromUrl(url) {
    if (!url) return null;
    const lowerUrl = String(url).toLowerCase();
    if (lowerUrl.includes('groupecreditagricole.jobs') || lowerUrl.includes('creditagricole')) return 'credit_agricole';
    if (lowerUrl.includes('careers.societegenerale.com') || lowerUrl.includes('societegenerale') || lowerUrl.includes('socgen.taleo.net')) return 'societe_generale';
    if (lowerUrl.includes('deloitte.com') || (lowerUrl.includes('myworkdayjobs.com') && lowerUrl.includes('deloitte'))) return 'deloitte';
    if (lowerUrl.includes('recrutement.bpce.fr')) return 'bpce';
    if (lowerUrl.includes('group.bnpparibas') || lowerUrl.includes('bwelcome.hr.bnpparibas')) return 'bnp_paribas';
    if (lowerUrl.includes('recrutement.creditmutuel.fr') || lowerUrl.includes('creditmutuel.fr')) return 'credit_mutuel';
    if (lowerUrl.includes('talents.bpifrance.fr') || lowerUrl.includes('bpi.tzportal.io')) return 'bpifrance';
    if (lowerUrl.includes('higher.gs.com') || lowerUrl.includes('hdpc.fa.us2.oraclecloud.com')) return 'goldman_sachs';
    if (lowerUrl.includes('jpmc.fa.oraclecloud.com')) return 'jp_morgan';
    if (lowerUrl.includes('careers.axa.com') || lowerUrl.includes('axa.com/careers-home') || lowerUrl.includes('icims.com/jobs/') || lowerUrl.includes('icims.eu/jobs/')) return 'axa';
    return null;
  }

  function getBankIdFromCompanyName(companyName) {
    const normalized = String(companyName || '').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '');
    if (!normalized) return null;
    if (normalized.includes('credit agricole') || normalized.includes('crédit agricole') || normalized.includes('amundi') || normalized.includes('lcl') || normalized.includes('bforbank') || normalized.includes('caceis') || normalized.includes('indosuez') || normalized.includes('idia')) return 'credit_agricole';
    if (normalized.includes('societe generale') || normalized.includes('société générale')) return 'societe_generale';
    if (normalized.includes('deloitte')) return 'deloitte';
    if (normalized.includes('bpce')) return 'bpce';
    if (normalized.includes('bnp paribas') || normalized.includes('arval') || normalized.includes('cardif') || normalized.includes('hello bank') || normalized.includes('nickel')) return 'bnp_paribas';
    if (normalized.includes('credit mutuel') || normalized.includes('crédit mutuel') || normalized.includes('cic') || normalized.includes('euro information')) return 'credit_mutuel';
    if (normalized.includes('bpifrance')) return 'bpifrance';
    if (normalized.includes('goldman sachs')) return 'goldman_sachs';
    if (normalized.includes('jp morgan') || normalized.includes('j.p. morgan') || normalized.includes('jpmorgan')) return 'jp_morgan';
    if (normalized.startsWith('axa') || normalized.includes(' groupe axa') || normalized.includes(' gie axa') || normalized.includes('direct assurance')) return 'axa';
    return null;
  }

  function getAxaApplyUrl(jobUrl, companyName = '') {
    const match = String(jobUrl || '').match(/\/jobs\/(\d+)(?:[/?#]|$)/i);
    if (!match) return jobUrl;
    const jobId = match[1];
    if (jobId === '16638') {
      return `https://careers-en-axa.icims.com/jobs/${jobId}/login?mobile=false&width=1331&height=500&bga=true&needsRedirect=false&jan1offset=60&jun1offset=120`;
    }
    const normalizedUrl = String(jobUrl || '').toLowerCase();
    const normalizedCompany = String(companyName || '').toLowerCase();
    if (normalizedCompany.includes('axa xl')) {
      return `https://careers-en-axa.icims.com/jobs/${jobId}/login?mobile=false&width=1331&height=500&bga=true&needsRedirect=false&jan1offset=60&jun1offset=120`;
    }
    if (normalizedUrl.includes('lang=en')) {
      return `https://careers-en-axa.icims.com/jobs/${jobId}/login?mobile=false&width=1331&height=500&bga=true&needsRedirect=false&jan1offset=60&jun1offset=120`;
    }
    if (normalizedUrl.includes('lang=fr')) {
      return `https://careers-fr-axa.icims.com/jobs/${jobId}/login?loginOnly=1&in_iframe=1`;
    }
    return jobUrl;
  }

  function findJobCard(el) {
    let node = el;
    while (node && node !== document.body) {
      const url = node.getAttribute?.('data-job-url');
      if (url) return { card: node, jobUrl: url };
      node = node.parentElement;
    }
    // Fallback : chercher le lien "Voir l'offre" à côté du bouton
    const actions = el.closest?.('.job-actions, .job-footer');
    const link = actions?.querySelector?.('a.job-link[href]');
    if (link?.href) {
      const card = actions?.closest?.('.job-card') || actions?.parentElement?.closest?.('.job-card') || actions;
      return { card, jobUrl: link.href };
    }
    return null;
  }

  function extractJobIdFromOnClick(btn) {
    const onclick = btn.getAttribute?.('onclick') || '';
    const m = onclick.match(/applyToJob\s*\(\s*['"]([^'"]+)['"]/);
    return m ? m[1] : null;
  }

  // Anti-spam + retry : empêcher les clics multiples, permettre retry si échec
  const COOLDOWN_MS = 2500;
  const FAILURE_TIMEOUT_MS = 4 * 60 * 1000;
  const processingJobs = new Map();

  function clearProcessing(jobId, reEnableButton) {
    const entry = processingJobs.get(jobId);
    if (entry) {
      if (entry.timeoutId) clearTimeout(entry.timeoutId);
      processingJobs.delete(jobId);
      if (reEnableButton && entry.btn && entry.btn.isConnected) {
        entry.btn.disabled = false;
        entry.btn.removeAttribute('data-taleos-processing');
        const orig = entry.btn.getAttribute('data-taleos-original-text');
        if (orig) entry.btn.textContent = orig;
      }
    }
  }

  function normalizeSiteForGa(bankId) {
    const raw = String(bankId || 'unknown').toLowerCase();
    if (raw.includes('credit') || raw.includes('agricole')) return 'credit_agricole';
    if (raw.includes('mutuel')) return 'credit_mutuel';
    if (raw.includes('bpifrance') || raw.includes('bpi')) return 'bpifrance';
    if (raw.includes('jp morgan') || raw.includes('jpmorgan') || raw.includes('jp_morgan')) return 'jp_morgan';
    if (raw.includes('societe') || raw.includes('socgen')) return 'societe_generale';
    if (raw.includes('bpce')) return 'bpce';
    if (raw.includes('deloitte')) return 'deloitte';
    return raw || 'unknown';
  }

  function showProfileIncompletePopup(missingFields, bankIdForTracking) {
    try {
      chrome.runtime.sendMessage({
        action: 'track_event',
        eventName: 'apply_blocked_profile',
        params: {
          site: normalizeSiteForGa(bankIdForTracking),
          missing_count: Array.isArray(missingFields) ? missingFields.length : 0
        }
      }).catch(function () { });
    } catch (_) { }

    const isBpceConnection = Array.isArray(missingFields) && missingFields.some(function (f) {
      return f.includes('Connexions') || f.includes('connexion BPCE') || f.includes('BPCE');
    });

    const titleText = isBpceConnection ? '🔗 Connexion manquante' : 'Profil incomplet';
    const msg = isBpceConnection
      ? 'Votre email de connexion BPCE n\'est pas configuré. Rendez-vous sur la page Connexions pour l\'ajouter avant de lancer une candidature BPCE.'
      : missingFields && missingFields.length > 0
        ? 'Votre profil est incomplet. Veuillez compléter toutes les informations requises dans Mon profil avant de lancer une candidature : ' + missingFields.join(', ')
        : 'Votre profil est incomplet. Complétez toutes les informations requises dans Mon profil avant de lancer une candidature.';
    const btnPrimaryText = isBpceConnection ? 'Configurer la connexion' : 'Compléter mon profil';
    const btnPrimaryTarget = isBpceConnection ? 'connexions.html?bank=bpce' : 'profile.html';

    // Créer la modale directement en DOM (évite CSP qui bloque l'injection de script inline)
    const existing = document.getElementById('taleos-profile-incomplete-modal');
    if (existing) existing.remove();

    const overlay = document.createElement('div');
    overlay.id = 'taleos-profile-incomplete-modal';
    overlay.setAttribute('role', 'dialog');
    overlay.setAttribute('aria-modal', 'true');
    overlay.setAttribute('aria-labelledby', 'taleos-profile-incomplete-title');
    overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.5);display:flex;align-items:center;justify-content:center;z-index:2147483647;font-family:system-ui,-apple-system,sans-serif';

    const box = document.createElement('div');
    box.style.cssText = 'background:#fff;padding:24px;border-radius:12px;max-width:450px;text-align:center;box-shadow:0 8px 32px rgba(0,0,0,0.2)';

    const title = document.createElement('h3');
    title.id = 'taleos-profile-incomplete-title';
    title.textContent = titleText;
    title.style.cssText = 'margin:0 0 16px;font-size:18px;color:#1f2937';

    const text = document.createElement('p');
    text.textContent = msg;
    text.style.cssText = 'margin:0 0 20px;font-size:14px;line-height:1.5;color:#4b5563';

    const btnProfile = document.createElement('button');
    btnProfile.textContent = btnPrimaryText;
    btnProfile.style.cssText = 'margin:8px;padding:10px 20px;background:#667eea;color:#fff;border:none;border-radius:8px;cursor:pointer;font-size:14px';

    const btnClose = document.createElement('button');
    btnClose.textContent = 'Fermer';
    btnClose.style.cssText = 'margin:8px;padding:10px 20px;background:#f3f4f6;border:none;border-radius:8px;cursor:pointer;font-size:14px;color:#374151';

    box.appendChild(title);
    box.appendChild(text);
    box.appendChild(btnProfile);
    box.appendChild(btnClose);
    overlay.appendChild(box);

    overlay.addEventListener('click', function (e) {
      if (e.target === overlay) overlay.remove();
    });
    btnProfile.addEventListener('click', function () {
      overlay.remove();
      window.location.href = new URL(btnPrimaryTarget, window.location.href).href;
    });
    btnClose.addEventListener('click', function () {
      overlay.remove();
    });

    document.body.appendChild(overlay);
  }

  function showApplyRetryToast(message) {
    const t = document.createElement('div');
    t.textContent = message || 'La candidature automatique n\'a pas pu démarrer. Réessayez dans un instant ou vérifiez votre profil.';
    Object.assign(t.style, {
      position: 'fixed', top: '20px', left: '50%', transform: 'translateX(-50%)', zIndex: 2147483647,
      background: '#1f2937', color: '#fff', padding: '12px 20px', borderRadius: '8px', fontSize: '14px',
      fontFamily: 'system-ui,sans-serif', boxShadow: '0 4px 12px rgba(0,0,0,0.25)', maxWidth: '420px', textAlign: 'center'
    });
    document.body.appendChild(t);
    setTimeout(function () { t.remove(); }, 7000);
  }

  function isProfileIncompleteApplyError(errText) {
    const s = String(errText || '').toLowerCase();
    return /profil\s+incomplet|profil\s+introuvable|informations\s+manquantes|complétez.*profil|champs?\s+obligatoires|requis.*profil|avant\s+de\s+lancer\s+une\s+candidature/i.test(s);
  }

  function setButtonProcessing(btn, jobId) {
    if (!btn.dataset.taleosOriginalText) btn.dataset.taleosOriginalText = btn.textContent || '📝 Candidater';
    btn.textContent = '⏳ En cours...';
    btn.disabled = true;
    btn.dataset.taleosProcessing = '1';
    btn.setAttribute('data-taleos-processing', '1');
    const timeoutId = setTimeout(function () {
      clearProcessing(jobId, true);
    }, FAILURE_TIMEOUT_MS);
    processingJobs.set(jobId, { btn, timestamp: Date.now(), timeoutId });
  }

  async function onApplyClick(e) {
    // Certains clics ciblent un TextNode (emoji/texte du bouton), qui n'a pas closest().
    const rawTarget = e.target;
    const targetEl = rawTarget && rawTarget.nodeType === Node.TEXT_NODE ? rawTarget.parentElement : rawTarget;
    const btn = targetEl?.closest?.('.job-apply-btn, button[onclick*="applyToJob"]');
    if (!btn) return;

    const found = findJobCard(btn);
    if (!found) return;

    const { card, jobUrl } = found;
    const jobId = extractJobIdFromOnClick(btn) || (card.querySelector('.job-id')?.textContent || '').trim();
    const jobTitle = (card.querySelector('.job-title')?.textContent || '').trim();
    const companyName = (
      card.querySelector('.job-company-name, .company-name-wrapper span, .job-company span, .job-company')?.textContent || ''
    ).trim();
    const publicationDate = (card.querySelector('.job-date')?.textContent || '').replace(/^Publiée le\s*/i, '').trim();
    const location = (card.querySelector('.tag-location')?.textContent || '').replace(/^🌍\s*/, '').trim();
    const contractType = (card.querySelector('.tag-contract')?.textContent || '').replace(/^📄\s*/, '').trim();
    const experienceLevel = (card.querySelector('.tag-experience')?.textContent || '').replace(/^💼\s*/, '').trim();
    const jobFamily = (card.querySelector('.tag-family')?.textContent || '').replace(/^🎯\s*/, '').trim();
    const offerMeta = {
      location: location || '',
      contractType: contractType || '',
      experienceLevel: experienceLevel || '',
      jobFamily: jobFamily || '',
      publicationDate: publicationDate || ''
    };
    let bankId = getBankIdFromUrl(jobUrl);
    if (jobUrl && String(jobUrl).toLowerCase().includes('groupecreditagricole.jobs')) {
      bankId = 'credit_agricole';
    }
    if (bankId === 'credit_agricole' && /société\s+générale|societe\s+generale|société\s+generale/i.test(companyName || '')) {
      bankId = 'societe_generale';
    }
    if ((bankId === 'credit_agricole' || !bankId) && /j\.?\s*p\.?\s*morgan|jp\s*morgan|jpmorgan/i.test(companyName || '')) {
      bankId = 'jp_morgan';
    }
    if (!bankId) bankId = getBankIdFromCompanyName(companyName);

    if (!jobId) return;

    const now = Date.now();
    const entry = processingJobs.get(jobId);
    if (entry) {
      if (now - entry.timestamp < COOLDOWN_MS) return;
      clearProcessing(jobId, false);
    }

    if (!isExtensionValid()) {
      reloadPageAndResumeApply({ jobId, jobTitle, bankId, jobUrl });
      return;
    }

    // Bloquer immédiatement le clic (sync) pour éviter le double onglet page + extension
    e.preventDefault();
    e.stopPropagation();
    e.stopImmediatePropagation();
    // Marquer tout de suite pour que la page (si elle reçoit l'événement) n'ouvre pas d'onglet
    btn.dataset.taleosProcessing = '1';
    btn.setAttribute('data-taleos-processing', '1');

    // Ping rapide : uniquement pour réveiller le service worker si besoin, mais on ne bloque plus l'automatisation sur un timeout
    try {
      await Promise.race([
        chrome.runtime.sendMessage({ action: 'ping' }),
        new Promise((_, reject) => setTimeout(() => reject(new Error('ping_timeout')), 1500))
      ]);
    } catch (e) {
      console.warn('[Taleos] Ping extension en échec ou timeout:', e?.message || e);
      if (isContextInvalidated(e) || isReceivingEndMissing(e)) {
        reloadPageAndResumeApply({ jobId, jobTitle, bankId, jobUrl });
        return;
      }
      // Sinon on tente quand même la suite.
    }

    try {
      const checkPromise = chrome.runtime.sendMessage({ action: 'taleos_check_profile_complete', bankId });
      const timeoutPromise = new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 6000));
      const checkRes = await Promise.race([checkPromise, timeoutPromise]);
      if (!checkRes || checkRes.complete !== true) {
        delete btn.dataset.taleosProcessing;
        btn.removeAttribute('data-taleos-processing');
        showProfileIncompletePopup(checkRes?.missingFields || [], bankId);
        return;
      }
    } catch (err) {
      console.warn('[Taleos] Vérification profil:', err);
      delete btn.dataset.taleosProcessing;
      btn.removeAttribute('data-taleos-processing');
      if (isContextInvalidated(err) || isReceivingEndMissing(err)) {
        reloadPageAndResumeApply({ jobId, jobTitle, bankId, jobUrl });
        return;
      }
      showProfileIncompletePopup([], bankId);
      return;
    }

    setButtonProcessing(btn, jobId);
    syncAuthFromPage(true);

    const openUrl = (bankId === 'credit_agricole' || jobUrl.includes('groupecreditagricole.jobs'))
      ? 'https://groupecreditagricole.jobs/fr/connexion/'
      : (bankId === 'axa' ? getAxaApplyUrl(jobUrl, companyName) : jobUrl);

    /** Ouvre l'URL d'offre uniquement si le profil Firestore est complet (revérification systématique). */
    async function openOfferUrlOnlyIfProfileComplete() {
      clearProcessing(jobId, true);
      let checkRes;
      try {
        checkRes = await chrome.runtime.sendMessage({ action: 'taleos_check_profile_complete', bankId });
      } catch (e) {
        showProfileIncompletePopup([], bankId);
        return;
      }
      if (!checkRes || checkRes.complete !== true) {
        showProfileIncompletePopup(checkRes?.missingFields || [], bankId);
        return;
      }
      chrome.storage.local.remove('taleos_apply_fallback');
      window.open(openUrl, '_blank');
    }

    async function tryApply() {
      await chrome.runtime.sendMessage({ action: 'ping' }).catch(() => { });
      return Promise.race([
        chrome.runtime.sendMessage({
          action: 'taleos_apply',
          offerUrl: jobUrl,
          bankId,
          jobId,
          jobTitle,
          companyName,
          offerMeta
        }),
        new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 12000))
      ]);
    }
    try {
      let response;
      try {
        response = await tryApply();
      } catch (firstErr) {
        if (!/timeout/i.test(firstErr?.message || '')) throw firstErr;
        await new Promise(r => setTimeout(r, 2000));
        response = await tryApply();
      }
      if (response?.error) {
        console.warn('[Taleos] handleApply:', response.error);
        if (isProfileIncompleteApplyError(response.error)) {
          clearProcessing(jobId, true);
          const match = String(response.error).match(/avant de lancer une candidature[:\s]+(.+)$/i);
          const missingFields = match ? match[1].split(',').map(m => m.trim()).filter(Boolean) : [];
          showProfileIncompletePopup(missingFields.length ? missingFields : [], bankId);
        } else {
          await openOfferUrlOnlyIfProfileComplete();
        }
      }
    } catch (err) {
      const msg = (err?.message || String(err)).toLowerCase();
      const contextInvalidated = isContextInvalidated(err);
      if (contextInvalidated || isReceivingEndMissing(err)) {
        clearProcessing(jobId, true);
        reloadPageAndResumeApply({ jobId, jobTitle, bankId, jobUrl });
        return;
      }
      console.warn('[Taleos] Candidature — timeout ou erreur:', err?.message || err);
      clearProcessing(jobId, true);
      try {
        const checkRes = await chrome.runtime.sendMessage({ action: 'taleos_check_profile_complete', bankId });
        if (!checkRes || checkRes.complete !== true) {
          showProfileIncompletePopup(checkRes?.missingFields || [], bankId);
          return;
        }
      } catch (_) {
        showProfileIncompletePopup([], bankId);
        return;
      }
      showApplyRetryToast(/timeout/i.test(msg)
        ? 'Délai dépassé : la candidature n\'a pas pu démarrer. Réessayez. Si le problème continue, enregistrez votre profil sur Taleos puis réessayez.'
        : 'Impossible de lancer la candidature pour le moment. Réessayez ou vérifiez que votre profil est à jour sur Taleos.');
    }
  }

  document.addEventListener('click', onApplyClick, true);

  chrome.runtime.onMessage.addListener(function (msg) {
    if (msg.action === 'taleos_candidature_success') {
      clearProcessing(msg.jobId || '', false);
      window.dispatchEvent(new CustomEvent('taleos-extension-candidature-success', {
        detail: { jobId: msg.jobId, status: msg.status }
      }));
    }
    if (msg.action === 'taleos_candidature_failure') {
      clearProcessing(msg.jobId || '', true);
      window.dispatchEvent(new CustomEvent('taleos-extension-candidature-failure', {
        detail: { jobId: msg.jobId, error: msg.error }
      }));
    }
    if (msg.action === 'taleos_request_auth') {
      syncAuthFromPage(true);
    }
    if (msg.action === 'taleos_auth_required') {
      window.dispatchEvent(new CustomEvent('taleos-extension-auth-required'));
    }
    if (msg.action === 'taleos_offer_unavailable') {
      clearProcessing(msg.jobId || '', true);
      window.dispatchEvent(new CustomEvent('taleos-extension-offer-unavailable', {
        detail: { jobId: msg.jobId, jobTitle: msg.jobTitle }
      }));
    }
  });

  window.addEventListener('taleos-request-test-connection', function (e) {
    const d = e.detail || {};
    if (!d.bankId || !d.email || !d.firebaseUserId) {
      const missing = [];
      if (!d.bankId) missing.push('bankId');
      if (!d.email) missing.push('email');
      if (!d.firebaseUserId) missing.push('session Taleos');
      window.dispatchEvent(new CustomEvent('taleos-test-connection-result', {
        detail: { success: false, message: `Bridge incomplet: ${missing.join(', ')}` }
      }));
      return;
    }
    chrome.runtime.sendMessage({
      action: 'test_connection',
      bankId: d.bankId,
      email: d.email,
      password: d.password || '',
      firebaseUserId: d.firebaseUserId,
      bankName: d.bankName,
      loginUrl: d.loginUrl || ''
    }).then(function (res) {
      window.dispatchEvent(new CustomEvent('taleos-test-connection-result', {
        detail: res || {}
      }));
    }).catch(function (err) {
      window.dispatchEvent(new CustomEvent('taleos-test-connection-result', {
        detail: { success: false, message: err?.message || 'Extension non disponible' }
      }));
    });
  });

  window.addEventListener('taleos-request-bridge-health', function () {
    let version = '';
    try { version = chrome?.runtime?.getManifest?.()?.version || ''; } catch (_) { }
    window.dispatchEvent(new CustomEvent('taleos-bridge-health-result', {
      detail: { ok: true, extensionId: chrome?.runtime?.id || '', version }
    }));
  });

  window.addEventListener('taleos-request-gmail-status', function () {
    chrome.runtime.sendMessage({ action: 'gmail_get_link_status' }).then(function (res) {
      window.dispatchEvent(new CustomEvent('taleos-gmail-status-result', {
        detail: res || { ok: false, message: 'Réponse vide' }
      }));
    }).catch(function (err) {
      window.dispatchEvent(new CustomEvent('taleos-gmail-status-result', {
        detail: { ok: false, message: err?.message || 'Extension non disponible' }
      }));
    });
  });

  // ─── AJOUT : Liaison Gmail directe via chrome.identity ───────────────────────
  window.addEventListener('taleos-request-gmail-link-direct', function () {
    chrome.runtime.sendMessage({ action: 'gmail_link_direct' }).then(function (res) {
      window.dispatchEvent(new CustomEvent('taleos-gmail-link-direct-result', {
        detail: res || { ok: false, message: 'Réponse vide' }
      }));
    }).catch(function (err) {
      window.dispatchEvent(new CustomEvent('taleos-gmail-link-direct-result', {
        detail: { ok: false, message: err?.message || 'Extension non disponible' }
      }));
    });
  });
  // ─────────────────────────────────────────────────────────────────────────────

  window.addEventListener('taleos-request-gmail-link-save', function (e) {
    const d = e.detail || {};
    chrome.runtime.sendMessage({
      action: 'gmail_link_save_token',
      accessToken: d.accessToken || '',
      expiresInSec: d.expiresInSec || 3600,
      gmailEmail: d.gmailEmail || ''
    }).then(function (res) {
      window.dispatchEvent(new CustomEvent('taleos-gmail-link-save-result', {
        detail: res || { ok: false, message: 'Réponse vide' }
      }));
    }).catch(function (err) {
      window.dispatchEvent(new CustomEvent('taleos-gmail-link-save-result', {
        detail: { ok: false, message: err?.message || 'Extension non disponible' }
      }));
    });
  });

  window.addEventListener('taleos-request-gmail-unlink', function () {
    chrome.runtime.sendMessage({ action: 'gmail_unlink' }).then(function (res) {
      window.dispatchEvent(new CustomEvent('taleos-gmail-unlink-result', {
        detail: res || { ok: false, message: 'Réponse vide' }
      }));
    }).catch(function (err) {
      window.dispatchEvent(new CustomEvent('taleos-gmail-unlink-result', {
        detail: { ok: false, message: err?.message || 'Extension non disponible' }
      }));
    });
  });

  window.addEventListener('taleos-request-outlook-status', function () {
    chrome.runtime.sendMessage({ action: 'outlook_get_link_status' }).then(function (res) {
      window.dispatchEvent(new CustomEvent('taleos-outlook-status-result', {
        detail: res || { ok: false, message: 'Réponse vide' }
      }));
    }).catch(function (err) {
      window.dispatchEvent(new CustomEvent('taleos-outlook-status-result', {
        detail: { ok: false, message: err?.message || 'Extension non disponible' }
      }));
    });
  });

  window.addEventListener('taleos-request-outlook-link', function (e) {
    chrome.runtime.sendMessage({
      action: 'outlook_link'
    }).then(function (res) {
      window.dispatchEvent(new CustomEvent('taleos-outlook-link-result', {
        detail: res || { ok: false, message: 'Réponse vide' }
      }));
    }).catch(function (err) {
      window.dispatchEvent(new CustomEvent('taleos-outlook-link-result', {
        detail: { ok: false, message: err?.message || 'Extension non disponible' }
      }));
    });
  });

  window.addEventListener('taleos-request-outlook-unlink', function () {
    chrome.runtime.sendMessage({ action: 'outlook_unlink' }).then(function (res) {
      window.dispatchEvent(new CustomEvent('taleos-outlook-unlink-result', {
        detail: res || { ok: false, message: 'Réponse vide' }
      }));
    }).catch(function (err) {
      window.dispatchEvent(new CustomEvent('taleos-outlook-unlink-result', {
        detail: { ok: false, message: err?.message || 'Extension non disponible' }
      }));
    });
  });

})();
