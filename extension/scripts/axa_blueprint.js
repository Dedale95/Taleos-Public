/**
 * Taleos - Blueprint AXA
 * Cartographie le flux careers.axa.com / careers-fr-axa.icims.com observé en production.
 */
(function () {
  'use strict';

  if (globalThis.__TALEOS_AXA_BLUEPRINT__) return;

  const LAST_CHECK_KEY = 'taleos_axa_blueprint_last_check';
  const LOG_KEY = 'taleos_axa_blueprint_log';
  const MAX_LOG_ENTRIES = 120;

  const TEXT = {
    offerPublic: ['postuler', 'description du poste', 'ce que vous apporterez'],
    loginIdentifier: ['enter your email', 'continue', 'continue with axa'],
    loginPassword: ['enter your password', 'log in', 'wrong username or password'],
    profile: ['profil du candidat', 'step 1 of 3', 'veuillez télécharger votre cv'],
    questions: ['questions supplémentaires requises pour le poste', 'step 2 of 3', 'salary expectations'],
    success: ['votre candidature a bien ete transmise', 'merci d avoir postule'],
    dashboard: ['tableau de bord', 'se deconnecter']
  };

  const PAGE_DEFS = {
    offer_public: {
      label: 'Offre publique AXA',
      hostIncludes: ['careers.axa.com'],
      pathMatches: [/\/careers-home\/jobs\//],
      selectorsAny: ['a[href*="icims.com/jobs/"][href*="/login"]', 'button', 'h1'],
      textPatterns: TEXT.offerPublic
    },
    email_step: {
      label: 'Login iCIMS AXA - email + GDPR',
      hostIncludes: ['careers-en-axa.icims.com'],
      pathMatches: [/\/jobs\/\d+\/login/],
      selectorsAny: ['#email', '#gdpr_consent_type', '#accept_gdpr', '#enterEmailSubmitButton'],
      textPatterns: ['do you want to be a part of the axa community', 'i acknowledge the privacy notice', 'next']
    },
    login_identifier: {
      label: 'Login iCIMS - email',
      hostIncludes: ['login.icims.eu'],
      pathMatches: [/\/u\/login\/identifier/],
      selectorsAny: ['#username', 'button[type="submit"]'],
      textPatterns: TEXT.loginIdentifier
    },
    login_password: {
      label: 'Login iCIMS - mot de passe',
      hostIncludes: ['login.icims.eu'],
      pathMatches: [/\/u\/login\/password/],
      selectorsAny: ['input[type="password"]', 'button[type="submit"]'],
      textPatterns: TEXT.loginPassword
    },
    apply_wrapper: {
      label: 'Wrapper iCIMS AXA',
      hostIncludes: ['careers-fr-axa.icims.com'],
      pathMatches: [/\/job\?mode=submit_apply/, /\/candidate\?.*mobile=false/],
      selectorsAny: ['#icims_content_iframe', 'iframe[src*="in_iframe=1"]'],
      textPatterns: ['politique de confidentialite', 'informations legales']
    },
    profile: {
      label: 'Profil du candidat',
      hostIncludes: ['careers-fr-axa.icims.com'],
      pathMatches: [/\/candidate/, /mode=apply/, /in_iframe=1/],
      selectorsAny: ['#PortalProfileFields.Resume_Button', '#PersonProfileFields.FirstName', '#cp_form_submit_i', '#634773_PersonProfileFields.PhoneNumber'],
      textPatterns: TEXT.profile
    },
    questions: {
      label: 'Questions supplémentaires',
      hostIncludes: ['careers-fr-axa.icims.com'],
      pathMatches: [/\/questions/],
      selectorsAny: ['#Q389', '#Q383', '#Q388', '#Q181', '#quesp_form_submit_i'],
      textPatterns: TEXT.questions
    },
    success: {
      label: 'Succès explicite',
      hostIncludes: ['careers-fr-axa.icims.com'],
      pathMatches: [/mode=submit_apply/],
      selectorsAny: ['body', 'main', '.iCIMS_ApplyContainer'],
      textPatterns: TEXT.success
    }
  };

  function normalizeText(value) {
    return String(value || '')
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/['’]/g, ' ')
      .toLowerCase()
      .replace(/\s+/g, ' ')
      .trim();
  }

  function getPageText(doc = document) {
    return normalizeText(doc.body?.innerText || doc.body?.textContent || '');
  }

  function isVisible(el) {
    if (!el) return false;
    const style = globalThis.getComputedStyle ? getComputedStyle(el) : null;
    const rect = typeof el.getBoundingClientRect === 'function' ? el.getBoundingClientRect() : null;
    return !!rect && rect.width > 0 && rect.height > 0 && style?.display !== 'none' && style?.visibility !== 'hidden';
  }

  function queryVisible(doc, selector) {
    try {
      return Array.from(doc.querySelectorAll(selector)).find(isVisible) || null;
    } catch (_) {
      return null;
    }
  }

  function hostMatches(def, host) {
    return (def.hostIncludes || []).every((part) => host.includes(part));
  }

  function pathMatches(def, pathname, href) {
    return (def.pathMatches || []).some((re) => re.test(pathname) || re.test(href));
  }

  function countTextMatches(text, patterns) {
    return (patterns || []).filter((pattern) => text.includes(normalizeText(pattern))).length;
  }

  function detectPage(doc = document, href = location.href) {
    const url = new URL(href, location.origin);
    const host = String(url.hostname || '').toLowerCase();
    const pathname = String(url.pathname || '').toLowerCase();
    const fullHref = String(url.href || '').toLowerCase();
    const text = getPageText(doc);

    if (text.includes(normalizeText(TEXT.success[0]))) {
      return { key: 'success', score: 100, label: PAGE_DEFS.success.label };
    }

    let best = { key: 'unknown', score: 0, label: 'Inconnue' };
    for (const [key, def] of Object.entries(PAGE_DEFS)) {
      let score = 0;
      if (hostMatches(def, host)) score += 2;
      if (pathMatches(def, pathname, fullHref)) score += 2;
      score += countTextMatches(text, def.textPatterns);
      if ((def.selectorsAny || []).some((selector) => queryVisible(doc, selector))) score += 2;
      if (score > best.score) best = { key, score, label: def.label };
    }
    return best;
  }

  function validateCurrentPage(expected) {
    const detected = detectPage();
    const targets = Array.isArray(expected) ? expected : [expected];
    const ok = targets.includes(detected.key);
    const result = {
      ok,
      detected: detected.key,
      label: detected.label,
      href: location.href,
      checkedAt: new Date().toISOString()
    };
    try {
      chrome.storage.local.set({ [LAST_CHECK_KEY]: result });
    } catch (_) {}
    return result;
  }

  function getStructureReport(expectedKey, doc = document) {
    const def = PAGE_DEFS[expectedKey];
    if (!def) return { ok: false, expectedKey, error: 'unknown blueprint page' };
    const matchedSelectors = (def.selectorsAny || []).filter((selector) => !!queryVisible(doc, selector));
    return {
      expectedKey,
      ok: matchedSelectors.length > 0,
      matchedSelectors,
      missingSelectors: (def.selectorsAny || []).filter((selector) => !matchedSelectors.includes(selector)),
      textMatches: (def.textPatterns || []).filter((pattern) => getPageText(doc).includes(normalizeText(pattern))),
      href: location.href
    };
  }

  async function recordLog(entry) {
    try {
      const out = await chrome.storage.local.get([LOG_KEY]);
      const current = Array.isArray(out[LOG_KEY]) ? out[LOG_KEY] : [];
      current.push({
        at: new Date().toISOString(),
        href: location.href,
        entry
      });
      while (current.length > MAX_LOG_ENTRIES) current.shift();
      await chrome.storage.local.set({ [LOG_KEY]: current });
    } catch (_) {}
  }

  globalThis.__TALEOS_AXA_BLUEPRINT__ = {
    detectPage,
    validateCurrentPage,
    getStructureReport,
    recordLog
  };
})();
