/**
 * Taleos - Remplissage automatique Deloitte (Workday)
 * Flux: Connexion → Utiliser ma dernière candidature → Formulaire complet
 */
(function() {
  'use strict';

  const BANNER_ID = 'taleos-deloitte-automation-banner';
  const MAX_PENDING_AGE = 10 * 60 * 1000;
  const SITE_DELOITTE_CAREERS = 'Site Deloitte Careers';

  const STEP = (n, msg) => `[STEP ${n}] ${msg}`;
  function log(msg, stepNum) {
    const prefix = stepNum != null ? STEP(stepNum, '') : '';
    console.log(`[${new Date().toLocaleTimeString('fr-FR')}] [Taleos Deloitte] ${prefix}${msg}`);
  }

  function showBanner() {
    if (document.getElementById(BANNER_ID)) return;
    const banner = document.createElement('div');
    banner.id = BANNER_ID;
    banner.textContent = '⏳ Automatisation Taleos en cours — Ne touchez à rien.';
    Object.assign(banner.style, {
      position: 'fixed', top: '0', left: '0', right: '0', zIndex: '2147483647',
      background: 'linear-gradient(135deg, #86bc25 0%, #43b02a 100%)', color: 'white',
      padding: '10px 20px', fontSize: '14px', fontWeight: '600', textAlign: 'center',
      fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
      boxShadow: '0 2px 10px rgba(0,0,0,0.2)'
    });
    document.body?.insertBefore(banner, document.body.firstChild);
  }

  function hideBanner() {
    document.getElementById(BANNER_ID)?.remove();
  }

  function fillInput(el, value) {
    if (!el || value == null || value === '') return;
    const str = String(value).trim();
    el.focus();
    const nativeSetter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set;
    if (nativeSetter) nativeSetter.call(el, str);
    else el.value = str;
    el.dispatchEvent(new Event('input', { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
    el.blur();
  }

  function fillSelect(el, value) {
    if (!el || value == null || value === '') return;
    const str = String(value).trim().toLowerCase();
    const opt = Array.from(el.options || []).find(o => {
      const v = (o.value || '').toLowerCase();
      const t = (o.textContent || '').trim().toLowerCase();
      return v === str || t === str || t.includes(str) || str.includes(t);
    });
    if (opt) {
      el.value = opt.value;
      el.dispatchEvent(new Event('change', { bubbles: true }));
    }
  }

  function findLabelAndInput(labelTexts) {
    const labels = Array.from(document.querySelectorAll('label, [data-automation-id="label"], span[role="presentation"]'));
    for (const label of labels) {
      const text = (label.textContent || '').trim();
      const match = labelTexts.some(t => text.toLowerCase().includes(t.toLowerCase()));
      if (match) {
        const forId = label.getAttribute('for');
        const input = forId ? document.getElementById(forId) : null;
        if (input) return input;
        const parent = label.closest('div[data-automation-id], div[class*="input"], li');
        if (parent) {
          const inp = parent.querySelector('input, select, textarea');
          if (inp) return inp;
        }
        const next = label.nextElementSibling || label.parentElement?.querySelector('input, select, textarea');
        if (next) return next;
      }
    }
    return null;
  }

  function findAndClickByText(texts, contextLabel) {
    const all = Array.from(document.querySelectorAll('button, a, span[role="button"], div[role="button"], [data-automation-id="promptOption"], [data-automation-id="compositeHeader"], label'));
    for (const el of all) {
      const t = (el.textContent || '').trim();
      if (texts.some(x => t.toLowerCase().includes(x.toLowerCase()))) {
        if (el.offsetParent !== null) {
          log(`[${contextLabel}] findAndClickByText: clic sur élément texte="${t}"`);
          el.click();
          return true;
        }
      }
    }
    const reuseLink = document.querySelector('a[href*="reuse"], [data-automation-id*="reuse"], [data-automation-id*="lastApplication"]');
    if (reuseLink?.offsetParent !== null) {
      log(`[${contextLabel}] findAndClickByText: clic sur lien de réutilisation (reuse/lastApplication)`);
      reuseLink.click();
      return true;
    }
    log(`[${contextLabel}] findAndClickByText: aucun élément trouvé pour textes="${texts.join(', ')}"`);
    return false;
  }

  function findSelectByLabel(labelTexts) {
    const inp = findLabelAndInput(labelTexts);
    return inp && inp.tagName === 'SELECT' ? inp : null;
  }

  function findInputByLabel(labelTexts) {
    const inp = findLabelAndInput(labelTexts);
    return inp && (inp.tagName === 'INPUT' || inp.tagName === 'TEXTAREA') ? inp : null;
  }

  /** Workday : cliquer une option de liste (menuItem / promptOption) dont le libellé correspond */
  function clickWorkdayOptionByLabelAndValue(labelKeywords, valueText) {
    const labels = Array.from(document.querySelectorAll('label, [data-automation-id="label"], span[role="presentation"]'));
    for (const label of labels) {
      const text = (label.textContent || '').trim().toLowerCase();
      if (!labelKeywords.some(k => text.includes(k.toLowerCase()))) continue;
      const container = label.closest('li, div[data-automation-id], section, [role="listbox"]') || document.body;
      const options = container.querySelectorAll('[data-automation-id="promptOption"], [data-automation-id="menuItem"], [role="option"]');
      const target = (valueText || '').trim().toLowerCase();
      for (const opt of options) {
        const t = (opt.textContent || opt.getAttribute('aria-label') || '').trim().toLowerCase();
        if (t === target || t.includes(target) || (target === 'oui' && /^oui$/i.test(t)) || (target === 'non' && /^non$/i.test(t))) {
          if (opt.offsetParent !== null) {
            opt.click();
            return true;
          }
        }
      }
    }
    return false;
  }

  async function notifyOfferUnavailable(jobId, jobTitle) {
    try {
      const { taleos_pending_tab } = await chrome.storage.local.get('taleos_pending_tab');
      let taleosTab = taleos_pending_tab;
      if (!taleosTab) {
        const tabs = await chrome.tabs.query({ url: ['*://*.taleos.co/*', '*://*.github.io/*', 'http://localhost/*'] });
        taleosTab = tabs[0]?.id;
      }
      if (taleosTab) {
        chrome.tabs.sendMessage(taleosTab, { action: 'taleos_offer_unavailable', jobId, jobTitle }).catch(() => {});
      }
      chrome.storage.local.remove('taleos_pending_deloitte');
      hideBanner();
      setTimeout(() => { try { chrome.tabs.remove(chrome.tabs.TAB_ID_NONE); } catch(_){} }, 4000);
    } catch (_) {}
  }

  async function runAutomation() {
    const { taleos_pending_deloitte, taleos_deloitte_did_login_click } = await chrome.storage.local.get(['taleos_pending_deloitte', 'taleos_deloitte_did_login_click']);
    if (!taleos_pending_deloitte) {
      log('Pending absent → skip', 0);
      return;
    }

    const age = Date.now() - (taleos_pending_deloitte.timestamp || 0);
    if (age > MAX_PENDING_AGE) {
      log('Pending expiré (>10 min) → skip', 0);
      chrome.storage.local.remove(['taleos_pending_deloitte', 'taleos_deloitte_did_login_click']);
      return;
    }

    window.__taleosDeloitteDidLoginClick = !!taleos_deloitte_did_login_click;
    const url = window.location.href;
    log('URL: ' + url.replace(/^https?:\/\/[^/]+/, ''), 0);

    const { profile, tabId } = taleos_pending_deloitte;
    const jobId = taleos_pending_deloitte.jobId || '';
    const jobTitle = taleos_pending_deloitte.jobTitle || '';
    const email = profile?.auth_email || profile?.email || '';
    const password = profile?.auth_password || '';

    // Détection "Offre introuvable"
    const pageText = (document.body?.innerText || '').toLowerCase();
    if (pageText.includes('offre introuvable') || pageText.includes('job not found') || pageText.includes('this position is no longer available') || pageText.includes('cette offre est peut-être expirée')) {
      log('Offre introuvable → notification Taleos', 0);
      await notifyOfferUnavailable(jobId, jobTitle);
      return;
    }

    if (!email || !password) {
      log('Identifiants manquants → arrêt', 0);
      chrome.storage.local.remove('taleos_pending_deloitte');
      return;
    }

    showBanner();

    // Sur deloitte.com : d'abord essayer un lien direct Workday, sinon chercher le bouton Postuler
    if (url.includes('deloitte.com') && !url.includes('myworkdayjobs.com')) {
      const workdayLink = document.querySelector('a[href*="myworkdayjobs.com"][href*="apply"], a[href*="myworkdayjobs.com"]');
      if (workdayLink?.href) {
        log('Lien Workday trouvé → redirection', 1);
        window.location.href = workdayLink.href;
        return;
      }
    }

    // Étape 1 : Page offre sans /apply → cliquer Postuler (plusieurs sélecteurs, chargement dynamique possible)
    if (!url.includes('/apply') && !url.includes('/apply/')) {
      const bySelector = document.querySelector('a.deloitte-green-button.deloitte-banner-apply-button, a[href*="/apply"], a[href*="myworkdayjobs.com"]');
      const byText = Array.from(document.querySelectorAll('a, button, [role="button"]')).find(el => {
        const t = (el.textContent || '').trim();
        const isPostuler = /^postuler(\s|$)/i.test(t) || (t.toLowerCase().includes('postuler') && t.length < 60);
        return isPostuler && el.offsetParent !== null;
      });
      const btn = bySelector || byText;
      if (btn && btn.offsetParent !== null) {
        log('Clic sur Postuler', 1);
        try { btn.click(); } catch (e) { log('Erreur clic Postuler: ' + e.message, 1); }
        setTimeout(runAutomation, 2000);
        return;
      }
      if (url.includes('deloitte.com')) {
        log('Bouton Postuler non trouvé → retry dans 2s', 1);
        maybeRetryForPostuler();
        return;
      }
    }

    // Étape 2 / 3 : Connexion d'abord (bouton ou formulaire), JAMAIS "Utiliser ma dernière candidature" avant
    const emailInput = document.querySelector('input[data-automation-id="email"]');
    const passwordInput = document.querySelector('input[data-automation-id="password"]');

    // 2a. Si le formulaire est visible → on le remplit et on envoie
    if (emailInput && passwordInput) {
      log('Formulaire connexion visible → remplissage email/mot de passe', 2);
      fillInput(emailInput, email);
      fillInput(passwordInput, password);

      const submitBtn = document.querySelector('[data-automation-id="click_filter"][aria-label="Connexion"], [aria-label="Connexion"][role="button"], button[data-automation-id="signInSubmitButton"]');
      if (submitBtn && submitBtn.offsetParent !== null) {
        log('Clic sur Connexion (soumission formulaire)', 2);
        chrome.storage.local.set({ taleos_deloitte_did_login_click: true });
        window.__taleosDeloitteDidLoginClick = true;
        try { submitBtn.click(); } catch (e) { log('Erreur clic submit: ' + e.message, 2); }
        setTimeout(runAutomation, 3000);
        return;
      }
    } else {
      // 2b. Sinon, on cherche le bouton / span "Connexion" pour afficher le formulaire
      const connexionSpan = Array.from(document.querySelectorAll('span')).find(s => /^connexion$/i.test((s.textContent || '').trim()));
      const connexionBtn = document.querySelector('[aria-label="Connexion"][role="button"], [data-automation-id="click_filter"][aria-label="Connexion"]');
      const btn = connexionSpan || connexionBtn;
      if (btn && btn.offsetParent !== null) {
        log('Clic sur bouton Connexion (affichage formulaire)', 2);
        chrome.storage.local.set({ taleos_deloitte_did_login_click: true });
        window.__taleosDeloitteDidLoginClick = true;
        try { btn.click(); } catch (e) { log('Erreur clic Connexion: ' + e.message, 2); }
        setTimeout(runAutomation, 2000);
        return;
      }
      log('Aucun bouton Connexion visible', 2);
    }

    // Étape 4 : "Utiliser ma dernière candidature" — seulement sur /apply (pas sur /apply/useMyLastApplication)
    if (!url.includes('useMyLastApplication')) {
      const hasConnexionUi = document.querySelector('input[data-automation-id="email"]') ||
        document.querySelector('input[data-automation-id="password"]') ||
        document.querySelector('[aria-label="Connexion"][role="button"], [data-automation-id="click_filter"][aria-label="Connexion"]') ||
        Array.from(document.querySelectorAll('span')).some(s => /^connexion$/i.test((s.textContent || '').trim()));

      const useLastAppBtn = document.querySelector('[data-automation-id="useMyLastApplication"]') ||
        document.querySelector('a[href*="useMyLastApplication"]') ||
        document.querySelector('a[role="button"][href*="useMyLastApplication"]');

      const didLogin = !!window.__taleosDeloitteDidLoginClick;
      log(`Connexion visible=${!!hasConnexionUi}, bouton "Utiliser ma dernière candidature"=${!!useLastAppBtn}, flag=${didLogin}`, 4);

      if (!hasConnexionUi && useLastAppBtn) {
        log('Clic sur "Utiliser ma dernière candidature"', 4);
        try {
          useLastAppBtn.scrollIntoView({ behavior: 'instant', block: 'center' });
        } catch (e) {}
        try {
          useLastAppBtn.click();
        } catch (e) {
          log('Erreur clic useMyLastApplication: ' + e.message, 4);
        }
        setTimeout(runAutomation, 2500);
        return;
      }

      if (url.includes('/apply') && !hasConnexionUi && !useLastAppBtn) {
        log('Attente bouton "Utiliser ma dernière candidature" → retry', 4);
        maybeRetryForUseLastApp();
        return;
      }
    } else {
      log('Déjà sur useMyLastApplication → remplissage formulaire', 4);
    }

    // Étape 5 : Remplir le formulaire de candidature (profil Firebase : Comment nous avez-vous connus, Avez-vous déjà travaillé pour Deloitte, etc.)
    let filled = false;

    // Comment nous avez-vous connus? → "Site Deloitte Careers"
    let hearAboutFilled = false;

    // Cas 1 : Workday searchBox spécifique
    const hearSearchBox = document.querySelector('input[data-automation-id="searchBox"][id="source--source"]');
    if (hearSearchBox && hearSearchBox.offsetParent !== null) {
      try {
        hearSearchBox.focus();
        hearSearchBox.click();
      } catch (e) {}
      const opt = document.querySelector('[data-automation-id="promptOption"][data-automation-label="Site Deloitte Careers"]');
      if (opt && opt.offsetParent !== null) {
        opt.click();
        hearAboutFilled = true;
        filled = true;
      }
    }

    // Cas 2 : label + select / input génériques
    if (!hearAboutFilled) {
      const hearAboutSelect = findSelectByLabel(['comment nous avez-vous connus', 'how did you hear about us']);
      if (hearAboutSelect) {
        fillSelect(hearAboutSelect, SITE_DELOITTE_CAREERS);
        hearAboutFilled = true;
        filled = true;
      }
      const hearAboutInput = findInputByLabel(['comment nous avez-vous connus', 'how did you hear about us']);
      if (hearAboutInput) {
        fillInput(hearAboutInput, SITE_DELOITTE_CAREERS);
        hearAboutFilled = true;
        filled = true;
      }
    }

    // Cas 3 : clic direct sur une option Workday
    if (!hearAboutFilled && clickWorkdayOptionByLabelAndValue(['comment nous avez-vous connus', 'how did you hear about us'], SITE_DELOITTE_CAREERS)) {
      filled = true;
    }

    // Avez-vous déjà travaillé pour Deloitte? (valeur depuis Firebase : deloitte_worked / deloitteWorked)
    const workedRaw = profile.deloitte_worked || profile.deloitteWorked || 'no';
    const workedYesNo = workedRaw === 'yes' ? 'Oui' : 'Non';
    log('Avez-vous déjà travaillé pour Deloitte? → ' + workedYesNo + ' (Firebase: ' + workedRaw + ')', 5);
    const workedSelect = findSelectByLabel(['avez-vous déjà travaillé pour deloitte', 'have you worked for deloitte']);
    if (workedSelect) {
      fillSelect(workedSelect, workedYesNo);
      filled = true;
    }
    const workedRadioValues = workedRaw === 'yes' ? ['yes', '1', 'oui', 'true'] : ['no', '0', 'non', 'false'];
    const workedRadios = document.querySelectorAll('input[type="radio"][name*="worked"], input[type="radio"][name*="deloitte"], input[type="radio"][name*="previous"]');
    for (const r of workedRadios) {
      const v = (r.value || '').toLowerCase();
      if (workedRadioValues.some(x => v.includes(x))) {
        r.checked = true;
        r.dispatchEvent(new Event('change', { bubbles: true }));
        filled = true;
        break;
      }
    }

    // Cas explicite Workday : radios candidateIsPreviousWorker
    if (!filled) {
      const selector = workedRaw === 'yes'
        ? 'input[name="candidateIsPreviousWorker"][type="radio"][value="true"]'
        : 'input[name="candidateIsPreviousWorker"][type="radio"][value="false"]';
      const radio = document.querySelector(selector);
      if (radio && radio.offsetParent !== null) {
        radio.click();
        filled = true;
      }
    }

    if (!filled && clickWorkdayOptionByLabelAndValue(['avez-vous déjà travaillé pour deloitte', 'have you worked for deloitte'], workedYesNo)) {
      filled = true;
    }

    // Si oui : ancien bureau, ancienne adresse email, pays
    if (workedRaw === 'yes') {
      const oldOffice = findInputByLabel(['votre ancien bureau', 'your previous office', 'ancien bureau']);
      if (oldOffice && profile.deloitte_old_office) {
        fillInput(oldOffice, profile.deloitte_old_office);
        filled = true;
      }
      const oldEmail = findInputByLabel(['votre ancienne adresse email', 'your previous email', 'ancienne adresse email']);
      if (oldEmail && profile.deloitte_old_email) {
        fillInput(oldEmail, profile.deloitte_old_email);
        filled = true;
      }
      const countryInput = findInputByLabel(['pays', 'country']);
      const countrySelect = findSelectByLabel(['pays', 'country']);
      const countryVal = profile.deloitte_country || profile.country || '';
      if (countryVal) {
        if (countryInput) fillInput(countryInput, countryVal);
        if (countrySelect) fillSelect(countrySelect, countryVal);
        filled = true;
      }
    }

    // Titre (civilité) : Madame ou Monsieur
    const titleSelect = findSelectByLabel(['titre', 'title', 'nom légal']);
    if (titleSelect && profile.civility) {
      fillSelect(titleSelect, profile.civility);
      filled = true;
    }

    // Prénom(s)
    const firstnameInput = findInputByLabel(['prénom', 'first name', 'prénoms']);
    if (firstnameInput && profile.firstname) {
      fillInput(firstnameInput, profile.firstname);
      filled = true;
    }

    // Nom de famille
    const lastnameInput = findInputByLabel(['nom de famille', 'last name', 'nom']);
    if (lastnameInput && profile.lastname) {
      fillInput(lastnameInput, profile.lastname);
      filled = true;
    }

    // Nature et nom de la voie (adresse)
    const addressInput = findInputByLabel(['nature et nom de la voie', 'address', 'adresse', 'street']);
    if (addressInput && profile.address) {
      fillInput(addressInput, profile.address);
      filled = true;
    }

    // Ville
    const cityInput = findInputByLabel(['ville', 'city']);
    if (cityInput && profile.city) {
      fillInput(cityInput, profile.city);
      filled = true;
    }

    // Code postal
    const zipInput = findInputByLabel(['code postal', 'postal code', 'zip']);
    if (zipInput && profile.zipcode) {
      fillInput(zipInput, profile.zipcode);
      filled = true;
    }

    // Type d'appareil téléphonique → Mobile Personnel
    const phoneTypeSelect = findSelectByLabel(['type d\'appareil téléphonique', 'phone type', 'type d\'appareil']);
    if (phoneTypeSelect) {
      fillSelect(phoneTypeSelect, 'Mobile Personnel');
      filled = true;
    }

    // Indicatif de pays
    const countryCodeInput = findInputByLabel(['indicatif de pays', 'country code', 'country dial']);
    if (countryCodeInput && profile.phone_country_code) {
      fillInput(countryCodeInput, profile.phone_country_code);
      filled = true;
    }

    // Numéro de téléphone
    const phoneInput = findInputByLabel(['numéro de téléphone', 'phone number', 'téléphone']);
    if (phoneInput && (profile.phone_number || profile['phone-number'] || profile.phone)) {
      fillInput(phoneInput, profile.phone_number || profile['phone-number'] || profile.phone);
      filled = true;
    }

    if (filled) {
      log('Champs remplis → réessai dans 2s', 5);
      setTimeout(runAutomation, 2000);
      return;
    }

    if (url.includes('/apply') && (emailInput || filled)) {
      log('Formulaire en cours → pending conservé', 5);
      setTimeout(runAutomation, 3000);
      return;
    }

    chrome.storage.local.remove(['taleos_pending_deloitte', 'taleos_deloitte_did_login_click']);
    setTimeout(hideBanner, 2000);
  }

  let runCount = 0;
  const MAX_RETRIES = 8;
  let postulerRetryCount = 0;
  const MAX_POSTULER_RETRIES = 6;

  function maybeRetryForPostuler() {
    if (postulerRetryCount >= MAX_POSTULER_RETRIES) return;
    if (!window.location.href.includes('deloitte.com') || window.location.href.includes('myworkdayjobs.com')) return;
    postulerRetryCount++;
    log('Retry ' + postulerRetryCount + '/' + MAX_POSTULER_RETRIES + ' (attente bouton Postuler)', 1);
    setTimeout(runAutomation, 2000);
  }

  function scheduleRun(delay) {
    chrome.storage.local.get('taleos_pending_deloitte').then((s) => {
      if (s.taleos_pending_deloitte) {
        runCount = 0;
        setTimeout(runAutomation, delay || 1500);
      }
    });
  }

  chrome.storage.onChanged.addListener((changes, area) => {
    if (area === 'local' && changes.taleos_pending_deloitte?.newValue) {
      runCount = 0;
      postulerRetryCount = 0;
      setTimeout(runAutomation, 1000);
    }
  });

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => scheduleRun(2000));
  } else {
    scheduleRun(1500);
  }

  window.addEventListener('pageshow', function(ev) {
    if (ev.persisted) scheduleRun(2000);
  });

  // Retry si on est sur /apply (URL de base) et que le bouton "Utiliser ma dernière candidature" n'est pas encore chargé
  function maybeRetryForUseLastApp() {
    const href = window.location.href;
    if (!href.includes('/apply') || href.includes('useMyLastApplication')) return;
    if (runCount >= MAX_RETRIES) return;
    runCount++;
    log('Retry ' + runCount + '/' + MAX_RETRIES + ' (attente bouton)', 4);
    setTimeout(runAutomation, 2000);
  }
})();
