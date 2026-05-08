/**
 * Taleos - Remplissage automatique AXA / iCIMS
 * Portée actuelle :
 * - ouverture de l'URL apply iCIMS
 * - redirection vers le vrai login iCIMS
 * - étape identifiant iCIMS
 * - étape mot de passe iCIMS
 * - fallback sur l'ancien écran email/consentement RGPD si AXA le réaffiche
 * - détection du formulaire candidat / succès
 *
 * La soumission finale du formulaire candidat AXA reste volontairement manuelle
 * tant que le mapping champ par champ n'est pas finalisé.
 */
(function() {
  'use strict';

  const PENDING_KEY = 'taleos_pending_axa';
  const MAX_PENDING_AGE = 20 * 60 * 1000;
  const BANNER_ID = 'taleos-axa-automation-banner';
  const SUCCESS_TEXT = 'Votre candidature a bien été transmise. Merci d\'avoir postulé.';
  const STEP_GUARD_PREFIX = 'taleos_axa_step_guard:';

  function log(message) {
    console.log(`[${new Date().toLocaleTimeString('fr-FR')}] [Taleos AXA] ${message}`);
  }

  function visible(el) {
    if (!el) return false;
    const style = window.getComputedStyle ? window.getComputedStyle(el) : null;
    const rect = typeof el.getBoundingClientRect === 'function' ? el.getBoundingClientRect() : { width: 1, height: 1 };
    if (style && (style.display === 'none' || style.visibility === 'hidden')) return false;
    return !!(rect.width || rect.height);
  }

  function stepGuardKey(step) {
    return `${STEP_GUARD_PREFIX}${step}:${window.location.pathname}:${window.location.search}`;
  }

  function hasStepGuard(step) {
    try {
      return sessionStorage.getItem(stepGuardKey(step)) === '1';
    } catch (_) {
      return false;
    }
  }

  function markStepGuard(step) {
    try {
      sessionStorage.setItem(stepGuardKey(step), '1');
    } catch (_) {}
  }

  function showBanner(text, tone) {
    const existing = document.getElementById(BANNER_ID);
    if (existing) {
      existing.textContent = text;
      existing.dataset.tone = tone || 'info';
      return;
    }
    const banner = document.createElement('div');
    banner.id = BANNER_ID;
    banner.textContent = text;
    banner.dataset.tone = tone || 'info';
    Object.assign(banner.style, {
      position: 'fixed',
      top: '0',
      left: '0',
      right: '0',
      zIndex: '2147483647',
      background: tone === 'success' ? 'linear-gradient(135deg, #0f8f57 0%, #16a34a 100%)' : 'linear-gradient(135deg, #0f1f36 0%, #133356 100%)',
      color: '#fff',
      padding: '10px 18px',
      fontSize: '14px',
      fontWeight: '600',
      textAlign: 'center',
      fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
      boxShadow: '0 2px 12px rgba(0,0,0,0.25)'
    });
    document.body?.appendChild(banner);
  }

  function detectPage() {
    const url = window.location.href;
    const host = window.location.hostname;
    const path = window.location.pathname;

    if (host.includes('careers.axa.com') && /\/careers-home\/jobs\/\d+/i.test(path)) return 'public_job';
    if (host.includes('careers-fr-axa.icims.com') && /\/jobs\/\d+\/login$/i.test(path) && !url.includes('loginOnly=1')) return 'wrapper_login';
    const visiblePassword = Array.from(document.querySelectorAll('input[type="password"], input[name="password"], input[name="passwd"]')).some(visible);
    const visibleUsername = Array.from(document.querySelectorAll('#username, input[name="username"], input[type="email"]')).some(visible);
    if (host.includes('login.icims.eu') && visiblePassword) return 'password_step';
    if (host.includes('login.icims.eu') && path.includes('/u/login/identifier') && visibleUsername) return 'identifier_step';
    if (host.includes('login.icims.eu') && path.includes('/u/login/password')) return 'password_step';
    if (host.includes('careers-fr-axa.icims.com') && document.querySelector('#enterEmailForm, input#email[name="css_loginName"]')) return 'email_step';
    if (host.includes('careers-fr-axa.icims.com') && document.querySelector('input[type="password"]')) return 'password_step';
    if (document.body && document.body.innerText && document.body.innerText.includes(SUCCESS_TEXT)) return 'success';
    if (host.includes('careers-fr-axa.icims.com') && (
      document.querySelector('input[name*="firstname" i], input[id*="firstName" i], input[name*="lastname" i], select[name*="Q383" i], select[name*="Q389" i], input[name*="desiredsalary" i], input[name*="salary" i]')
    )) return 'candidate_form';
    return 'unknown';
  }

  function textValue(el) {
    return (el?.textContent || el?.innerText || '').trim();
  }

  function fireInput(el, value) {
    if (!el) return;
    el.focus();
    el.value = value;
    el.dispatchEvent(new Event('input', { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
    el.blur();
  }

  function selectValue(select, value) {
    if (!select) return false;
    const normalizedTarget = String(value || '').trim().toLowerCase();
    const options = Array.from(select.options || []);
    const exact = options.find((opt) => String(opt.value || '').trim() === value);
    const byLabel = options.find((opt) => textValue(opt).toLowerCase() === normalizedTarget);
    const choice = exact || byLabel;
    if (!choice) return false;
    select.value = choice.value;
    select.dispatchEvent(new Event('input', { bubbles: true }));
    select.dispatchEvent(new Event('change', { bubbles: true }));
    return true;
  }

  function setCheckbox(box, checked) {
    if (!box) return;
    if (!!box.checked === !!checked) return;
    box.click();
  }

  function describeConsent(profile) {
    const optedIn = String(profile.axa_talent_pool || 'Non').trim().toLowerCase() === 'oui';
    return {
      firebase: optedIn ? 'Communauté AXA = Oui' : 'Communauté AXA = Non',
      value: optedIn ? '37002057001' : '37002057002',
      labelIncludes: optedIn ? 'opportunités futures' : 'ce poste',
      checkbox: true
    };
  }

  function compareAndFillField(label, element, targetValue) {
    if (!element) {
      log(`   ⚠️ ${label} : champ introuvable`);
      return;
    }
    const current = String(element.value || '').trim();
    const target = String(targetValue || '').trim();
    if (current === target) {
      log(`   ✅ ${label} : formulaire='${current || '(vide)'}' | Firebase='${target || '(vide)'}' -> Skip`);
      return;
    }
    log(`   ✏️ ${label} : formulaire='${current || '(vide)'}' | Firebase='${target || '(vide)'}' -> Correction`);
    fireInput(element, target);
  }

  function compareAndFillSelect(label, select, target) {
    if (!select) {
      log(`   ⚠️ ${label} : champ introuvable`);
      return;
    }
    const currentOption = select.options?.[select.selectedIndex];
    const currentText = textValue(currentOption);
    const targetText = target.firebase;
    if (currentText.toLowerCase().includes(target.labelIncludes.toLowerCase())) {
      log(`   ✅ ${label} : formulaire='${currentText || '(vide)'}' | Firebase='${targetText}' -> Skip`);
      return;
    }
    const applied = selectValue(select, target.value);
    log(`   ${applied ? '✏️' : '⚠️'} ${label} : formulaire='${currentText || '(vide)'}' | Firebase='${targetText}' -> ${applied ? 'Correction' : 'Option introuvable'}`);
  }

  async function handleWrapperLogin() {
    const iframe = document.querySelector('#icims_content_iframe[src]');
    if (!iframe?.src) {
      const loginOnlyUrl = window.location.href.includes('loginOnly=1')
        ? window.location.href
        : window.location.href.replace(/\/login(?:\?.*)?$/i, (m) => m.includes('?') ? `${m}&loginOnly=1&in_iframe=1` : '/login?loginOnly=1&in_iframe=1');
      log('🔗 AXA wrapper login : fallback direct vers loginOnly');
      window.location.replace(loginOnlyUrl);
      return;
    }
    showBanner('AXA → ouverture du vrai formulaire de candidature…');
    log(`🔗 AXA → redirection vers l’iframe iCIMS réelle`);
    window.location.replace(iframe.src);
  }

  async function handleIdentifierStep(profile) {
    showBanner('AXA → connexion en cours (identifiant)…');
    if (hasStepGuard('identifier')) {
      log('⏸️ AXA : étape identifiant déjà déclenchée sur cette URL -> attente de transition');
      return;
    }
    const emailInput = document.querySelector('#username, input[name="username"]');
    const continueButton = Array.from(document.querySelectorAll('button[type="submit"], button, input[type="submit"]')).find((el) => {
      const label = (el.value || textValue(el)).toLowerCase();
      return /continue|continuer/.test(label);
    });

    log('🧾 AXA → audit détaillé Firebase vs formulaire (étape identifiant)');
    compareAndFillField('Email', emailInput, profile.auth_email || profile.email || '');

    if (!continueButton) {
      log('⚠️ AXA → bouton Continue introuvable');
      return;
    }

    log('➡️ AXA : clic sur Continue après saisie de l’email');
    markStepGuard('identifier');
    continueButton.click();
  }

  async function handleEmailStep(profile) {
    showBanner('AXA → connexion en cours (email + consentement)…');
    if (hasStepGuard('email_step')) {
      log('⏸️ AXA : étape email AXA déjà déclenchée sur cette URL -> attente de transition');
      return;
    }
    const emailInput = document.querySelector('input#email[name="css_loginName"], #email');
    const consentSelect = document.querySelector('select[name="gdpr_consent_type"], #gdpr_consent_type');
    const consentCheckbox = document.querySelector('input#accept_gdpr[name="accept_gdpr"]');
    const submitButton = document.querySelector('#enterEmailSubmitButton, input[value="Suivant"], button[type="submit"]');

    log('🧾 AXA → audit détaillé Firebase vs formulaire (étape email)');
    compareAndFillField('Email', emailInput, profile.auth_email || profile.email || '');
    compareAndFillSelect('Consentement AXA', consentSelect, describeConsent(profile));
    if (consentCheckbox) {
      const desiredChecked = true;
      if (consentCheckbox.checked === desiredChecked) {
        log('   ✅ Validation RGPD : case déjà cochée -> Skip');
      } else {
        log('   ✏️ Validation RGPD : case non cochée -> Correction');
        setCheckbox(consentCheckbox, true);
      }
    } else {
      log('   ⚠️ Validation RGPD : case introuvable');
    }

    if (!submitButton) {
      log('⚠️ AXA → bouton "Suivant" introuvable');
      return;
    }

    log('➡️ AXA : clic sur Suivant après email/consentement');
    markStepGuard('email_step');
    submitButton.click();
  }

  async function handlePasswordStep(profile) {
    showBanner('AXA → connexion en cours (mot de passe)…');
    if (hasStepGuard('password')) {
      log('⏸️ AXA : étape mot de passe déjà déclenchée sur cette URL -> attente de transition');
      return;
    }
    const passwordInput = document.querySelector('input[type="password"], input[name="password"], input[name="passwd"]');
    const emailInput = document.querySelector('input[type="email"], input[name="username"], input[name="email"], #username');
    const submitButton = Array.from(document.querySelectorAll('button, input[type="submit"]')).find((el) => {
      const label = (el.value || textValue(el)).toLowerCase();
      return /se connecter|connexion|sign in|log in|continue|continuer|log in to axa/.test(label);
    });

    log('🧾 AXA → audit détaillé Firebase vs formulaire (étape mot de passe)');
    if (emailInput) compareAndFillField('Email', emailInput, profile.auth_email || profile.email || '');
    compareAndFillField('Mot de passe', passwordInput, profile.auth_password || '');

    if (!submitButton) {
      log('⚠️ AXA → bouton de connexion introuvable');
      return;
    }

    log('➡️ AXA : clic sur Se connecter');
    markStepGuard('password');
    submitButton.click();
  }

  async function handleCandidateForm(profile) {
    showBanner('AXA → formulaire candidat détecté. Relecture recommandée avant envoi.', 'success');
    log('🚀 AXA → formulaire candidat détecté');
    log('🧾 AXA → audit détaillé Firebase vs formulaire (pré-mapping minimal)');

    const firstName = document.querySelector('input[name*="firstname" i], input[id*="firstName" i]');
    const lastName = document.querySelector('input[name*="lastname" i], input[id*="lastName" i]');
    const email = document.querySelector('input[type="email"], input[name*="email" i]');
    const phone = document.querySelector('input[type="tel"], input[name*="phone" i]');

    compareAndFillField('Prénom', firstName, profile.firstname || '');
    compareAndFillField('Nom', lastName, profile.lastname || '');
    compareAndFillField('Email', email, profile.email || profile.auth_email || '');
    compareAndFillField('Téléphone', phone, profile['phone-number'] || profile.phone_number || '');

    const finalSubmit = Array.from(document.querySelectorAll('button, input[type="submit"]')).find((el) => {
      const label = (el.value || textValue(el)).toLowerCase();
      return /envoyer|submit|postuler/.test(label);
    });
    if (finalSubmit) {
      log('⏸️ AXA : bouton de soumission finale détecté, mais la soumission reste manuelle pour validation utilisateur');
    }
  }

  async function handleSuccess() {
    showBanner('AXA → candidature envoyée avec succès.', 'success');
    log(`✅ AXA : message de confirmation détecté -> "${SUCCESS_TEXT}"`);
    const { taleos_pending_axa } = await chrome.storage.local.get(PENDING_KEY);
    chrome.storage.local.remove(PENDING_KEY).catch(() => {});
    chrome.runtime.sendMessage({
      action: 'candidature_success',
      bankId: 'axa',
      jobId: taleos_pending_axa?.jobId || taleos_pending_axa?.profile?.__jobId || '',
      jobTitle: taleos_pending_axa?.jobTitle || taleos_pending_axa?.profile?.__jobTitle || '',
      companyName: taleos_pending_axa?.companyName || taleos_pending_axa?.profile?.__companyName || 'AXA',
      offerUrl: taleos_pending_axa?.offerUrl || taleos_pending_axa?.profile?.__offerUrl || window.location.href
    }).catch(() => {});
  }

  async function runAutomation(profile) {
    const page = detectPage();
    log(`🚀 Démarrage AXA sur ${page} (${window.location.pathname}${window.location.search})`);

    if (page === 'public_job') {
      const match = window.location.pathname.match(/\/jobs\/(\d+)/i);
      if (match) {
        const nextUrl = `https://careers-fr-axa.icims.com/jobs/${match[1]}/login?loginOnly=1&in_iframe=1`;
        showBanner('AXA → ouverture du portail de candidature…');
        log('🔗 AXA → redirection depuis la page vitrine vers le portail iCIMS');
        window.location.replace(nextUrl);
      }
      return;
    }
    if (page === 'wrapper_login') return handleWrapperLogin();
    if (page === 'identifier_step') return handleIdentifierStep(profile);
    if (page === 'email_step') return handleEmailStep(profile);
    if (page === 'password_step') return handlePasswordStep(profile);
    if (page === 'candidate_form') return handleCandidateForm(profile);
    if (page === 'success') return handleSuccess();
    log('⏭️ AXA → aucune action sur cette page');
  }

  function shouldRunPending(pending) {
    if (!pending?.profile) return false;
    const age = Date.now() - (pending.timestamp || 0);
    if (age > MAX_PENDING_AGE) return false;
    const host = window.location.hostname;
    return host.includes('careers.axa.com') || host.includes('careers-fr-axa.icims.com') || host.includes('login.icims.eu');
  }

  function init() {
    if (window.__taleosAxaInit) return;
    window.__taleosAxaInit = true;

    chrome.storage.local.get(PENDING_KEY).then(({ taleos_pending_axa }) => {
      if (!shouldRunPending(taleos_pending_axa)) return;
      setTimeout(() => runAutomation(taleos_pending_axa.profile), 900);
    });

    chrome.storage.onChanged.addListener((changes, area) => {
      if (area !== 'local' || !changes[PENDING_KEY]?.newValue) return;
      const pending = changes[PENDING_KEY].newValue;
      if (!shouldRunPending(pending)) return;
      setTimeout(() => runAutomation(pending.profile), 900);
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
