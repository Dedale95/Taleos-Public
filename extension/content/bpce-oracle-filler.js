/**
 * Taleos - Remplissage formulaire BPCE Oracle Cloud (ekez.fa.em2.oraclecloud.com)
 * Étape 2 : Email (Firebase) → CGU cochée → Suivant
 */
(function() {
  'use strict';

  const MAX_PENDING_AGE = 10 * 60 * 1000;
  const BANNER_ID = 'taleos-bpce-oracle-banner';

  const STEP = (n, msg) => `[STEP ${n}] ${msg}`;
  function log(msg, stepNum) {
    const prefix = stepNum != null ? STEP(stepNum, '') : '';
    console.log(`[${new Date().toLocaleTimeString('fr-FR')}] [Taleos BPCE Oracle] ${prefix}${msg}`);
  }

  function showBanner() {
    if (document.getElementById(BANNER_ID)) return;
    const banner = document.createElement('div');
    banner.id = BANNER_ID;
    banner.textContent = '⏳ Automatisation Taleos en cours — Ne touchez à rien.';
    Object.assign(banner.style, {
      position: 'fixed', top: '0', left: '0', right: '0', zIndex: '2147483647',
      background: 'linear-gradient(135deg, #003366 0%, #0055a4 100%)', color: 'white',
      padding: '10px 20px', fontSize: '14px', fontWeight: '600', textAlign: 'center',
      fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
      boxShadow: '0 2px 10px rgba(0,0,0,0.2)'
    });
    document.body?.insertBefore(banner, document.body.firstChild);
  }

  function hideBanner() {
    document.getElementById(BANNER_ID)?.remove();
  }

  function fillInput(input, value) {
    if (!input || value == null || value === '') return;
    const str = String(value).trim();
    input.focus();
    const nativeSetter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set;
    if (nativeSetter) nativeSetter.call(input, str);
    else input.value = str;
    input.dispatchEvent(new Event('input', { bubbles: true }));
    input.dispatchEvent(new Event('change', { bubbles: true }));
    input.blur();
  }

  function findEmailInput() {
    return document.querySelector('#primary-email-0') ||
           document.querySelector('input[name="primary-email"]') ||
           document.querySelector('input[type="email"][aria-label*="électronique"]') ||
           document.querySelector('input[type="email"]');
  }

  function findCguCheckbox() {
    return document.querySelector('span.apply-flow-input-checkbox__button') ||
           document.querySelector('.apply-flow-input-checkbox__button');
  }

  function findNextButton() {
    const byTitle = document.querySelector('button[title="Suivant"]');
    if (byTitle) return byTitle;
    const byAria = document.querySelector('button[aria-label="Suivant"]');
    if (byAria) return byAria;
    const buttons = document.querySelectorAll('button.apply-flow-pagination__button, button[data-bind*="next-button"]');
    for (const btn of buttons) {
      if ((btn.textContent || '').trim().includes('Suivant')) return btn;
    }
    return document.querySelector('button[type="submit"]');
  }

  async function waitForElement(selectorFn, maxWait = 15000) {
    const start = Date.now();
    while (Date.now() - start < maxWait) {
      const el = selectorFn();
      if (el && el.offsetParent !== null) return el;
      await new Promise(r => setTimeout(r, 300));
    }
    return null;
  }

  async function runAutomation() {
    const { taleos_pending_bpce } = await chrome.storage.local.get('taleos_pending_bpce');
    if (!taleos_pending_bpce) {
      log('⏭️  Pas de candidature BPCE en cours (taleos_pending_bpce absent) → skip', 2);
      return;
    }

    const age = Date.now() - (taleos_pending_bpce.timestamp || 0);
    if (age > MAX_PENDING_AGE) {
      log('⏭️  Pending expiré (>10 min) → skip', 2);
      chrome.storage.local.remove(['taleos_pending_bpce', 'taleos_bpce_tab_id']);
      return;
    }

    const { profile } = taleos_pending_bpce;
    const email = (profile?.email || profile?.auth_email || '').trim();
    if (!email) {
      log('❌ Email manquant dans le profil Firebase → arrêt', 2);
      chrome.storage.local.remove(['taleos_pending_bpce', 'taleos_bpce_tab_id']);
      return;
    }

    showBanner();
    log('📋 Étape 2 Oracle Cloud : remplissage email (Firebase) → CGU cochée → Suivant', 2);
    log('   Email: ' + (email ? email.replace(/(.{2}).*(@.*)/, '$1***$2') : '—'), 2);

    const emailInput = await waitForElement(findEmailInput);
    if (!emailInput) {
      log('❌ Champ email (#primary-email-0, input[name="primary-email"]) non trouvé', 2);
      hideBanner();
      return;
    }

    fillInput(emailInput, email);
    log('   ✅ Email renseigné (nativeSetter + input/change events)', 2);

    const cguCheckbox = await waitForElement(findCguCheckbox);
    if (!cguCheckbox) {
      log('❌ Case CGU (span.apply-flow-input-checkbox__button) non trouvée', 2);
      hideBanner();
      return;
    }

    const isChecked = cguCheckbox.classList.contains('apply-flow-input-checkbox__button--checked');
    if (!isChecked) {
      cguCheckbox.click();
      log('   ✅ CGU cochée (clic sur span.apply-flow-input-checkbox__button)', 2);
      await new Promise(r => setTimeout(r, 300));
    } else {
      log('   — CGU déjà cochée', 2);
    }

    const nextBtn = findNextButton();
    if (!nextBtn || nextBtn.disabled) {
      log('❌ Bouton Suivant non trouvé ou désactivé (button[title="Suivant"])', 2);
      hideBanner();
      return;
    }

    nextBtn.click();
    log('✅ Clic sur Suivant → passage à l\'étape suivante du formulaire', 2);

    chrome.storage.local.remove(['taleos_pending_bpce', 'taleos_bpce_tab_id']);
    hideBanner();
  }

  function init() {
    if (window.__taleosBpceOracleInit) return;
    window.__taleosBpceOracleInit = true;

    chrome.storage.local.get('taleos_pending_bpce').then((s) => {
      if (s.taleos_pending_bpce) {
        setTimeout(runAutomation, 1200);
      }
    });

    chrome.storage.onChanged.addListener((changes, area) => {
      if (area === 'local' && changes.taleos_pending_bpce?.newValue) {
        setTimeout(runAutomation, 1200);
      }
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
