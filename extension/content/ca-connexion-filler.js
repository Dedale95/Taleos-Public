/**
 * Taleos - Remplissage formulaire connexion CA
 * S'exécute sur la page /connexion/ après navigation (le script offre est détruit)
 */
(function() {
  'use strict';
  if (!window.location.pathname.includes('connexion')) return;

  const delay = ms => new Promise(r => setTimeout(r, ms));
  const MAX_PENDING_AGE = 2 * 60 * 1000;

  function log(msg) {
    console.log(`[${new Date().toLocaleTimeString('fr-FR')}] [Taleos CA Connexion] ${msg}`);
  }

  function forceFillInput(input, value) {
    if (!input || value == null) return false;
    input.focus();
    input.select();
    try {
      const nativeSetter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set;
      if (nativeSetter) nativeSetter.call(input, value);
      else input.value = value;
      ['input', 'change', 'keyup', 'blur'].forEach(ev => {
        input.dispatchEvent(new Event(ev, { bubbles: true }));
      });
      return true;
    } catch (e) {
      input.value = value;
      input.dispatchEvent(new Event('input', { bubbles: true }));
      return true;
    }
  }

  function findLoginInputs() {
    const selectors = [
      ['#form-login-email', '#form-login-password', '#form-login-submit'],
      ['input[id*="login-email"]', 'input[id*="login-password"]', 'button[id*="login-submit"]'],
      ['input[type="email"]', 'input[type="password"]', 'button[type="submit"]']
    ];
    for (const [eSel, pSel, sSel] of selectors) {
      const e = document.querySelector(eSel);
      const p = document.querySelector(pSel);
      const s = document.querySelector(sSel);
      if (e && p) return { email: e, pass: p, submit: s };
    }
    return null;
  }

  async function waitForForm(maxWait = 8000) {
    const start = Date.now();
    while (Date.now() - start < maxWait) {
      const inputs = findLoginInputs();
      if (inputs?.email && inputs.email.offsetParent !== null) return inputs;
      await delay(300);
    }
    return null;
  }

  async function run() {
    const { taleos_pending_offer } = await chrome.storage.local.get('taleos_pending_offer');
    if (!taleos_pending_offer) return;
    const age = Date.now() - (taleos_pending_offer.timestamp || 0);
    if (age > MAX_PENDING_AGE) {
      chrome.storage.local.remove('taleos_pending_offer');
      return;
    }
    const { offerUrl, profile } = taleos_pending_offer;
    if (!profile?.auth_email || !profile?.auth_password) {
      log('❌ Identifiants manquants dans pending_offer');
      chrome.storage.local.remove('taleos_pending_offer');
      return;
    }
    log('📧 Remplissage formulaire connexion...');
    const inputs = await waitForForm();
    if (!inputs) {
      log('❌ Formulaire non trouvé après 8s');
      return;
    }
    forceFillInput(inputs.email, profile.auth_email);
    await delay(200);
    forceFillInput(inputs.pass, profile.auth_password);
    await delay(300);
    if (inputs.submit) {
      log('✅ Envoi login...');
      inputs.submit.click();
      await delay(500);
      chrome.runtime.sendMessage({
        action: 'after_login_submit',
        offerUrl,
        bankId: 'credit_agricole',
        profile
      });
    } else {
      log('⚠️ Bouton submit non trouvé');
    }
  }

  run().catch(e => console.error('[Taleos CA Connexion]', e));
})();
