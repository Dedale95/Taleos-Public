/**
 * Taleos - Remplissage formulaire connexion CA
 * S'exécute sur la page /connexion/ ou /login/ après navigation (FR/EN)
 * Gère aussi candidature-validee (succès) et admin-ajax.php
 */
(function() {
  'use strict';
  const path = window.location.pathname.toLowerCase();

  const BANNER_ID = 'taleos-ca-automation-banner';
  function showAutomationBanner() {
    if (document.getElementById(BANNER_ID)) return;
    const banner = document.createElement('div');
    banner.id = BANNER_ID;
    banner.innerHTML = '⚠️ Automatisation Taleos en cours — Ne touchez à rien, cela pourrait perturber le processus.';
    Object.assign(banner.style, {
      position: 'fixed', top: '0', left: '0', right: '0', zIndex: '2147483647',
      background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)', color: 'white',
      padding: '10px 20px', fontSize: '14px', fontWeight: '600', textAlign: 'center',
      fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
      boxShadow: '0 2px 10px rgba(0,0,0,0.2)'
    });
    const root = document.body || document.documentElement;
    if (root) (root.firstChild ? root.insertBefore(banner, root.firstChild) : root.appendChild(banner));
  }

  if (path.includes('candidature-validee')) {
    showAutomationBanner();
    const m = path.match(/candidature-validee\/([^/]+)/);
    const jobId = m ? m[1] : null;
    function trySendSuccess(retries = 0) {
      if (retries > 20) return;
      chrome.storage.local.get(['taleos_success_pending', 'taleos_redirect_fallback']).then((s) => {
        const pending = s.taleos_success_pending;
        const offerUrl = pending?.offerUrl || s.taleos_redirect_fallback;
        if (jobId && offerUrl) {
          chrome.runtime.sendMessage({
            action: 'candidature_success',
            jobId,
            jobTitle: pending?.jobTitle || '',
            companyName: pending?.companyName || 'Crédit Agricole',
            offerUrl
          });
          chrome.storage.local.remove('taleos_success_pending');
        } else if (jobId && retries < 20) {
          setTimeout(() => trySendSuccess(retries + 1), 300);
        }
      });
    }
    trySendSuccess();
    return;
  }

  if (path.includes('/candidature/') || path.includes('/application/') || path.includes('/apply/') ||
      path.includes('/nos-offres-emploi/') || path.includes('/our-offers/') || path.includes('/our-offres/')) {
    chrome.storage.local.get(['taleos_pending_offer', 'taleos_redirect_fallback', 'taleos_pending_tab']).then((s) => {
      if (s.taleos_pending_offer || s.taleos_redirect_fallback || s.taleos_pending_tab) showAutomationBanner();
    });
  }

  if (path.includes('admin-ajax')) {
    const delay = ms => new Promise(r => setTimeout(r, ms));
    chrome.storage.local.get(['taleos_pending_offer', 'taleos_redirect_fallback']).then(async (s) => {
      const url = s.taleos_pending_offer?.offerUrl || s.taleos_redirect_fallback;
      if (url) {
        await delay(8000);
        window.location.replace(url);
      }
    });
    return;
  }
  if (!path.includes('connexion') && !path.includes('login') && !path.includes('connection')) return;

  const delay = ms => new Promise(r => setTimeout(r, ms));
  const MAX_PENDING_AGE = 2 * 60 * 1000;

  function log(msg) {
    console.log(`[${new Date().toLocaleTimeString('fr-FR')}] [Taleos CA Connexion] ${msg}`);
  }

  function hideAutomationBanner() {
    document.getElementById(BANNER_ID)?.remove();
  }

  function dismissCookieBanner() {
    const acceptFirst = document.querySelector('button.rgpd-btn-accept, button[class*="rgpd"][class*="accept"]') ||
      Array.from(document.querySelectorAll('button')).find(b => /^accepter|^accept|tout accepter|accept all/i.test((b.textContent || '').trim()));
    const btn = acceptFirst || document.querySelector('button.rgpd-btn-refuse, button[class*="rgpd"]') ||
      Array.from(document.querySelectorAll('button')).find(b => /accepter|refuser|accept|refuse|fermer|close/i.test(b.textContent || ''));
    if (btn && btn.offsetParent !== null) {
      btn.click();
      return true;
    }
    return false;
  }

  /** Remplissage direct (copier-coller) - résout les problèmes AJAX */
  function fillInput(input, value) {
    if (!input || value == null) return;
    const str = String(value);
    input.focus();
    const nativeSetter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set;
    if (nativeSetter) nativeSetter.call(input, str);
    else input.value = str;
    input.dispatchEvent(new Event('input', { bubbles: true }));
    input.dispatchEvent(new Event('change', { bubbles: true }));
    input.blur();
  }

  function findLoginInputs() {
    const selectors = [
      ['#form-login-email', '#form-login-password', '#form-login-submit'],
      ['input[id*="login-email"]', 'input[id*="login-password"]', 'button[id*="login-submit"]'],
      ['input[name*="email"]', 'input[name*="password"]', 'button[type="submit"]'],
      ['input[type="email"]', 'input[type="password"]', 'button[type="submit"]'],
      ['input[type="email"]', 'input[type="password"]', 'input[type="submit"]']
    ];
    for (const [eSel, pSel, sSel] of selectors) {
      const e = document.querySelector(eSel);
      const p = document.querySelector(pSel);
      let s = document.querySelector(sSel);
      if (!s && e) {
        const form = e.closest('form');
        if (form) {
          s = form.querySelector('button[type="submit"], input[type="submit"]');
          if (!s) {
            const btns = form.querySelectorAll('button, input[type="submit"]');
            s = Array.from(btns).find(b => /connexion|se connecter|login|sign in|connect/i.test((b.value || b.textContent || '').trim()));
          }
        }
      }
      if (e && p) return { email: e, pass: p, submit: s };
    }
    return null;
  }

  async function waitForForm(maxWait = 12000) {
    const start = Date.now();
    while (Date.now() - start < maxWait) {
      const inputs = findLoginInputs();
      if (inputs?.email && inputs.email.offsetParent !== null) return inputs;
      await delay(300);
    }
    return null;
  }

  /** Attend que l'animation de chargement soit terminée (spinner, overlay, etc.) */
  async function waitForLoadingComplete(maxWait = 25000) {
    const loadingSelectors = [
      '.spinner.is-active',
      '[class*="loading"][class*="active"]',
      '[class*="spinner"][class*="active"]',
      '[class*="loader"][class*="active"]',
      '[aria-busy="true"]',
      '[class*="overlay"][class*="loading"]',
      '.page-loader',
      '[class*="page-loader"]'
    ];
    const isVisible = (el) => el && el.offsetParent !== null && getComputedStyle(el).visibility !== 'hidden' && getComputedStyle(el).opacity !== '0';
    const hasVisibleLoading = () => {
      for (const sel of loadingSelectors) {
        const els = document.querySelectorAll(sel);
        if (Array.from(els).some(isVisible)) return true;
      }
      return false;
    };
    let stableCount = 0;
    const start = Date.now();
    while (Date.now() - start < maxWait) {
      if (!hasVisibleLoading()) {
        stableCount++;
        if (stableCount >= 4) {
          log('   ✅ Animation de chargement terminée.');
          return true;
        }
      } else {
        stableCount = 0;
      }
      await delay(500);
    }
    log('   ⚠️ Timeout attente chargement (navigation quand même).');
    return false;
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
    chrome.storage.local.set({ taleos_redirect_fallback: offerUrl });
    showAutomationBanner();
    log('📧 Remplissage formulaire connexion...');
    if (dismissCookieBanner()) {
      await delay(1500);
    }
    const inputs = await waitForForm();
    if (!inputs) {
      log('❌ Formulaire non trouvé après 12s');
      return;
    }
    await delay(800);
    log('   📋 Remplissage email (copier-coller)...');
    fillInput(inputs.email, profile.auth_email);
    await delay(100);
    log('   📋 Remplissage mot de passe (copier-coller)...');
    fillInput(inputs.pass, profile.auth_password);
    const submitBtn = inputs.submit;
    if (!submitBtn) {
      log('⚠️ Bouton Connexion introuvable');
      return;
    }
    // Toujours cliquer sur le bouton "Connexion" (comme l'utilisateur manuel)
    // form.requestSubmit() déclenchait un flux AJAX différent → erreur 400 admin-ajax
    log('✅ Clic sur le bouton Connexion...');
    submitBtn.scrollIntoView({ behavior: 'instant', block: 'center' });
    await delay(200);
    submitBtn.focus();
    await delay(100);
    submitBtn.click();
    log('⏳ Attente fin authentification (disparition chargement)...');
    await delay(3000);
    await waitForLoadingComplete(25000);
    log('📂 Navigation vers l\'offre...');
    window.location.href = offerUrl;
  }

  run().catch(e => console.error('[Taleos CA Connexion]', e));
})();
