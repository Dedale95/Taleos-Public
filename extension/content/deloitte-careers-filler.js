/**
 * Taleos - Remplissage automatique Deloitte (Workday)
 * Sur fina.wd103.myworkdayjobs.com : Postuler → Connexion → fill email/password → submit
 */
(function() {
  'use strict';

  const BANNER_ID = 'taleos-deloitte-automation-banner';
  const MAX_PENDING_AGE = 5 * 60 * 1000;

  function log(msg) {
    console.log(`[${new Date().toLocaleTimeString('fr-FR')}] [Taleos Deloitte] ${msg}`);
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
    if (!el || value == null) return;
    const str = String(value);
    el.focus();
    const nativeSetter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set;
    if (nativeSetter) nativeSetter.call(el, str);
    else el.value = str;
    el.dispatchEvent(new Event('input', { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
    el.blur();
  }

  async function runAutomation() {
    const { taleos_pending_deloitte } = await chrome.storage.local.get('taleos_pending_deloitte');
    if (!taleos_pending_deloitte) return;

    const age = Date.now() - (taleos_pending_deloitte.timestamp || 0);
    if (age > MAX_PENDING_AGE) {
      chrome.storage.local.remove('taleos_pending_deloitte');
      return;
    }

    const { profile, tabId } = taleos_pending_deloitte;
    const email = profile?.auth_email || profile?.email || '';
    const password = profile?.auth_password || '';

    if (!email || !password) {
      log('Identifiants Deloitte manquants (page Connexions)');
      chrome.storage.local.remove('taleos_pending_deloitte');
      return;
    }

    showBanner();
    const url = window.location.href;

    // Sur deloitte.com : chercher un lien vers myworkdayjobs.com/apply et y naviguer
    if (url.includes('deloitte.com') && !url.includes('myworkdayjobs.com')) {
      const workdayLink = document.querySelector('a[href*="myworkdayjobs.com"][href*="apply"]');
      if (workdayLink && workdayLink.href) {
        log('Redirection vers Workday apply...');
        window.location.href = workdayLink.href;
        return;
      }
    }

    // Étape 1 : Si on est sur la page offre (sans /apply), cliquer sur Postuler
    if (!url.includes('/apply') && !url.includes('/apply/')) {
      const postulerBtn = document.querySelector('a.deloitte-green-button.deloitte-banner-apply-button');
      if (postulerBtn && postulerBtn.offsetParent !== null) {
        log('Clic sur Postuler...');
        postulerBtn.click();
        return;
      }
      const postulerByHref = document.querySelector('a[href*="/apply"]');
      if (postulerByHref && postulerByHref.offsetParent !== null) {
        log('Clic sur Postuler (lien /apply)...');
        postulerByHref.click();
        return;
      }
      const postulerLink = Array.from(document.querySelectorAll('a')).find(a =>
        /^postuler$/i.test((a.textContent || '').trim())
      );
      if (postulerLink) {
        log('Clic sur Postuler (fallback)...');
        postulerLink.click();
        return;
      }
    }

    // Étape 2 : Sur la page apply - cliquer Connexion si le formulaire login n'est pas visible
    const emailInput = document.querySelector('input[data-automation-id="email"]');
    const passwordInput = document.querySelector('input[data-automation-id="password"]');

    if (!emailInput || !passwordInput) {
      const connexionSpan = Array.from(document.querySelectorAll('span.css-1xtbc5b, span')).find(s =>
        /^connexion$/i.test((s.textContent || '').trim())
      );
      if (connexionSpan && connexionSpan.offsetParent !== null) {
        log('Clic sur Connexion...');
        connexionSpan.click();
        setTimeout(runAutomation, 1500);
        return;
      }
      const connexionBtn = document.querySelector('[aria-label="Connexion"][role="button"], [data-automation-id="click_filter"][aria-label="Connexion"], div[aria-label="Connexion"][role="button"]');
      if (connexionBtn && connexionBtn.offsetParent !== null) {
        log('Clic sur Connexion (aria-label)...');
        connexionBtn.click();
        setTimeout(runAutomation, 1500);
        return;
      }
    }

    // Étape 3 : Remplir email et mot de passe
    if (emailInput && passwordInput) {
      fillInput(emailInput, email);
      fillInput(passwordInput, password);

      const submitBtn = document.querySelector('[data-automation-id="click_filter"][aria-label="Connexion"], [aria-label="Connexion"][role="button"], div[aria-label="Connexion"][role="button"]');
      if (submitBtn && submitBtn.offsetParent !== null) {
        log('Clic sur Connexion (submit)...');
        submitBtn.click();
        chrome.storage.local.remove('taleos_pending_deloitte');
        setTimeout(hideBanner, 3000);
        return;
      }
      const signInBtn = document.querySelector('button[data-automation-id="signInSubmitButton"]');
      if (signInBtn) {
        log('Clic sur signInSubmitButton...');
        signInBtn.click();
        chrome.storage.local.remove('taleos_pending_deloitte');
        setTimeout(hideBanner, 3000);
        return;
      }
    }
  }

  chrome.storage.local.get('taleos_pending_deloitte').then((s) => {
    if (s.taleos_pending_deloitte) {
      setTimeout(runAutomation, 1500);
    }
  });

  chrome.storage.onChanged.addListener((changes, area) => {
    if (area === 'local' && changes.taleos_pending_deloitte?.newValue) {
      setTimeout(runAutomation, 1000);
    }
  });

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => setTimeout(runAutomation, 2000));
  }
})();
