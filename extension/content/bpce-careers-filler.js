/**
 * Taleos - Remplissage automatique BPCE (recrutement.bpce.fr)
 * Étape 1 : Clic sur "Postuler directement" pour ouvrir le formulaire Oracle
 * (Le formulaire email/CGU/Suivant est géré par bpce-oracle-filler.js sur Oracle Cloud)
 */
(function() {
  'use strict';

  const MAX_PENDING_AGE = 10 * 60 * 1000;
  const BANNER_ID = 'taleos-bpce-automation-banner';

  const STEP = (n, msg) => `[STEP ${n}] ${msg}`;
  function log(msg, stepNum) {
    const prefix = stepNum != null ? STEP(stepNum, '') : '';
    console.log(`[${new Date().toLocaleTimeString('fr-FR')}] [Taleos BPCE] ${prefix}${msg}`);
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

  function findPostulerButton() {
    const links = document.querySelectorAll('a[href*="oraclecloud.com"][href*="apply"], a[title="Postuler"], a.c-button--big, a.c-offer-sticky-button');
    for (const a of links) {
      const text = (a.textContent || '').trim();
      if (/postuler|postulez|candidater/i.test(text)) return a;
    }
    return document.querySelector('a[href*="oraclecloud.com"][href*="apply"]');
  }

  async function waitForElement(selectorFn, maxWait = 8000) {
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
      log('⏭️  Pas de candidature BPCE en cours (taleos_pending_bpce absent) → skip', 1);
      return;
    }

    const age = Date.now() - (taleos_pending_bpce.timestamp || 0);
    if (age > MAX_PENDING_AGE) {
      log('⏭️  Pending expiré (>10 min) → skip', 1);
      chrome.storage.local.remove(['taleos_pending_bpce', 'taleos_bpce_tab_id']);
      return;
    }

    const isOfferPage = /\/job\//.test(window.location.pathname || '');
    if (!isOfferPage) {
      log('⏭️  Pas sur une page offre (/job/) → skip', 1);
      return;
    }

    showBanner();
    log('📋 Étape 1 recrutement.bpce.fr : clic sur "Postuler directement" pour ouvrir le formulaire Oracle', 1);
    log('   Recherche du bouton (a[href*="oraclecloud.com"], a.c-button--big, a.c-offer-sticky-button)...', 1);

    const postulerBtn = await waitForElement(findPostulerButton);
    if (!postulerBtn) {
      log('❌ Bouton Postuler non trouvé', 1);
      hideBanner();
      return;
    }

    log('✅ Clic sur "Postuler directement" → ouverture formulaire Oracle dans nouvel onglet', 1);
    postulerBtn.click();
    hideBanner();
  }

  function init() {
    if (window.__taleosBpceInit) return;
    window.__taleosBpceInit = true;

    chrome.storage.local.get('taleos_pending_bpce').then((s) => {
      if (s.taleos_pending_bpce) {
        setTimeout(runAutomation, 800);
      }
    });

    chrome.storage.onChanged.addListener((changes, area) => {
      if (area === 'local' && changes.taleos_pending_bpce?.newValue) {
        setTimeout(runAutomation, 800);
      }
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
