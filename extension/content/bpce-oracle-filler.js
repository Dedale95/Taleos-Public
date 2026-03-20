/**
 * Taleos - Remplissage formulaire BPCE Oracle Cloud (ekez.fa.em2.oraclecloud.com)
 * Flux multi-étapes : Email+CGU → Code PIN → Données personnelles → Questions → Documents → Alertes
 * Version 1.0.55 : Correction des sélecteurs pour les boutons (Oui/Non) et textarea avec espaces
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
    const nativeSetter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set || 
                         Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value')?.set;
    if (nativeSetter) nativeSetter.call(input, str);
    else input.value = str;
    input.dispatchEvent(new Event('input', { bubbles: true }));
    input.dispatchEvent(new Event('change', { bubbles: true }));
    input.blur();
  }

  function clickButtonByText(textToFind, container = document) {
    // On cherche tous les boutons et les spans de texte (Oracle met souvent le texte dans un span)
    const elements = container.querySelectorAll('button, .cx-select-pill-section, .cx-select-pill-name, [role="button"]');
    const target = String(textToFind || '').trim().toLowerCase();
    
    for (const el of elements) {
      const elText = (el.textContent || '').trim().toLowerCase();
      if (elText === target) {
        // Si on a trouvé le span, on clique sur le bouton parent
        const btn = el.closest('button') || el.closest('.cx-select-pill-section') || el;
        const isSelected = btn.classList.contains('cx-select-pill-section--selected') || 
                           btn.getAttribute('aria-pressed') === 'true' ||
                           btn.getAttribute('aria-checked') === 'true';
        if (!isSelected) {
          btn.click();
          return true;
        }
        return 'already_selected';
      }
    }
    return false;
  }

  async function setFileInputFromStorage(inputEl, storagePath, filename) {
    if (!inputEl || !storagePath) return false;
    try {
      const r = await new Promise((resolve) => {
        chrome.runtime.sendMessage({ action: 'fetch_storage_file', storagePath }, resolve);
      });
      if (r?.error) throw new Error(r.error);
      if (!r?.base64) return false;
      const bin = atob(r.base64);
      const arr = new Uint8Array(bin.length);
      for (let i = 0; i < bin.length; i++) arr[i] = bin.charCodeAt(i);
      const blob = new Blob([arr], { type: r.type || 'application/pdf' });
      const file = new File([blob], filename || 'cv.pdf', { type: blob.type });
      const dt = new DataTransfer();
      dt.items.add(file);
      inputEl.files = dt.files;
      inputEl.dispatchEvent(new Event('change', { bubbles: true }));
      inputEl.dispatchEvent(new Event('input', { bubbles: true }));
      return true;
    } catch (e) {
      log('   ❌ Erreur upload: ' + (e?.message || e), 2);
      return false;
    }
  }

  function findNextButton() {
    const byTitle = document.querySelector('button[title="Suivant"]');
    if (byTitle) return byTitle;
    const buttons = document.querySelectorAll('button');
    for (const btn of buttons) {
      const txt = (btn.textContent || '').trim();
      if (txt.includes('Suivant') || txt === 'SUIVANT') return btn;
    }
    return document.querySelector('button[type="submit"]');
  }

  async function runAutomation() {
    const { taleos_pending_bpce } = await chrome.storage.local.get('taleos_pending_bpce');
    if (!taleos_pending_bpce) return;

    const age = Date.now() - (taleos_pending_bpce.timestamp || 0);
    if (age > MAX_PENDING_AGE) {
      chrome.storage.local.remove(['taleos_pending_bpce', 'taleos_bpce_tab_id']);
      return;
    }

    const { profile } = taleos_pending_bpce;
    const email = (profile?.email || profile?.auth_email || '').trim();
    if (!email) return;

    showBanner();

    // --- Étape 1 : Email + CGU ---
    const emailInput = document.querySelector('#primary-email-0') || document.querySelector('input[type="email"]');
    if (emailInput && emailInput.offsetParent !== null && !document.querySelector('#pin-code-1')) {
      log('📋 Étape 1 : Email + CGU', 2);
      fillInput(emailInput, email);
      const cgu = document.querySelector('span.apply-flow-input-checkbox__button') || document.querySelector('.apply-flow-input-checkbox__button');
      if (cgu && !cgu.classList.contains('apply-flow-input-checkbox__button--checked')) {
        cgu.click();
      }
      const nextBtn = findNextButton();
      if (nextBtn) nextBtn.click();
      return;
    }

    // --- Étape 1b : Code PIN ---
    const pinInput1 = document.querySelector('#pin-code-1');
    if (pinInput1 && pinInput1.offsetParent !== null) {
      log('📋 Étape 1b : Vérification d\'identité (Code PIN)', 2);
      const { taleos_bpce_pin_code } = await chrome.storage.local.get('taleos_bpce_pin_code');
      if (taleos_bpce_pin_code) {
        const pinStr = String(taleos_bpce_pin_code).trim();
        if (pinStr.length === 6) {
          log('   📌 Code PIN trouvé → remplissage automatique', 2);
          for (let i = 0; i < 6; i++) {
            fillInput(document.querySelector(`#pin-code-${i + 1}`), pinStr[i]);
          }
          setTimeout(() => {
            const verifyBtn = Array.from(document.querySelectorAll('button')).find(b => b.textContent.includes('VÉRIFIER'));
            if (verifyBtn) verifyBtn.click();
          }, 500);
        }
      } else {
        log('   ⏳ En attente du code PIN...', 2);
      }
      return;
    }

    // --- Étape 2 : Données personnelles ---
    const lastNameInput = document.querySelector('input[id*="lastName"]');
    const firstNameInput = document.querySelector('input[id*="firstName"]');
    if (lastNameInput && lastNameInput.offsetParent !== null) {
      log('📋 Étape 2 : Données personnelles', 2);
      fillInput(lastNameInput, profile.last_name || profile.lastname);
      fillInput(firstNameInput, profile.first_name || profile.firstname);
      
      const civility = (profile.civility || '').toLowerCase();
      if (civility.includes('monsieur')) clickButtonByText('M.');
      else if (civility.includes('madame')) clickButtonByText('Mme');

      const phoneInput = document.querySelector('input[type="tel"]');
      if (phoneInput) fillInput(phoneInput, profile.phone || profile.phone_number);

      const countryDropdown = document.querySelector('input[id*="country-codes-dropdown"]');
      if (countryDropdown) fillInput(countryDropdown, profile.phone_country_code || '+33');

      // --- Étape 3 : Questions ---
      log('📋 Étape 3 : Questions de candidature', 2);
      
      // Question Handicap
      const handicapVal = (profile.bpce_handicap || 'Non').trim();
      const handicapContainer = Array.from(document.querySelectorAll('.apply-flow-block, .input-row')).find(el => el.textContent.toLowerCase().includes('handicap'));
      if (handicapContainer) {
        const ok = clickButtonByText(handicapVal, handicapContainer);
        if (ok) log(`   ✅ Handicap → ${handicapVal}`, 2);
      }

      // Disponibilité (Sélecteur ID dynamique robuste)
      const disponibiliteTextarea = document.querySelector('textarea[name="300000620007177"]') || document.querySelector('textarea[id^="300000620007177"]');
      const availableFrom = (profile.available_from || profile.available_date || '').trim();
      if (disponibiliteTextarea && availableFrom) {
        fillInput(disponibiliteTextarea, availableFrom);
        log('   ✅ Disponibilité renseignée', 2);
      }

      // Vivier Natixis
      const vivierVal = (profile.bpce_vivier_natixis || 'Oui').trim();
      const vivierContainer = Array.from(document.querySelectorAll('.apply-flow-block, .input-row')).find(el => el.textContent.toLowerCase().includes('vivier') || el.textContent.toLowerCase().includes('natixis'));
      if (vivierContainer) {
        const ok = clickButtonByText(vivierVal, vivierContainer);
        if (ok) log(`   ✅ Vivier Natixis → ${vivierVal}`, 2);
      }

      // LinkedIn
      const linkedinInput = document.querySelector('input[type="url"][id*="siteLink"]');
      if (linkedinInput && profile.linkedin_url) {
        fillInput(linkedinInput, profile.linkedin_url);
        log('   ✅ LinkedIn renseigné', 2);
      }

      log('✅ Formulaire rempli ! Veuillez vérifier et cliquer sur SOUMETTRE.', 2);
    }
  }

  function init() {
    if (window.__taleosBpceOracleInit) return;
    window.__taleosBpceOracleInit = true;

    chrome.storage.local.get('taleos_pending_bpce').then(s => {
      if (s.taleos_pending_bpce) setTimeout(runAutomation, 1200);
    });

    chrome.storage.onChanged.addListener((changes, area) => {
      if (area === 'local' && (changes.taleos_pending_bpce?.newValue || changes.taleos_bpce_pin_code?.newValue)) {
        setTimeout(runAutomation, 800);
      }
    });

    const observer = new MutationObserver(() => {
      clearTimeout(window.__taleosBpceDebounce);
      window.__taleosBpceDebounce = setTimeout(() => {
        chrome.storage.local.get('taleos_pending_bpce').then(s => {
          if (s.taleos_pending_bpce) runAutomation();
        });
      }, 1000);
    });
    observer.observe(document.body, { childList: true, subtree: true });
    log('👁️  MutationObserver actif (V1.0.55)', 2);
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();

  window.__taleosBpceInjectPin = (pin) => {
    chrome.storage.local.set({ taleos_bpce_pin_code: pin });
    log('📌 Code PIN injecté manuellement : ' + pin, 2);
  };
})();
