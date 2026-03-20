/**
 * Taleos - Remplissage formulaire BPCE Oracle Cloud (ekez.fa.em2.oraclecloud.com)
 * Version 1.0.58 : Détection persistante après le code PIN et sélecteurs robustes.
 */
(function() {
  'use strict';

  const BANNER_ID = 'taleos-bpce-oracle-banner';
  let lastProcessedStep = 0;
  let isAutomationRunning = false;

  function log(msg, stepNum) {
    const prefix = stepNum ? `[STEP ${stepNum}] ` : '';
    console.log(`[${new Date().toLocaleTimeString('fr-FR')}] [Taleos BPCE Oracle] ${prefix}${msg}`);
  }

  function showBanner() {
    if (document.getElementById(BANNER_ID)) return;
    const banner = document.createElement('div');
    banner.id = BANNER_ID;
    banner.textContent = '⏳ Automatisation Taleos active — Ne touchez à rien.';
    Object.assign(banner.style, {
      position: 'fixed', top: '0', left: '0', right: '0', zIndex: '2147483647',
      background: 'linear-gradient(135deg, #003366 0%, #0055a4 100%)', color: 'white',
      padding: '10px 20px', fontSize: '14px', fontWeight: '600', textAlign: 'center',
      fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
      boxShadow: '0 2px 10px rgba(0,0,0,0.2)'
    });
    document.body?.insertBefore(banner, document.body.firstChild);
  }

  function fillInput(input, value, label) {
    if (!input || value == null) return false;
    const newVal = String(value).trim();
    const currentVal = (input.value || '').trim();
    if (currentVal === newVal) {
      if (label) log(`   — ${label} → déjà "${currentVal}" (Skip)`);
      return false;
    }
    input.focus();
    const nativeSetter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set || 
                         Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value')?.set;
    if (nativeSetter) nativeSetter.call(input, newVal);
    else input.value = newVal;
    input.dispatchEvent(new Event('input', { bubbles: true }));
    input.dispatchEvent(new Event('change', { bubbles: true }));
    input.blur();
    if (label) log(`   ✅ ${label} → "${newVal}" (Mis à jour)`);
    return true;
  }

  function clickButtonByText(textToFind, container = document, label) {
    const elements = container.querySelectorAll('button, .cx-select-pill-section, .cx-select-pill-name, [role="button"]');
    const target = String(textToFind || '').trim().toLowerCase();
    for (const el of elements) {
      const elText = (el.textContent || '').trim().toLowerCase();
      if (elText === target) {
        const btn = el.closest('button') || el.closest('.cx-select-pill-section') || el;
        const isSelected = btn.classList.contains('cx-select-pill-section--selected') || 
                           btn.getAttribute('aria-pressed') === 'true' ||
                           btn.getAttribute('aria-checked') === 'true';
        if (!isSelected) {
          btn.click();
          if (label) log(`   ✅ ${label} → "${textToFind}" (Cliqué)`);
          return true;
        }
        if (label) log(`   — ${label} → déjà "${textToFind}" (Skip)`);
        return 'already_selected';
      }
    }
    return false;
  }

  async function runAutomation() {
    if (isAutomationRunning) return;
    isAutomationRunning = true;

    try {
      const { taleos_pending_bpce } = await chrome.storage.local.get('taleos_pending_bpce');
      if (!taleos_pending_bpce) {
        isAutomationRunning = false;
        return;
      }
      const { profile } = taleos_pending_bpce;
      showBanner();

      // --- Étape 1 : Email + CGU ---
      const emailInput = document.querySelector('#primary-email-0') || document.querySelector('input[type="email"]');
      if (emailInput && emailInput.offsetParent !== null && !document.querySelector('[id*="pin-code"]')) {
        if (lastProcessedStep < 1) {
          log('📋 Étape 1 : Email + CGU', 1);
          fillInput(emailInput, profile.email || profile.auth_email, 'Email');
          const cgu = document.querySelector('span.apply-flow-input-checkbox__button') || document.querySelector('.apply-flow-input-checkbox__button');
          if (cgu && !cgu.classList.contains('apply-flow-input-checkbox__button--checked')) {
            cgu.click();
            log('   ✅ CGU cochée');
          }
          const nextBtn = document.querySelector('button[title="Suivant"]') || Array.from(document.querySelectorAll('button')).find(b => b.textContent.includes('Suivant'));
          if (nextBtn) {
            nextBtn.click();
            lastProcessedStep = 1;
            log('✅ Clic Suivant → Code PIN');
          }
        }
        isAutomationRunning = false;
        return;
      }

      // --- Étape 1b : Code PIN ---
      const pinInput = document.querySelector('#pin-code-1');
      if (pinInput && pinInput.offsetParent !== null) {
        if (lastProcessedStep < 1.5) {
          log('📋 Étape 1b : Vérification d\'identité (Code PIN)', 1.5);
          const { taleos_bpce_pin_code } = await chrome.storage.local.get('taleos_bpce_pin_code');
          if (taleos_bpce_pin_code && String(taleos_bpce_pin_code).length === 6) {
            const pin = String(taleos_bpce_pin_code);
            for (let i = 0; i < 6; i++) {
              const field = document.querySelector(`#pin-code-${i + 1}`);
              if (field) fillInput(field, pin[i]);
            }
            const verifyBtn = Array.from(document.querySelectorAll('button')).find(b => b.textContent.includes('VÉRIFIER'));
            if (verifyBtn) {
              verifyBtn.click();
              lastProcessedStep = 1.5;
              log('✅ Code PIN soumis');
            }
          } else {
            log('   ⏳ En attente du code PIN...');
          }
        }
        isAutomationRunning = false;
        return;
      }

      // --- Étape 2 : Formulaire complet ---
      const lastNameInput = document.querySelector('#lastName-5') || document.querySelector('input[id*="lastName"]');
      if (lastNameInput && lastNameInput.offsetParent !== null) {
        if (lastProcessedStep < 2) {
          log('📋 Étape 2 : Formulaire complet', 2);
          
          // Contact
          fillInput(lastNameInput, profile.last_name || profile.lastname, 'Nom');
          fillInput(document.querySelector('#firstName-6') || document.querySelector('input[id*="firstName"]'), profile.first_name || profile.firstname, 'Prénom');
          
          const civ = (profile.civility || '').toLowerCase();
          if (civ.includes('monsieur')) clickButtonByText('M.', document, 'Titre');
          else if (civ.includes('madame')) clickButtonByText('Mme', document, 'Titre');

          fillInput(document.querySelector('input[type="tel"]'), profile.phone || profile.phone_number, 'Téléphone');
          fillInput(document.querySelector('input[id*="country-codes-dropdown"]'), profile.phone_country_code || '+33', 'Code Pays');

          // Questions
          log('📋 Étape 3 : Questions de candidature', 3);
          const handicapVal = (profile.bpce_handicap || 'Non').trim();
          const handicapContainer = Array.from(document.querySelectorAll('.apply-flow-block, .input-row')).find(el => el.textContent.toLowerCase().includes('handicap'));
          if (handicapContainer) clickButtonByText(handicapVal, handicapContainer, 'Handicap');

          const disponibiliteTextarea = document.querySelector('textarea[name="300000620007177"]') || document.querySelector('textarea[id^="300000620007177"]');
          if (disponibiliteTextarea) fillInput(disponibiliteTextarea, profile.available_from || profile.available_date, 'Disponibilité');

          const vivierVal = (profile.bpce_vivier_natixis || 'Oui').trim();
          const vivierContainer = Array.from(document.querySelectorAll('.apply-flow-block, .input-row')).find(el => el.textContent.toLowerCase().includes('vivier') || el.textContent.toLowerCase().includes('natixis'));
          if (vivierContainer) clickButtonByText(vivierVal, vivierContainer, 'Vivier Natixis');

          // LinkedIn
          const linkedinInput = document.querySelector('input[id*="siteLink"]');
          if (linkedinInput) fillInput(linkedinInput, profile.linkedin_url, 'LinkedIn');

          lastProcessedStep = 2;
          log('✅ Formulaire rempli ! Veuillez vérifier et SOUMETTRE.');
        }
      }
    } catch (e) {
      log('❌ Erreur automation: ' + e.message);
    } finally {
      isAutomationRunning = false;
    }
  }

  function init() {
    if (window.__taleosBpceOracleInit) return;
    window.__taleosBpceOracleInit = true;
    log('👁️  MutationObserver actif (V1.0.58)');
    
    // Déclenchement périodique plus fréquent au début pour ne pas rater la transition après le PIN
    const fastCheck = setInterval(() => {
      if (lastProcessedStep >= 1.5 && lastProcessedStep < 2) {
        runAutomation();
      }
    }, 1000);
    
    // Arrêter le check rapide après 30 secondes pour économiser les ressources
    setTimeout(() => clearInterval(fastCheck), 30000);

    const observer = new MutationObserver(() => {
      clearTimeout(window.__taleosBpceDebounce);
      window.__taleosBpceDebounce = setTimeout(runAutomation, 800);
    });
    observer.observe(document.body, { childList: true, subtree: true });
    runAutomation();
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
