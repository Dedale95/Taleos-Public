/**
 * Taleos - Remplissage formulaire BPCE Oracle Cloud (ekez.fa.em2.oraclecloud.com)
 * Flux multi-étapes : Email+CGU → Code PIN → Données personnelles → Questions → Documents → Alertes
 * Gestion du code PIN : Peut être injecté automatiquement via message ou saisi manuellement
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

  function clickPillByText(labelText, value) {
    const pills = document.querySelectorAll('.cx-select-pill-section, button.cx-select-pill-section');
    for (const pill of pills) {
      const text = (pill.textContent || '').trim();
      const target = String(value || '').trim();
      if (!target) continue;
      if (text === target || text.includes(target) || target.includes(text)) {
        const isSelected = pill.classList.contains('cx-select-pill-section--selected') || pill.getAttribute('aria-pressed') === 'true';
        if (!isSelected) {
          pill.click();
          log('   ✅ ' + labelText + ' → ' + text, 2);
          return true;
        }
        log('   — ' + labelText + ' → déjà ' + text, 2);
        return false;
      }
    }
    log('   ⏭️  ' + labelText + ' → option non trouvée pour "' + value + '"', 2);
    return false;
  }

  function civilityToBpce(civility) {
    const c = (civility || '').trim().toLowerCase();
    if (c.includes('monsieur')) return 'M.';
    if (c.includes('madame')) return 'Mme';
    return '';
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

  async function waitForElement(selectorFn, maxWait = 15000) {
    const start = Date.now();
    while (Date.now() - start < maxWait) {
      const el = selectorFn();
      if (el && el.offsetParent !== null) return el;
      await new Promise(r => setTimeout(r, 300));
    }
    return null;
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

  async function runAutomation() {
    const { taleos_pending_bpce } = await chrome.storage.local.get('taleos_pending_bpce');
    if (!taleos_pending_bpce) {
      log('⏭️  Pas de candidature BPCE en cours → skip', 2);
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

    // --- Étape 1 : Email + CGU ---
    const emailInput = document.querySelector('#primary-email-0') || document.querySelector('input[name="primary-email"]') || document.querySelector('input[type="email"][aria-label*="électronique"]');
    if (emailInput && emailInput.offsetParent !== null) {
      log('📋 Étape 1 : Email + CGU', 2);
      fillInput(emailInput, email);
      log('   ✅ Email renseigné', 2);

      const cguCheckbox = document.querySelector('span.apply-flow-input-checkbox__button') || document.querySelector('.apply-flow-input-checkbox__button');
      if (cguCheckbox && cguCheckbox.offsetParent !== null) {
        const isChecked = cguCheckbox.classList.contains('apply-flow-input-checkbox__button--checked');
        if (!isChecked) {
          cguCheckbox.click();
          log('   ✅ CGU cochée', 2);
          await new Promise(r => setTimeout(r, 300));
        }
      }

      const nextBtn = findNextButton();
      if (nextBtn && !nextBtn.disabled) {
        nextBtn.click();
        log('✅ Clic Suivant → étape suivante', 2);
        setTimeout(runAutomation, 1500);
      }
      return;
    }

    // --- Étape 1b : Code PIN (Vérification d'identité) ---
    const pinInput1 = document.querySelector('#pin-code-1');
    if (pinInput1 && pinInput1.offsetParent !== null) {
      log('📋 Étape 1b : Vérification d\'identité (Code PIN)', 2);
      const { taleos_bpce_pin_code } = await chrome.storage.local.get('taleos_bpce_pin_code');
      if (taleos_bpce_pin_code) {
        const pinStr = String(taleos_bpce_pin_code).trim();
        if (pinStr.length === 6) {
          log('   📌 Code PIN trouvé en storage → remplissage automatique', 2);
          for (let i = 0; i < 6; i++) {
            const pinField = document.querySelector(`#pin-code-${i + 1}`);
            if (pinField) fillInput(pinField, pinStr[i]);
          }
          await new Promise(r => setTimeout(r, 500));
          const verifyBtn = Array.from(document.querySelectorAll('button')).find(b => b.textContent.includes('VÉRIFIER'));
          if (verifyBtn) {
            verifyBtn.click();
            log('✅ Code PIN soumis → vérification en cours', 2);
            setTimeout(runAutomation, 2000);
          }
        }
      } else {
        log('   ⏳ En attente du code PIN (à saisir manuellement ou via API email)', 2);
      }
      return;
    }

    // --- Étape 2 : Données personnelles (Nom, Prénom, Titre, Téléphone) ---
    const lastNameInput = document.querySelector('input[name="lastName"]') || document.querySelector('#lastName-12') || document.querySelector('#lastName-10');
    const firstNameInput = document.querySelector('input[name="firstName"]') || document.querySelector('#firstName-13') || document.querySelector('#firstName-11');
    if (lastNameInput && firstNameInput && lastNameInput.offsetParent !== null) {
      log('📋 Étape 2 : Données personnelles', 2);
      fillInput(lastNameInput, profile.lastname || profile.last_name);
      log('   ✅ Nom renseigné', 2);
      fillInput(firstNameInput, profile.firstname || profile.first_name);
      log('   ✅ Prénom renseigné', 2);

      const titreBpce = civilityToBpce(profile.civility);
      if (titreBpce) {
        clickPillByText('Titre', titreBpce);
        await new Promise(r => setTimeout(r, 200));
      }

      const phoneInput = document.querySelector('input.phone-row__input') || document.querySelector('input[type="tel"][aria-label*="téléphone"]') || document.querySelector('.phone-row input[type="tel"]') || Array.from(document.querySelectorAll('input[type="tel"]')).find(i => i.value || !i.disabled);
      const phoneNumber = (profile.phone_number || profile.phone || '').replace(/\D/g, '');
      if (phoneInput && phoneNumber && phoneInput.offsetParent !== null) {
        fillInput(phoneInput, phoneNumber);
        log('   ✅ Téléphone renseigné', 2);
      }

      const countryCodeDropdown = document.querySelector('#country-codes-dropdownphoneNumber') || document.querySelector('input[name="phoneNumber"][role="combobox"]') || document.querySelector('input[role="combobox"]');
      const phoneCountryCode = (profile.phone_country_code || '+33').trim();
      if (countryCodeDropdown && phoneCountryCode && countryCodeDropdown.offsetParent !== null) {
        fillInput(countryCodeDropdown, phoneCountryCode);
        countryCodeDropdown.dispatchEvent(new Event('blur', { bubbles: true }));
        await new Promise(r => setTimeout(r, 300));
      }

      const nextBtn2 = findNextButton();
      if (nextBtn2 && !nextBtn2.disabled) {
        nextBtn2.click();
        log('✅ Clic Suivant → Questions', 2);
        setTimeout(runAutomation, 1500);
      }
      return;
    }

    // --- Étape 3 : Questions (handicap, disponibilité, vivier Natixis) ---
    const handicapLabel = Array.from(document.querySelectorAll('label, span')).find(el => /handicap.*titre de reconnaissance/i.test(el.textContent || ''));
    if (handicapLabel && handicapLabel.offsetParent !== null) {
      log('📋 Étape 3 : Questions de candidature', 2);
      const handicapVal = (profile.bpce_handicap || '').trim();
      if (handicapVal) clickPillByText('Handicap', handicapVal);

      const disponibiliteTextarea = document.querySelector('textarea[name="300000620007177"]') || Array.from(document.querySelectorAll('textarea')).find(t => /disponibilité/i.test((t.closest('label') || t.previousElementSibling || {}).textContent || ''));
      const availableFrom = (profile.available_date || profile.available_from || '').trim();
      if (disponibiliteTextarea && availableFrom && disponibiliteTextarea.offsetParent !== null) {
        fillInput(disponibiliteTextarea, availableFrom);
        log('   ✅ Disponibilité renseignée', 2);
      }

      const vivierVal = (profile.bpce_vivier_natixis || '').trim();
      if (vivierVal) {
        const vivierLabel = Array.from(document.querySelectorAll('label, span')).find(el => /vivier.*candidats|natixis.*conserver/i.test(el.textContent || ''));
        if (vivierLabel) {
          const container = vivierLabel.closest('.input-row__control-container') || vivierLabel.closest('.apply-flow-block');
          if (container) {
            const pills = container.querySelectorAll('.cx-select-pill-section');
            for (const pill of pills) {
              if ((pill.textContent || '').trim() === vivierVal) {
                if (!pill.classList.contains('cx-select-pill-section--selected')) {
                  pill.click();
                  log('   ✅ Vivier Natixis → ' + vivierVal, 2);
                }
                break;
              }
            }
          }
        }
      }

      const nextBtn3 = findNextButton();
      if (nextBtn3 && !nextBtn3.disabled) {
        nextBtn3.click();
        log('✅ Clic Suivant → Documents', 2);
        setTimeout(runAutomation, 1500);
      }
      return;
    }

    // --- Étape 4 : Documents (CV, lettre de motivation, LinkedIn) ---
    const cvFileInput = document.querySelector('input[type="file"][id*="attachment-upload"]') || document.querySelector('.file-upload-wrapper input[type="file"]');
    const linkedinInput = document.querySelector('input[name="siteLink-1"]') || document.querySelector('input[type="url"][id*="siteLink"]');
    const isDocumentsSection = (cvFileInput && cvFileInput.offsetParent !== null) || (linkedinInput && linkedinInput.offsetParent !== null);
    if (isDocumentsSection) {
      log('📋 Étape 4 : Documents annexes', 2);
      const cvPath = profile.cv_storage_path;
      const cvName = profile.cv_filename || (cvPath ? cvPath.split('/').pop() : 'cv.pdf');
      if (cvPath && cvFileInput) {
        const cvInputs = document.querySelectorAll('input[type="file"]');
        const cvInput = cvInputs[0] || cvFileInput;
        const ok = await setFileInputFromStorage(cvInput, cvPath, cvName);
        if (ok) log('   ✅ CV uploadé', 2);
      }
      const lmPath = profile.lm_storage_path;
      const lmName = profile.lm_filename || (lmPath ? lmPath.split('/').pop() : 'lettre.pdf');
      if (lmPath) {
        const lmInputs = document.querySelectorAll('input[type="file"]');
        const lmInput = lmInputs.length > 1 ? lmInputs[1] : lmInputs[0];
        if (lmInput) {
          const ok = await setFileInputFromStorage(lmInput, lmPath, lmName);
          if (ok) log('   ✅ Lettre de motivation uploadée', 2);
        }
      }
      const linkedinUrl = (profile.linkedin_url || '').trim();
      if (linkedinInput && linkedinUrl && linkedinInput.offsetParent !== null) {
        fillInput(linkedinInput, linkedinUrl);
        log('   ✅ LinkedIn renseigné', 2);
      }

      const nextBtn4 = findNextButton();
      if (nextBtn4 && !nextBtn4.disabled) {
        nextBtn4.click();
        log('✅ Clic Suivant → Alertes', 2);
        setTimeout(runAutomation, 1500);
      }
      return;
    }

    // --- Étape 5 : Job alerts checkbox ---
    const jobAlertsCheckbox = document.querySelector('#job-alerts-checkbox') || document.querySelector('input[type="checkbox"][id*="job-alerts"]');
    if (jobAlertsCheckbox && jobAlertsCheckbox.offsetParent !== null) {
      log('📋 Étape 5 : Alertes emploi BPCE', 2);
      if (profile.bpce_job_alerts && !jobAlertsCheckbox.checked) {
        jobAlertsCheckbox.click();
        log('   ✅ Case alertes cochée', 2);
      } else if (!profile.bpce_job_alerts && jobAlertsCheckbox.checked) {
        jobAlertsCheckbox.click();
        log('   ✅ Case alertes décochée', 2);
      }
      await new Promise(r => setTimeout(r, 300));

      const finalBtn = findNextButton();
      if (finalBtn && !finalBtn.disabled) {
        finalBtn.click();
        log('✅ Clic Suivant / Soumettre → candidature envoyée', 2);
        chrome.storage.local.remove(['taleos_pending_bpce', 'taleos_bpce_tab_id']);
      }
      hideBanner();
      return;
    }

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
      if (area === 'local') {
        if (changes.taleos_pending_bpce?.newValue) {
          setTimeout(runAutomation, 1200);
        }
        if (changes.taleos_bpce_pin_code?.newValue) {
          log('📌 Code PIN reçu via message → relance de l\'automatisation', 2);
          setTimeout(runAutomation, 500);
        }
      }
    });

    // Écouteur pour recevoir le code PIN via message (depuis background.js ou API email)
    chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
      if (msg.action === 'bpce_pin_code') {
        const pinCode = String(msg.pinCode || '').trim();
        if (pinCode.length === 6) {
          log('📌 Code PIN reçu via message : ' + pinCode, 2);
          chrome.storage.local.set({ taleos_bpce_pin_code: pinCode });
          sendResponse({ ok: true });
        } else {
          log('❌ Code PIN invalide (doit avoir 6 chiffres)', 2);
          sendResponse({ ok: false, error: 'Code invalide' });
        }
      }
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

  // Fonction publique pour injecter le code PIN manuellement (utile pour tests)
  window.__taleosBpceInjectPin = function(pinCode) {
    chrome.storage.local.set({ taleos_bpce_pin_code: pinCode });
    log('📌 Code PIN injecté manuellement : ' + pinCode, 2);
  };
})();
