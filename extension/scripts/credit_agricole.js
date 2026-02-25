/**
 * Taleos - Automatisation Crédit Agricole (groupecreditagricole.jobs)
 * Logique conforme au notebook Python : Je postule → Connexion → Identifiants → Reload → Je postule → Formulaire
 * Support multilingue : FR, EN
 */

(function() {
  'use strict';

  const delay = ms => new Promise(r => setTimeout(r, ms));
  const offerUrl = window.location.href;

  const TEXTS = {
    apply: ['Je postule', 'Apply', 'Apply now', 'I apply'],
    applyExclude: ['Comment postuler', 'How to apply', 'comment postuler', 'how to apply', 'Étapes de recrutement', 'Recruitment stages'],
    cookieDismiss: ['Refuser', 'Refuse', 'Accepter', 'Accept', 'Fermer', 'Close', 'Reject', 'Tout accepter', 'Accept all'],
    loginLinkHref: ['connexion', 'login', 'sign-in', 'signin'],
    alreadyApplied: [
      'vous avez déjà postulé', 'désolé.*déjà postulé', 'suivre ma candidature',
      'you have already applied', 'sorry.*already applied', 'track my application', 'already applied'
    ],
    rgpdCheckbox: ['Je déclare avoir lu', 'I declare that I have read', 'I have read', 'J\'accepte', 'I accept'],
    successMessage: [
      'votre candidature a été envoyée avec succès', 'envoyée avec succès', 'candidature validée',
      'your application has been sent', 'application sent successfully', 'application submitted'
    ],
    selectPlaceholder: ['Sélectionnez', 'Select', 'Choose', 'Choisir']
  };

  function log(msg) {
    const t = new Date().toLocaleTimeString('fr-FR');
    console.log(`[${t}] [Taleos CA] ${msg}`);
  }

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
    root.insertBefore(banner, root.firstChild);
  }
  function hideAutomationBanner() {
    document.getElementById(BANNER_ID)?.remove();
  }

  function findText(selector, text) {
    const els = document.querySelectorAll(selector || '*');
    return Array.from(els).find(el => (el.textContent || '').includes(text));
  }

  function findClickablePostule() {
    const byDataPopin = document.querySelector('button.cta.primary[data-popin="popin-application"], button[data-popin="popin-application"]');
    if (byDataPopin && byDataPopin.offsetParent !== null) return byDataPopin;
    const isExcluded = (el) => {
      const txt = (el.textContent || '').trim().toLowerCase();
      const href = (el.getAttribute?.('href') || '').toLowerCase();
      if (TEXTS.applyExclude.some(x => txt.includes(x.toLowerCase()))) return true;
      if (/comment-postuler|nos-conseils|our-tips|how-to-apply|etapes-de-recrutement/.test(href)) return true;
      if (el.closest?.('nav, [role="navigation"]')) return true;
      return false;
    };
    const byTag = document.querySelectorAll('button, a, [role="button"], .cta, [class*="cta"]');
    for (const el of byTag) {
      if (isExcluded(el)) continue;
      const txt = (el.textContent || '').trim();
      if (!/^Je postule$/i.test(txt) && !/^Apply(\s+now)?$/i.test(txt) && !/^I apply$/i.test(txt)) continue;
      if (el.offsetParent === null) continue;
      return el;
    }
    for (const el of byTag) {
      if (isExcluded(el)) continue;
      const txt = (el.textContent || '').trim();
      if (!TEXTS.apply.some(t => txt === t || txt.includes(t))) continue;
      if (el.offsetParent === null) continue;
      return el;
    }
    return null;
  }

  function findCookieDismissButton() {
    const byClass = document.querySelector('button.rgpd-btn-refuse, button.rgpd-btn-accept, [class*="rgpd"][class*="btn"]');
    if (byClass && byClass.offsetParent !== null) return byClass;
    const buttons = document.querySelectorAll('button, a, [role="button"]');
    for (const btn of buttons) {
      const txt = (btn.textContent || '').trim().toLowerCase();
      if (TEXTS.cookieDismiss.some(t => txt === t.toLowerCase() || txt.includes(t.toLowerCase()))) {
        if (btn.offsetParent !== null) return btn;
      }
    }
    return null;
  }

  async function waitForPostuleButton(maxWait = 20000) {
    const start = Date.now();
    while (Date.now() - start < maxWait) {
      const btn = findClickablePostule();
      if (btn && btn.offsetParent !== null) return btn;
      await delay(500);
    }
    return null;
  }

  /** Attend que l'animation de chargement soit terminée avant d'interagir */
  async function waitForLoadingComplete(maxWait = 30000) {
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
    log('   ⚠️ Timeout attente chargement (poursuite quand même).');
    return false;
  }

  function safeFill(id, value, label) {
    let el = document.getElementById(id);
    if (!el) el = document.querySelector(`input[name="${id}"], input[name*="${id.replace('form-apply-', '')}"]`);
    if (!el) return Promise.resolve();
    const current = (el.value || '').trim();
    const target = value != null ? String(value).trim() : '';
    if (!target) return Promise.resolve();
    if (current === target) {
      log(`   ✅ ${label} : Déjà correct (Firebase identique) -> Skip`);
      return Promise.resolve();
    }
    log(`   ✏️  ${label} : Remplacer '${current || '(vide)'}' par '${target}' (Firebase)`);
    const nativeSetter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set;
    if (nativeSetter) nativeSetter.call(el, target);
    else el.value = target;
    el.dispatchEvent(new Event('input', { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
    el.dispatchEvent(new Event('blur', { bubbles: true }));
    return Promise.resolve();
  }

  function getCleanListFromText(raw) {
    if (!raw) return [];
    const hasPlaceholder = TEXTS.selectPlaceholder.some(p => raw.includes(p));
    if (hasPlaceholder) return [];
    return raw.split(',').map(p => p.replace(/\s*\(\d+\)/g, '').trim().toLowerCase()).filter(Boolean);
  }

  async function auditCombobox(ariaId, expectedVal, label) {
    if (!expectedVal) return;
    const trigger = document.querySelector(`div[aria-controls="${ariaId}"], button[aria-controls="${ariaId}"]`);
    if (!trigger) return;
    const current = (trigger.textContent || '').trim();
    const exp = expectedVal.toLowerCase();
    const cur = current.toLowerCase();
    const placeholderMatch = TEXTS.selectPlaceholder.some(p => current.includes(p));
    if (exp && cur && (exp.includes(cur) || cur.includes(exp)) && !placeholderMatch) {
      log(`   ✅ ${label} : Déjà correct (${current} = Firebase) -> Skip`);
      return;
    }
    log(`   ✏️  ${label} : Remplacer '${current || TEXTS.selectPlaceholder[0]}' par '${expectedVal}' (Firebase)`);
    trigger.click();
    await delay(500);
    const panel = document.getElementById(ariaId);
    if (!panel) return;
    const labels = panel.querySelectorAll('label');
    for (const lbl of labels) {
      const txt = (lbl.textContent || '').replace(/\s*\(\d+\)/g, '').trim().toLowerCase();
      if (txt && expectedVal.toLowerCase().includes(txt)) {
        lbl.click();
        await delay(500);
        break;
      }
    }
    document.body.click();
    await delay(500);
  }

  async function syncMultiselectSmart(btnId, label, expectedList) {
    if (!expectedList || !expectedList.length) return;
    const btn = document.getElementById(btnId);
    if (!btn) return;
    const span = btn.querySelector('.button-text');
    const currentText = span ? span.textContent : btn.textContent;
    const curSel = getCleanListFromText(currentText || '');
    const expClean = expectedList.map(x => String(x).toLowerCase());
    if (new Set(curSel).size === new Set(expClean).size && curSel.every(c => expClean.includes(c))) {
      log(`   ✅ ${label} : Déjà synchronisé (${curSel.join(', ')} = Firebase) -> Skip`);
      return;
    }
    log(`   ⚙️  ${label} : Actuel [${curSel.join(', ') || 'vide'}] -> Cible [${expClean.join(', ')}] (Firebase)`);
    btn.click();
    await delay(1000);
    const dropdown = btn.nextElementSibling;
    if (!dropdown) return;
    const labels = dropdown.querySelectorAll('label');
    for (const lbl of labels) {
      const raw = lbl.textContent || '';
      const val = raw.replace(/\s*\(\d+\)/g, '').trim().toLowerCase();
      if (!val) continue;
      const forId = lbl.getAttribute('for');
      const input = document.getElementById(forId);
      if (!input) continue;
      const isChecked = input.checked;
      const shouldBe = expClean.includes(val);
      if (isChecked && !shouldBe) {
        log(`      🗑️ Retrait : ${val}`);
        lbl.click();
        await delay(100);
      } else if (!isChecked && shouldBe) {
        log(`      ➕ Ajout : ${val}`);
        lbl.click();
        await delay(100);
      }
    }
    btn.click();
    await delay(300);
  }

  async function setFileInputFromStorage(inputId, storagePath, filename) {
    if (!storagePath) return;
    const input = document.getElementById(inputId);
    if (!input) return;
    try {
      const r = await new Promise(resolve => {
        chrome.runtime.sendMessage({ action: 'fetch_storage_file', storagePath }, resolve);
      });
      if (r?.error) throw new Error(r.error);
      if (!r?.base64) return;
      const bin = atob(r.base64);
      const arr = new Uint8Array(bin.length);
      for (let i = 0; i < bin.length; i++) arr[i] = bin.charCodeAt(i);
      const blob = new Blob([arr], { type: r.type || 'application/pdf' });
      const file = new File([blob], filename || 'document.pdf', { type: blob.type });
      const dt = new DataTransfer();
      dt.items.add(file);
      input.files = dt.files;
      input.dispatchEvent(new Event('change', { bubbles: true }));
      input.dispatchEvent(new Event('input', { bubbles: true }));
    } catch (e) {
      log(`   ❌ Erreur upload fichier: ${e.message}`);
    }
  }

  async function runAuditAndFill(p) {
    const nextBtn = () => document.querySelector('button.cta.next-step');
    const firstnameEl = document.getElementById('form-apply-firstname');
    const valPrenom = (firstnameEl?.value || '').trim();
    if (valPrenom) {
      log(`🔵 MODE : VÉRIFICATION (Prénom détecté: '${valPrenom}')`);
    } else {
      log('🟢 MODE : REMPLISSAGE NEUF (Formulaire vide)');
    }

    log('📂 [1/4] Mes informations');
    await safeFill('form-apply-firstname', p.firstname, 'Prénom');
    await safeFill('form-apply-lastname', p.lastname, 'Nom');
    await safeFill('form-apply-address', p.address, 'Adresse');
    await safeFill('form-apply-zipcode', p.zipcode, 'Code Postal');
    await safeFill('form-apply-city', p.city, 'Ville');
    await safeFill('form-apply-phone-number', p['phone-number'], 'Téléphone');
    await auditCombobox('customSelect-civility', p.civility, 'Civilité');
    await auditCombobox('customSelect-country', p.country, 'Pays');
    const nb = nextBtn();
    if (nb) { nb.click(); await delay(2000); }

    log('📂 [2/4] Mes documents');
    const acc1 = document.querySelector("button[aria-controls='accordion-item-1']");
    if (acc1 && acc1.getAttribute('aria-expanded') === 'false') {
      acc1.click();
      await delay(1000);
    }
    const cvInput = document.getElementById('form-apply-cv');
    const cvContainer = cvInput?.parentElement;
    const cvText = (cvContainer?.textContent || '').toLowerCase();
    const hasCvInput = cvInput?.files?.length > 0;
    const hasCvUi = /\.pdf|\.doc/.test(cvText) || cvContainer?.querySelector('.uploaded-file, .file-name');
    const hasCv = hasCvInput || !!hasCvUi;
    if (p.cv_storage_path && !hasCv) {
      log('   ✏️  CV : Manquant -> Upload depuis Firebase');
      await setFileInputFromStorage('form-apply-cv', p.cv_storage_path, 'cv.pdf');
      await delay(3000);
    } else {
      log(`   ✅ CV : ${hasCv ? 'Présent (Firebase identique ou déjà uploadé) -> Skip' : 'Non requis'}`);
    }
    const lmInput = document.getElementById('form-apply-lm');
    const lmContainer = lmInput?.parentElement;
    const lmText = (lmContainer?.textContent || '').toLowerCase();
    const hasLmInput = lmInput?.files?.length > 0;
    const hasLmUi = /\.pdf|\.doc/.test(lmText) || lmContainer?.querySelector('.uploaded-file, .file-name');
    const hasLm = hasLmInput || !!hasLmUi;
    if (p.lm_storage_path && !hasLm) {
      log('   ✏️  LM : Manquante -> Upload depuis Firebase');
      await setFileInputFromStorage('form-apply-lm', p.lm_storage_path, 'lm.pdf');
      await delay(2000);
    } else {
      log(`   ✅ LM : ${hasLm ? 'Présente (Firebase identique ou déjà uploadée) -> Skip' : 'Non requise'}`);
    }
    const nb2 = nextBtn();
    if (nb2) { nb2.click(); await delay(2000); }

    log('📂 [3/4] Mon profil');
    const acc2 = document.querySelector("button[aria-controls='accordion-item-2']");
    if (acc2 && acc2.getAttribute('aria-expanded') === 'false') {
      acc2.click();
      await delay(1000);
    }
    await syncMultiselectSmart('form-apply-input-families', 'Métiers', p.job_families);
    if (p.contract_types?.[0]) await auditCombobox('customSelect-contract', p.contract_types[0], 'Contrat');
    if (p.available_date) await safeFill('form-apply-available-date', p.available_date, 'Date Dispo');
    await syncMultiselectSmart('form-apply-input-continents', 'Continents', p.continents);
    await delay(500);
    await syncMultiselectSmart('form-apply-input-countries', 'Pays Cibles', p.target_countries);
    await delay(500);
    await syncMultiselectSmart('form-apply-input-regions', 'Régions', p.target_regions);
    await auditCombobox('customSelect-experience-level', p.experience_level, 'Expérience');
    const nb3 = nextBtn();
    if (nb3) { nb3.click(); await delay(2000); }

    log('📂 [4/4] Mes formations');
    const acc3 = document.querySelector("button[aria-controls='accordion-item-3']");
    if (acc3 && acc3.getAttribute('aria-expanded') === 'false') {
      acc3.click();
      await delay(1000);
    }
    await auditCombobox('customSelect-education-level', p.education_level, 'Niveau Etudes');
    await auditCombobox('customSelect-school', p.school_type, 'Ecole');
    await auditCombobox('customSelect-diploma-status', p.diploma_status, 'Statut');
    await safeFill('form-apply-diploma-date-obtained', p.diploma_year, 'Année Diplôme');
    window.scrollTo(0, document.body.scrollHeight);
    await delay(500);
    log('   🗣️  Gestion des langues (Firebase vs formulaire)...');
    for (let i = 0; i < (p.languages || []).length; i++) {
      const lang = p.languages[i];
      if (!lang?.name) continue;
      const langTrigger = document.querySelector(`div[aria-controls='customSelect-language-${i + 1}']`);
      const levelTrigger = document.querySelector(`div[aria-controls='customSelect-language-level-${i + 1}']`);
      if (i > 0 && !langTrigger) {
        const addBtn = document.getElementById('add-language-btn');
        if (addBtn) {
          log(`      ➕ Clic 'Ajouter langue' pour Slot ${i + 1}`);
          addBtn.click();
          await delay(1000);
        }
      }
      if (langTrigger) {
        const cur = (langTrigger.textContent || '').trim();
        if (cur && cur.toLowerCase().includes(lang.name.toLowerCase()) && !cur.includes('Sélectionnez')) {
          log(`      ✅ Langue ${i + 1} : Déjà correct (${cur} = Firebase) -> Skip`);
        } else {
          log(`      ✏️  Langue ${i + 1} : '${cur || 'Sélectionnez'}' -> '${lang.name}' (Firebase)`);
          langTrigger.click();
          await delay(500);
          const panel = document.getElementById(`customSelect-language-${i + 1}`);
          const labels = panel?.querySelectorAll('label') || [];
          for (const lbl of labels) {
            if ((lbl.textContent || '').toLowerCase().includes(lang.name.toLowerCase())) { lbl.click(); break; }
          }
        }
      }
      if (levelTrigger) {
        const cur = (levelTrigger.textContent || '').trim();
        if (cur && cur.toLowerCase().includes((lang.level || '').toLowerCase()) && !cur.includes('Sélectionnez')) {
          log(`      ✅ Niveau ${i + 1} : Déjà correct (${cur} = Firebase) -> Skip`);
        } else {
          log(`      ✏️  Niveau ${i + 1} : '${cur || 'Sélectionnez'}' -> '${lang.level || ''}' (Firebase)`);
          levelTrigger.click();
          await delay(500);
          const panel = document.getElementById(`customSelect-language-level-${i + 1}`);
          const opts = panel?.querySelectorAll('label') || [];
          for (const o of opts) {
            if ((o.textContent || '').toLowerCase().includes((lang.level || '').toLowerCase())) { o.click(); break; }
          }
        }
      }
    }
    const nb4 = nextBtn();
    if (nb4) { nb4.click(); await delay(2000); }
  }

  async function waitForForm(maxWait = 45000) {
    const start = Date.now();
    const formSelectors = [
      () => document.getElementById('form-apply-firstname'),
      () => document.querySelector('[id*="form-apply"][id*="firstname"]'),
      () => document.querySelector('input[name*="firstname"], input[id*="firstname"]'),
      () => document.querySelector('#form-apply-lastname, input[name*="lastname"]'),
      () => document.querySelector('form[id*="apply"], form[class*="apply"]')
    ];
    while (Date.now() - start < maxWait) {
      for (const sel of formSelectors) {
        const el = sel();
        if (el && el.offsetParent !== null) return true;
      }
      await delay(500);
    }
    return false;
  }

  async function waitForLoginForm(maxWait = 10000) {
    const start = Date.now();
    while (Date.now() - start < maxWait) {
      const el = document.querySelector('#form-login-email, input[id*="login-email"], input[type="email"][name*="mail"]');
      if (el && el.offsetParent !== null) return true;
      const iframes = document.querySelectorAll('iframe');
      for (const f of iframes) {
        try {
          const doc = f.contentDocument;
          if (doc) {
            const ie = doc.querySelector('#form-login-email, input[id*="login"], input[type="email"]');
            if (ie) return true;
          }
        } catch (_) {}
      }
      await delay(300);
    }
    return false;
  }

  function forceFillInput(input, value) {
    if (!input || value == null) return false;
    input.focus();
    input.select();
    try {
      const nativeSetter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set;
      if (nativeSetter) {
        nativeSetter.call(input, value);
      } else {
        input.value = value;
      }
      ['input', 'change', 'keyup', 'blur'].forEach(ev => {
        input.dispatchEvent(new Event(ev, { bubbles: true }));
      });
      return true;
    } catch (e) {
      console.warn('[Taleos CA] forceFillInput:', e);
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
    const iframes = document.querySelectorAll('iframe');
    for (const f of iframes) {
      try {
        const doc = f.contentDocument;
        if (doc) {
          const e = doc.querySelector('#form-login-email, input[id*="login"], input[type="email"]');
          const p = doc.querySelector('#form-login-password, input[id*="password"], input[type="password"]');
          const s = doc.querySelector('#form-login-submit, button[type="submit"]');
          if (e && p) return { email: e, pass: p, submit: s };
        }
      } catch (_) {}
    }
    return null;
  }

  function dumpProfile(p) {
    const sep = '🟦'.repeat(40);
    console.log(`\n${sep}\n📋 DUMP COMPLET DES DONNÉES CHARGÉES (FIREBASE)\n${sep}`);
    const fmt = (k, v) => {
      if (v == null) return;
      const val = typeof v === 'object' ? JSON.stringify(v) : String(v);
      const mask = /password|auth_password|token/i.test(k) ? '******** (Masqué)' : val;
      console.log(`[Taleos CA] 🔹 ${String(k).padEnd(22)} : ${mask}`);
    };
    fmt('civility', p.civility);
    fmt('firstname', p.firstname);
    fmt('lastname', p.lastname);
    fmt('address', p.address);
    fmt('zipcode', p.zipcode);
    fmt('city', p.city);
    fmt('country', p.country);
    fmt('phone-number', p['phone-number']);
    fmt('job_families', p.job_families);
    fmt('contract_types', p.contract_types);
    fmt('available_date', p.available_date);
    fmt('continents', p.continents);
    fmt('target_countries', p.target_countries);
    fmt('target_regions', p.target_regions);
    fmt('experience_level', p.experience_level);
    fmt('education_level', p.education_level);
    fmt('school_type', p.school_type);
    fmt('diploma_status', p.diploma_status);
    fmt('diploma_year', p.diploma_year);
    fmt('languages', p.languages);
    fmt('cv_storage_path', p.cv_storage_path ? '(présent)' : null);
    fmt('lm_storage_path', p.lm_storage_path ? '(présent)' : null);
    fmt('auth_email', p.auth_email);
    fmt('auth_password', p.auth_password ? '******** (Masqué)' : null);
    console.log(`${sep}\n`);
  }

  async function waitForFormReady(maxWait = 25000) {
    const start = Date.now();
    let lastVal = '';
    let stableCount = 0;
    while (Date.now() - start < maxWait) {
      const el = document.getElementById('form-apply-firstname');
      const cur = (el?.value || '').trim();
      if (el && el.offsetParent != null) {
        if (cur === lastVal) {
          stableCount++;
          if (stableCount >= 3) return true;
        } else {
          lastVal = cur;
          stableCount = 0;
        }
      }
      await delay(500);
    }
    return !!document.getElementById('form-apply-firstname');
  }

  async function waitForSuccessMessage(maxWait = 45000) {
    const start = Date.now();
    const re = new RegExp(TEXTS.successMessage.join('|'), 'i');
    while (Date.now() - start < maxWait) {
      const txt = document.body?.textContent || '';
      if (re.test(txt)) return true;
      await delay(1000);
    }
    return false;
  }

  async function main(profile) {
    showAutomationBanner();
    const phase = profile.__phase;
    const p = { ...profile };
    const jobId = p.__jobId;
    const jobTitle = p.__jobTitle;
    const companyName = p.__companyName;
    const offerUrlForNotify = p.__offerUrl || offerUrl;
    delete p.__phase;
    delete p.__jobId;
    delete p.__jobTitle;
    delete p.__companyName;
    delete p.__offerUrl;

    const offerIdMatch = offerUrl.match(/reference--([\d-]+)--/);
    const offerId = offerIdMatch ? offerIdMatch[1] : 'INCONNU';

    console.log('\n' + '='.repeat(60));
    const phaseLabels = { 2: 'Formulaire (après reload)', 3: 'Formulaire direct (redirection /candidature/)' };
    log(phaseLabels[phase] ? `🚀 PHASE ${phase} : ${phaseLabels[phase]}` : '🚀 DÉMARRAGE BOT CRÉDIT AGRICOLE');
    log(`🔗 URL : ${offerUrl}`);
    log(`🆔 ID  : ${offerId}`);
    console.log('='.repeat(60));

    const alreadyAppliedRe = new RegExp(TEXTS.alreadyApplied.join('|'), 'i');
    const checkAlreadyApplied = () => alreadyAppliedRe.test(document.body?.textContent || '');
    for (let i = 0; i < 3; i++) {
      if (checkAlreadyApplied()) {
        log('🛑 Déjà candidaté à cette offre (détecté sur la page).');
        hideAutomationBanner();
        if (jobId) {
          chrome.runtime.sendMessage({
            action: 'candidature_success',
            jobId,
            jobTitle,
            companyName,
            offerUrl: offerUrlForNotify
          });
          log('   ✅ Tuile Taleos mise à jour.');
        }
        return;
      }
      if (i < 2) await delay(2000);
    }

    log(`🔄 Connexion Firebase PROFILES...`);
    log(`✅ Identifiants trouvés pour : ${p.auth_email || '(vide)'}`);
    dumpProfile(p);

    try {
      if (phase === 2) {
        log('⏳ Attente chargement page offre (fin animation)...');
        await waitForLoadingComplete(30000);
        const cookieBtn = findCookieDismissButton();
        if (cookieBtn) { cookieBtn.click(); await delay(300); }
        const btnPostuleWait = await waitForPostuleButton(25000);
        if (btnPostuleWait) {
          log('   ✅ Bouton "Je postule" détecté, clic (même onglet).');
          btnPostuleWait.scrollIntoView?.({ behavior: 'instant', block: 'center' });
          await delay(200);
          btnPostuleWait.click();
        }
        return;
      } else if (phase === 3) {
        const successRe = new RegExp(TEXTS.successMessage.join('|'), 'i');
        const isSuccessPage = window.location.pathname.includes('candidature-validee') || successRe.test(document.body?.textContent || '');
        if (isSuccessPage) {
          hideAutomationBanner();
          log('🎉 VOTRE CANDIDATURE A ÉTÉ ENVOYÉE AVEC SUCCÈS');
          log('👉 Fermeture de l\'onglet dans 5 secondes...');
          if (jobId) {
            chrome.runtime.sendMessage({
              action: 'candidature_success',
              jobId,
              jobTitle,
              companyName,
              offerUrl: offerUrlForNotify
            });
          }
          return;
        }
        log('   ✅ Page formulaire directe détectée (pas de reload)');
      } else {
        await delay(3000);

        const cookieBtn = findCookieDismissButton();
        if (cookieBtn) { cookieBtn.click(); await delay(500); }

        const btnPostule1 = findClickablePostule();
        if (btnPostule1) {
          log('🖱️ Clic "Je postule" (1ère fois)');
          btnPostule1.scrollIntoView?.({ behavior: 'smooth', block: 'center' });
          await delay(300);
          btnPostule1.click();
          for (let i = 0; i < 6; i++) {
            await delay(500);
            const p = document.getElementById('popin-application');
            if (p && (p.classList.contains('open') || p.offsetParent !== null)) break;
          }
          await delay(500);
        }

        const popin = document.getElementById('popin-application');
        const searchRoot = (popin && (popin.classList.contains('open') || popin.offsetParent !== null)) ? popin : document;
        const loginBtn = searchRoot.querySelector('a.cta.secondary.arrow[href*="connexion"]') ||
          searchRoot.querySelector('a[href*="connexion"]') ||
          searchRoot.querySelector('a[href*="login"]') ||
          searchRoot.querySelector('a[href*="sign-in"]');
        if (loginBtn) {
          log('🔑 Connexion de l\'utilisateur...');
          if (!p.auth_email || !p.auth_password) {
            log('   ❌ Identifiants CA manquants. Configurez-les sur la page Connexions de Taleos.');
          } else {
            log(`   📧 Identifiants récupérés : ${p.auth_email}`);
            log('   📌 Stockage état avant navigation (formulaire sur page séparée)...');
            chrome.storage.local.set({
              taleos_pending_offer: {
                offerUrl,
                bankId: 'credit_agricole',
                profile: { ...p, __phase: 2, __jobId: jobId, __jobTitle: jobTitle, __companyName: companyName, __offerUrl: offerUrlForNotify },
                timestamp: Date.now()
              }
            });
            loginBtn.click();
            return;
          }
        } else {
          log('   ℹ️  Déjà connecté ou bouton connexion absent.');
        }

        log('🔄 Demande de rechargement au background...');
        chrome.runtime.sendMessage({
          action: 'reload_and_continue',
          offerUrl,
          bankId: 'credit_agricole',
          profile: { ...p, __phase: 2 }
        });
        return;
      }

      if (phase === 3) {
        log('⏳ Attente chargement formulaire...');
        await delay(5000);
        await waitForLoadingComplete(20000);
        let formReady = await waitForForm(30000);
        if (!formReady) {
          formReady = !!document.querySelector('input[id*="firstname"], input[name*="firstname"], #form-apply-firstname');
        }
        if (!formReady) {
          log('❌ Timeout: Le formulaire ne s\'est pas affiché.');
          return;
        }
        log('   ✅ Formulaire détecté (DOM).');
        log('   ⏳ Attente formulaire prêt (hydration)...');
        const hydrated = await waitForFormReady(15000);
        if (!hydrated) log('   ⚠️ Hydration partielle, remplissage quand même.');
        else log('   ✅ Formulaire prêt.');
        await runAuditAndFill(p);
        window.scrollTo(0, document.body.scrollHeight);
        await delay(1000);
        const rgpdLabel = Array.from(document.querySelectorAll('label')).find(l => {
          const t = (l.textContent || '').toLowerCase();
          return TEXTS.rgpdCheckbox.some(p => t.includes(p.toLowerCase()));
        });
        if (rgpdLabel) {
          const chk = rgpdLabel.querySelector('.checkbox-btn') || document.querySelector('.checkbox-btn:last-of-type');
          if (chk && !chk.classList.contains('checked') && !chk.classList.contains('active')) {
            chk.click();
            log('   ✅ RGPD coché.');
          }
        }
        await delay(3000);
        const submitBtn = document.getElementById('applyBtn');
        if (submitBtn && !submitBtn.disabled) {
          await chrome.storage.local.set({ taleos_success_pending: { jobId, jobTitle: jobTitle || '', companyName: companyName || 'Crédit Agricole', offerUrl: offerUrlForNotify } });
          log('🚀 FINALISATION');
          log('   🔎 Recherche RGPD (Méthode Robuste)...');
          log('   ⏳ Attente de sécurité de 3 SECONDES avant envoi...');
          log('🚀 CLIC FINAL : ENVOI DE LA CANDIDATURE...');
          submitBtn.click();
          log('⏳ Attente du message de confirmation (Timeout 45s)...');
          const success = await waitForSuccessMessage(45000);
          if (success) {
            console.log('\n' + '✅'.repeat(35));
            log('🎉 VOTRE CANDIDATURE A ÉTÉ ENVOYÉE AVEC SUCCÈS');
            log('👉 Fermeture de l\'onglet dans 5 secondes...');
            console.log('✅'.repeat(35) + '\n');
            if (jobId) {
              chrome.runtime.sendMessage({
                action: 'candidature_success',
                jobId,
                jobTitle,
                companyName,
                offerUrl: offerUrlForNotify
              });
            }
          } else {
            log('⚠️ Message de succès non détecté (timeout).');
          }
        } else {
          log('❌ Bouton Envoyer introuvable ou grisé.');
        }
        log('💤 Fin du script.');
        return;
      }

      const dejaCandidat = TEXTS.alreadyApplied.some(t => document.body.textContent.toLowerCase().includes(t.toLowerCase()));
      if (dejaCandidat) {
        log('🛑 Déjà candidaté à cette offre.');
        return;
      }

      const formAlreadyVisible = document.getElementById('form-apply-firstname')?.offsetParent != null;
      if (!formAlreadyVisible) {
        const btnPostule2 = findClickablePostule();
        if (btnPostule2) {
          log('🖱️ Clic "Je postule" (2ème fois - après login)');
          btnPostule2.scrollIntoView?.({ behavior: 'smooth', block: 'center' });
          await delay(500);
          btnPostule2.click();
          await delay(2000);
        } else {
          log('❌ Bouton "Je postule" introuvable.');
          return;
        }
      } else {
        log('   ✅ Formulaire déjà visible (redirection directe après login)');
      }

      log('⏳ Attente chargement formulaire (fin animation)...');
      await waitForLoadingComplete(30000);
      let formReady = formAlreadyVisible || await waitForForm(15000);
      if (!formReady) {
        log('   ⚠️ Formulaire non détecté. Nouveau clic "Je postule"...');
        const btnRetry = findClickablePostule();
        if (btnRetry) {
          btnRetry.scrollIntoView?.({ behavior: 'smooth', block: 'center' });
          await delay(500);
          btnRetry.click();
          await delay(2000);
          formReady = await waitForForm(30000);
        }
      }
      if (!formReady) {
        log('❌ Timeout: Le formulaire ne s\'est pas affiché.');
        return;
      }
      log('   ✅ Formulaire détecté (DOM).');
      log('   ⏳ Attente formulaire prêt (hydration)...');
      await waitForFormReady(25000);
      log('   ✅ Formulaire prêt.');

      await runAuditAndFill(p);

      window.scrollTo(0, document.body.scrollHeight);
      await delay(1000);

      const rgpdLabel2 = Array.from(document.querySelectorAll('label')).find(l => {
        const t = (l.textContent || '').toLowerCase();
        return TEXTS.rgpdCheckbox.some(p => t.includes(p.toLowerCase()));
      });
      if (rgpdLabel2) {
        const chk = rgpdLabel2.querySelector('.checkbox-btn') || document.querySelector('.checkbox-btn:last-of-type');
        if (chk && !chk.classList.contains('checked') && !chk.classList.contains('active')) {
          chk.click();
          log('   ✅ RGPD Coché (Via Label Texte)');
        }
      }

      await delay(3000);
      const submitBtn = document.getElementById('applyBtn');
      if (submitBtn && !submitBtn.disabled) {
        await chrome.storage.local.set({ taleos_success_pending: { jobId, jobTitle: jobTitle || '', companyName: companyName || 'Crédit Agricole', offerUrl: offerUrlForNotify } });
        log('🚀 FINALISATION');
        log('   🔎 Recherche RGPD (Méthode Robuste)...');
        log('   ⏳ Attente de sécurité de 3 SECONDES avant envoi...');
        log('🚀 CLIC FINAL : ENVOI DE LA CANDIDATURE...');
        submitBtn.click();
        log('⏳ Attente du message de confirmation (Timeout 45s)...');
        const success = await waitForSuccessMessage(45000);
        if (success) {
          console.log('\n' + '✅'.repeat(35));
          log('🎉 VOTRE CANDIDATURE A ÉTÉ ENVOYÉE AVEC SUCCÈS');
          log('👉 Message détecté : \'Votre candidature a été envoyée avec succès\'');
          console.log('✅'.repeat(35) + '\n');
          if (jobId) {
            chrome.runtime.sendMessage({
              action: 'candidature_success',
              jobId,
              jobTitle,
              companyName,
              offerUrl: offerUrlForNotify
            });
          }
        } else {
          log('⚠️ Message de succès non détecté (timeout).');
        }
      } else {
        log('❌ Bouton Envoyer introuvable ou grisé.');
      }
      log('💤 Fin du script.');
    } catch (e) {
      log(`❌ Erreur: ${e.message}`);
      console.error(e);
    } finally {
      hideAutomationBanner();
    }
  }

  window.__taleosRun = function(profile) {
    main(profile).catch(e => console.error('[Taleos CA]', e));
  };
})();
