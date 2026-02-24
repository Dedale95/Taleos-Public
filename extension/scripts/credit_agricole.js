/**
 * Taleos - Automatisation Crédit Agricole (groupecreditagricole.jobs)
 * Logique conforme au notebook Python : Je postule → Connexion → Identifiants → Reload → Je postule → Formulaire
 */

(function() {
  'use strict';

  const delay = ms => new Promise(r => setTimeout(r, ms));
  const offerUrl = window.location.href;

  function log(msg) {
    const t = new Date().toLocaleTimeString('fr-FR');
    console.log(`[${t}] [Taleos CA] ${msg}`);
  }

  function findText(selector, text) {
    const els = document.querySelectorAll(selector || '*');
    return Array.from(els).find(el => (el.textContent || '').includes(text));
  }

  function safeFill(id, value, label) {
    const el = document.getElementById(id);
    if (!el || !value) return Promise.resolve();
    const current = (el.value || '').trim();
    const target = String(value).trim();
    if (current === target) {
      log(`   ✅ ${label} : Déjà correct -> Skip`);
      return Promise.resolve();
    }
    log(`   ✏️  ${label} : '${current}' -> '${target}'`);
    el.value = target;
    el.dispatchEvent(new Event('input', { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
    return Promise.resolve();
  }

  function getCleanListFromText(raw) {
    if (!raw || raw.includes('Sélectionnez')) return [];
    return raw.split(',').map(p => p.replace(/\s*\(\d+\)/g, '').trim().toLowerCase()).filter(Boolean);
  }

  async function auditCombobox(ariaId, expectedVal, label) {
    if (!expectedVal) return;
    const trigger = document.querySelector(`div[aria-controls="${ariaId}"], button[aria-controls="${ariaId}"]`);
    if (!trigger) return;
    const current = (trigger.textContent || '').trim();
    if (expectedVal.toLowerCase().includes(current.toLowerCase()) && !current.includes('Sélectionnez')) {
      log(`   ✅ ${label} : Déjà correct -> Skip`);
      return;
    }
    log(`   ✏️  ${label} : '${current}' -> '${expectedVal}'`);
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
      log(`   ✅ ${label} : Déjà synchronisé -> Skip`);
      return;
    }
    log(`   ⚙️  ${label} : ${curSel.join(',')} -> ${expClean.join(',')}`);
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
        lbl.click();
        await delay(100);
      } else if (!isChecked && shouldBe) {
        lbl.click();
        await delay(100);
      }
    }
    btn.click();
    await delay(300);
  }

  async function setFileInput(inputId, fileUrl) {
    if (!fileUrl) return;
    const input = document.getElementById(inputId);
    if (!input) return;
    try {
      const res = await fetch(fileUrl, { mode: 'cors' });
      const blob = await res.blob();
      const file = new File([blob], 'cv.pdf', { type: blob.type || 'application/pdf' });
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

    log('📂 [1/4] Infos Personnelles');
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

    log('📂 [2/4] Documents');
    const acc1 = document.querySelector("button[aria-controls='accordion-item-1']");
    if (acc1 && acc1.getAttribute('aria-expanded') === 'false') {
      acc1.click();
      await delay(1000);
    }
    const cvInput = document.getElementById('form-apply-cv');
    const cvContainer = cvInput?.parentElement;
    const hasCv = cvInput?.files?.length > 0 || (cvContainer?.textContent || '').toLowerCase().includes('.pdf');
    if (p.cv_url && !hasCv) {
      log('   ⚠️ CV manquant. Upload...');
      await setFileInput('form-apply-cv', p.cv_url);
      await delay(3000);
    } else log('   ✅ CV : Présent -> Skip');
    const lmInput = document.getElementById('form-apply-lm');
    const hasLm = lmInput?.files?.length > 0 || (lmInput?.parentElement?.textContent || '').toLowerCase().includes('.pdf');
    if (p.lm_url && !hasLm) {
      log('   ⚠️ LM manquante. Upload...');
      await setFileInput('form-apply-lm', p.lm_url);
      await delay(2000);
    } else log('   ✅ LM : Présente -> Skip');
    const nb2 = nextBtn();
    if (nb2) { nb2.click(); await delay(2000); }

    log('📂 [3/4] Critères');
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

    log('📂 [4/4] Formation');
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
    for (let i = 0; i < (p.languages || []).length; i++) {
      const lang = p.languages[i];
      if (!lang?.name) continue;
      const langTrigger = document.querySelector(`div[aria-controls='customSelect-language-${i + 1}']`);
      const levelTrigger = document.querySelector(`div[aria-controls='customSelect-language-level-${i + 1}']`);
      if (i > 0 && !langTrigger) {
        const addBtn = document.getElementById('add-language-btn');
        if (addBtn) { addBtn.click(); await delay(1000); }
      }
      if (langTrigger) {
        const cur = (langTrigger.textContent || '').trim();
        if (!cur.includes(lang.name) || cur.includes('Sélectionnez')) {
          langTrigger.click();
          await delay(500);
          const panel = document.getElementById(`customSelect-language-${i + 1}`);
          const labels = panel?.querySelectorAll('label') || [];
          for (const lbl of labels) {
            if ((lbl.textContent || '').includes(lang.name)) { lbl.click(); break; }
          }
        }
      }
      if (levelTrigger) {
        const cur = (levelTrigger.textContent || '').trim();
        if (!cur.includes(lang.level) || cur.includes('Sélectionnez')) {
          levelTrigger.click();
          await delay(500);
          const panel = document.getElementById(`customSelect-language-level-${i + 1}`);
          const opts = panel?.querySelectorAll('label') || [];
          for (const o of opts) {
            if ((o.textContent || '').includes(lang.level)) { o.click(); break; }
          }
        }
      }
    }
    const nb4 = nextBtn();
    if (nb4) { nb4.click(); await delay(2000); }
  }

  async function waitForForm(maxWait = 35000) {
    const start = Date.now();
    while (Date.now() - start < maxWait) {
      const el = document.getElementById('form-apply-firstname');
      if (el && el.offsetParent !== null) return true;
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

  async function main(profile) {
    const phase = profile.__phase;
    const p = { ...profile };
    delete p.__phase;

    log(phase === 2 ? '🚀 PHASE 2 : Formulaire (après reload)' : '🚀 DÉMARRAGE BOT CRÉDIT AGRICOLE');
    log(`🔗 URL : ${offerUrl}`);

    try {
      if (phase === 2) {
        await delay(3000);
      } else {
        await delay(3000);

        const rgpd = document.querySelector('button.rgpd-btn-refuse');
        if (rgpd) { rgpd.click(); await delay(500); }

        const btnPostule1 = findText('*', 'Je postule');
        if (btnPostule1) {
          log('🖱️ Clic "Je postule" (1ère fois)');
          btnPostule1.click();
          await delay(2000);
        }

        const loginBtn = document.querySelector('a.cta.secondary.arrow[href*="connexion"]');
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
                profile: { ...p, __phase: 2 },
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

      const dejaCandidat = document.body.textContent.includes('Suivre ma candidature');
      if (dejaCandidat) {
        log('🛑 Déjà candidaté à cette offre.');
        return;
      }

      const btnPostule2 = findText('*', 'Je postule');
      if (btnPostule2) {
        log('🖱️ Clic "Je postule" (2ème fois - après login)');
        btnPostule2.click();
      } else {
        log('❌ Bouton "Je postule" introuvable.');
        return;
      }

      log('⏳ Attente chargement formulaire...');
      const formReady = await waitForForm(30000);
      if (!formReady) {
        log('❌ Timeout: Le formulaire ne s\'est pas affiché.');
        return;
      }
      log('   ✅ Formulaire détecté.');

      log('   ⏳ Pause 20s (hydration)...');
      await delay(20000);

      await runAuditAndFill(p);

      window.scrollTo(0, document.body.scrollHeight);
      await delay(1000);

      const rgpdLabel = Array.from(document.querySelectorAll('label')).find(l => (l.textContent || '').includes('Je déclare avoir lu'));
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
        log('🚀 ENVOI CANDIDATURE...');
        submitBtn.click();
      } else {
        log('❌ Bouton Envoyer introuvable ou grisé.');
      }
    } catch (e) {
      log(`❌ Erreur: ${e.message}`);
      console.error(e);
    }
  }

  window.__taleosRun = function(profile) {
    main(profile).catch(e => console.error('[Taleos CA]', e));
  };
})();
