/**
 * Taleos - Automatisation Société Générale (socgen.taleo.net)
 * Flux : Login → Reset draft (si présent) → Skip étapes → Profil → CV → Envoi
 */
(function() {
  'use strict';

  const delay = ms => new Promise(r => setTimeout(r, ms));
  const DEBUG = false; // true = logs détaillés (iframe, attentes, etc.)

  function log(msg) {
    if (!DEBUG && window !== window.top) return; // Pas de log depuis l'iframe
    const t = new Date().toLocaleTimeString('fr-FR');
    console.log(`[${t}] [Taleos SG] ${msg}`);
  }

  const BANNER_ID = 'taleos-sg-automation-banner';
  function showBanner() {
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

  function getSearchRoots() {
    const roots = [document];
    try {
      if (window.parent !== window && window.parent.document) roots.push(window.parent.document);
      if (window.top !== window && window.top.document && !roots.includes(window.top.document)) roots.push(window.top.document);
      const iframes = document.querySelectorAll('iframe');
      for (const f of iframes) {
        try {
          if (f.contentDocument && !roots.includes(f.contentDocument)) roots.push(f.contentDocument);
        } catch (_) {}
      }
      if (window.top?.document) {
        const topIframes = window.top.document.querySelectorAll?.('iframe') || [];
        for (const f of topIframes) {
          try {
            if (f.contentDocument && !roots.includes(f.contentDocument)) roots.push(f.contentDocument);
          } catch (_) {}
        }
      }
    } catch (_) {}
    return roots;
  }

  function findByIdContains(partialId) {
    const sel = `input[id*="${partialId}"], input[name*="${partialId}"], a[id*="${partialId}"]`;
    for (const root of getSearchRoots()) {
      const el = root.querySelector(sel);
      if (el) return el;
    }
    return null;
  }

  /** Fallback : trouve un input par le texte du label associé (Prénom, Nom, Email, etc.) */
  function findInputByLabelPatterns(patterns) {
    const re = new RegExp(patterns.join('|'), 'i');
    for (const root of getSearchRoots()) {
      const labels = root.querySelectorAll?.('label, td, th, span, div[class*="label"]') || [];
      for (const lbl of labels) {
        const txt = (lbl.textContent || '').trim();
        if (!re.test(txt) || txt.length > 80) continue;
        const inp = lbl.querySelector?.('input[type="text"], input:not([type])') ||
          (lbl.getAttribute?.('for') ? root.querySelector?.(`input[id="${lbl.getAttribute('for')}"]`) : null) ||
          lbl.nextElementSibling?.querySelector?.('input') ||
          lbl.closest?.('tr')?.querySelector?.('input[type="text"], input:not([type])') ||
          lbl.closest?.('div')?.querySelector?.('input[type="text"], input:not([type])');
        if (inp && inp.offsetParent !== null) return inp;
      }
    }
    return null;
  }

  /** Trouve les champs du formulaire via le bloc contenant "Informations personnelles" ou "Merci de vérifier" */
  function findProfileInputsInStep2Block() {
    for (const root of getSearchRoots()) {
      const all = root.querySelectorAll?.('form, div[class*="content"], div[class*="form"], div[class*="edit"], section') || [];
      for (const el of all) {
        const txt = (el.textContent || '').slice(0, 500);
        if (!/informations personnelles|personal information|merci de vérifier|vérifier et compléter/i.test(txt)) continue;
        const inputs = el.querySelectorAll?.('input[type="text"], input:not([type])') || [];
        const visible = Array.from(inputs).filter(i => i.offsetParent !== null && !/hidden|search|login|password/i.test(i.id || i.name || ''));
        if (visible.length >= 3) return visible;
      }
    }
    return null;
  }

  function findAllByIdContains(partialId) {
    return Array.from(document.querySelectorAll(`input[id*="${partialId}"], input[name*="${partialId}"]`));
  }

  async function setFileInputFromStorage(inputEl, storagePath, filename) {
    if (!inputEl || !storagePath) return false;
    try {
      const r = await new Promise(resolve => {
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
      log(`❌ Erreur upload fichier: ${e.message}`);
      return false;
    }
  }

  function dismissCookieBanner() {
    try {
      const host = document.querySelector('#didomi-host');
      const btn = host?.shadowRoot?.querySelector('#didomi-notice-disagree-button') ||
        document.querySelector('#didomi-notice-disagree-button') ||
        document.querySelector('button[class*="didomi"]');
      if (btn && btn.offsetParent !== null) {
        btn.click();
        return true;
      }
      document.body.style.setProperty('overflow', 'auto', 'important');
    } catch (_) {}
    return false;
  }

  function is404OfferPage() {
    for (const root of getSearchRoots()) {
      const txt = (root.body?.innerText || root.body?.innerHTML || root.documentElement?.innerHTML || '').toLowerCase();
      if (/page not found|error 404|job position is no longer online|the requested page no longer exists/i.test(txt)) return true;
    }
    return false;
  }

  function isSuccessPage() {
    for (const root of getSearchRoots()) {
      const txt = (root.body?.innerText || root.body?.innerHTML || '').toLowerCase();
      if (/dans la boîte|votre candidature.*boîte|recruteurs va l'étudier|accéder à votre profil/i.test(txt)) return true;
      if (/your submission|thank you!|thank you for your submission/i.test(txt)) return true;
    }
    return false;
  }

  async function main(profile) {
    if (DEBUG) log(`main() - frame: ${window === window.top ? 'main' : 'iframe'}`);
    if (isSuccessPage()) {
      const jobId = profile?.__jobId || profile?.jobId || '';
      const jobTitle = profile?.__jobTitle || profile?.jobTitle || '';
      const companyName = profile?.__companyName || profile?.companyName || 'Société Générale';
      const offerUrl = profile?.__offerUrl || profile?.offerUrl || '';
      log('🎉 Page succès détectée — Candidature envoyée avec succès !');
      if (jobId || offerUrl) {
        try {
          chrome.runtime.sendMessage({ action: 'candidature_success', jobId, jobTitle, companyName, offerUrl });
        } catch (_) {}
      }
      return;
    }
    if (is404OfferPage()) {
      log('⛔ Offre non disponible (404) — arrêt de l\'automatisation.');
      try {
        chrome.storage.local.remove(['taleos_pending_sg', 'taleos_sg_tab_id']);
        const jobId = profile?.__jobId || profile?.jobId || '';
        const offerUrl = profile?.__offerUrl || profile?.offerUrl || '';
        if (jobId || offerUrl) {
          chrome.runtime.sendMessage({
            action: 'candidature_failure',
            jobId,
            offerUrl,
            error: 'Offre non disponible (404) — L\'offre n\'est plus en ligne.'
          });
        }
      } catch (_) {}
      const banner = document.createElement('div');
      banner.id = BANNER_ID;
      banner.innerHTML = '⛔ Offre non disponible (404) — L\'offre n\'est plus en ligne. Candidature annulée.';
      Object.assign(banner.style, {
        position: 'fixed', top: '0', left: '0', right: '0', zIndex: '2147483647',
        background: 'linear-gradient(135deg, #c53030 0%, #9b2c2c 100%)', color: 'white',
        padding: '12px 20px', fontSize: '14px', fontWeight: '600', textAlign: 'center',
        fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
        boxShadow: '0 2px 10px rgba(0,0,0,0.2)'
      });
      const root = document.body || document.documentElement;
      if (root) (root.firstChild ? root.insertBefore(banner, root.firstChild) : root.appendChild(banner));
      return;
    }
    if (window !== window.top) {
      await delay(2000);
      const hasForm = !!(findByIdContains('FirstName') || findByIdContains('personal_info_FirstName'));
      if (!hasForm) return;
    }
    const loginName = document.querySelector('#dialogTemplate-dialogForm-login-name1') ||
      findByIdContains('login-name1') || findByIdContains('login-name');
    const loginPass = document.querySelector('#dialogTemplate-dialogForm-login-password') ||
      findByIdContains('login-password');
    const hasLoginForm = loginName && loginPass && loginName.offsetParent !== null && loginPass.offsetParent !== null;

    const url = (window.location?.href || '').toLowerCase();
    const isJobapply = url.includes('jobapply') || (window === window.top && url.includes('socgen.taleo.net'));
    if (isJobapply && !hasLoginForm) {
      for (let w = 0; w < 5; w++) {
        const hasForm = findByIdContains('FirstName') || findByIdContains('personal_info_FirstName') ||
          findByIdContains('saveContinueCmdBottom');
        if (hasForm) break;
        if (DEBUG) log(`   ⏳ Attente formulaire (${w + 1}/5)`);
        await delay(1500);
      }
    }
    showBanner();
    const jobId = profile.__jobId || '';
    const jobTitle = profile.__jobTitle || '';
    const companyName = profile.__companyName || 'Société Générale';
    const offerUrl = profile.__offerUrl || '';

    const urlStart = (window.location?.href || '').toLowerCase();
    const isTaleoPage = urlStart.includes('socgen.taleo.net');
    if (!urlStart.includes('flow.jsf')) {
      log('🚀 DÉMARRAGE AUTOMATISATION SOCIÉTÉ GÉNÉRALE');
      try {
        sessionStorage.removeItem('taleos_sg_profile_filled');
        if (!isTaleoPage) {
          sessionStorage.removeItem('taleos_sg_navigate_profile_attempted');
          chrome.storage.local.remove('taleos_sg_navigate_profile_attempted');
        }
      } catch (_) {}
    }

    try {
      dismissCookieBanner();
      await delay(800);

      const loginName = document.querySelector('#dialogTemplate-dialogForm-login-name1') ||
        findByIdContains('login-name1') || findByIdContains('login-name');
      const loginPass = document.querySelector('#dialogTemplate-dialogForm-login-password') ||
        findByIdContains('login-password');
      const loginSubmit = document.querySelector('#dialogTemplate-dialogForm-login-defaultCmd') ||
        document.querySelector('input[id*="login-defaultCmd"]');

      if (loginName && loginPass && profile.auth_email && profile.auth_password) {
        log('🔑 Connexion Taleo...');
        loginName.focus();
        loginName.value = profile.auth_email;
        loginName.dispatchEvent(new Event('input', { bubbles: true }));
        await delay(100);
        loginPass.value = profile.auth_password;
        loginPass.dispatchEvent(new Event('input', { bubbles: true }));
        await delay(200);
        if (loginSubmit) {
          loginSubmit.click();
          log('   ✅ Connexion envoyée.');
        }
        await delay(6000);
      }

      function isFlowPage() {
        const url = (window.top?.location?.href || window.location?.href || '').toLowerCase();
        return url.includes('flow.jsf');
      }

      // Ne faire le reset que depuis la frame principale (éviter que l'iframe about:blank déclenche un reset en boucle)
      let resetLink = null;
      if (window === window.top && !isFlowPage()) {
        for (const root of getSearchRoots()) {
          resetLink = root.querySelector?.('a[id*="dtGotoPageLink"]');
          if (resetLink) break;
        }
        if (resetLink && resetLink.offsetParent !== null) {
          log('🔄 Reset du formulaire (draft détecté)...');
          resetLink.click();
          await delay(5000);
        }
      }

      function isPiecesJointesPage() {
        const profileForm = findByIdContains('personal_info_FirstName') || findByIdContains('FirstName');
        if (profileForm && profileForm.offsetParent !== null) return false;
        for (const root of getSearchRoots()) {
          const table = root.querySelector?.('table.attachment-list');
          const upload = root.querySelector?.('input[id*="uploadedFile"], input[id*="attachFileCommand"]');
          if ((table && table.offsetParent !== null) || (upload && upload.offsetParent !== null)) return true;
        }
        return false;
      }

      function hasProfileFormVisible() {
        const fn = findByIdContains('personal_info_FirstName') || findByIdContains('FirstName');
        if (fn && fn.offsetParent !== null) return true;
        return false;
      }

      /** Détecte la page courante via les liens dtGotoPageLink (sélectionné = pas command-link-visited ou parent .selected/.current) */
      function getCurrentStepFromNav() {
        for (const root of getSearchRoots()) {
          const links = root.querySelectorAll?.('a[id*="dtGotoPageLink"]') || [];
          for (const a of links) {
            const t = ((a.title || a.textContent || '').toLowerCase());
            const parent = a.closest?.('td, li, div');
            const isSelected = parent?.classList?.contains?.('selected') || parent?.classList?.contains?.('current') ||
              parent?.classList?.contains?.('active') || !a.classList?.contains?.('command-link-visited');
            if (!isSelected) continue;
            if (/informations personnelles|personal information/i.test(t)) return 'informations';
            if (/pièces jointes|attachments|document/i.test(t)) return 'pieces';
            if (/vérifier et postuler|review and submit|submit/i.test(t)) return 'verifier';
          }
        }
        return null;
      }

      /** Fallback : détecte l'étape 2 via le texte visible de la page */
      function isInformationsPersonnellesPage() {
        for (const root of getSearchRoots()) {
          const txt = ((root.title || '') + ' ' + (root.body?.innerText || '') + ' ' + (root.body?.innerHTML || '')).toLowerCase();
          if (/informations personnelles|personal information|persönliche angaben|información personal|informazioni personali/i.test(txt)) return true;
        }
        return false;
      }

      function findFlowStartOrContinueBtn() {
        const exclude = (el) => {
          if (el.closest?.('table')) return true;
          const t = ((el.textContent || el.innerText || el.value || '').trim()).toLowerCase();
          return /\.pdf|erreur|error|supprimer|delete|resume -/i.test(t);
        };
        for (const root of getSearchRoots()) {
          const byId = root.querySelector?.('input[id*="saveContinueCmdBottom"]');
          if (byId && byId.offsetParent !== null && !exclude(byId)) return byId;
          const btns = root.querySelectorAll?.('input[type="button"][value*="Sauvegarder"], input[type="submit"][value*="Sauvegarder"], button') || [];
          for (const el of btns) {
            if (!el || el.offsetParent === null || exclude(el)) continue;
            const t = ((el.value || el.textContent || '').trim()).toLowerCase();
            if (/sauvegarder et continuer|save and continue/i.test(t)) return el;
          }
        }
        return null;
      }

      const step2Active = getCurrentStepFromNav() === 'informations' || hasProfileFormVisible() || isInformationsPersonnellesPage();
      if (window === window.top && isFlowPage() && !isPiecesJointesPage() && !step2Active) {
        if (DEBUG) log('   Page flow.jsf (après reset)');
        await delay(2000);
        const flowBtn = findFlowStartOrContinueBtn();
        if (flowBtn) {
          if (DEBUG) log('   Clic démarrer flux');
          flowBtn.scrollIntoView({ behavior: 'instant', block: 'center' });
          await delay(500);
          flowBtn.click();
          await delay(5000);
        } else {
          log('   ⚠️ Aucun bouton "Commencer/Continuer" trouvé sur flow.jsf.');
        }
      }

      function findDisclaimerCheckbox() {
        for (const root of getSearchRoots()) {
          const cbs = root.querySelectorAll?.('input[type="checkbox"]') || [];
          for (const cb of cbs) {
            const label = cb.closest('label') || root.querySelector?.(`label[for="${cb.id}"]`);
            const txt = ((label?.textContent || cb.getAttribute('aria-label') || '') + (cb.value || '')).toLowerCase();
            if (/accept|accepter|agree|consent|disclaimer|déclaration|confirm/i.test(txt) && cb.offsetParent !== null) return cb;
          }
        }
        return null;
      }

      function isConfidentialityAgreementPage() {
        const txt = (document.title + ' ' + (document.body?.innerText || '') + ' ' + (document.body?.innerHTML || '')).toLowerCase();
        return /accord de confidentialit|confidentiality agreement|vertraulichkeitsvereinbarung|acuerdo de confidencialidad|accordo di riservatezza/i.test(txt);
      }

      function findDisclaimerOrContinueBtn() {
        for (const root of getSearchRoots()) {
          const exactIds = [
            'et-ef-content-flowTemplate-LegalDisclaimerPage-legalDisclaimerContinueButton',
            'legalDisclaimerContinueButton'
          ];
          for (const id of exactIds) {
            const el = root.getElementById?.(id) || root.querySelector?.(`input[id*="${id}"], button[id*="${id}"]`);
            if (el && el.offsetParent !== null) return el;
          }
          const byId = [
            'legalDisclaimerAcceptButton',
            'legalDisclaimer',
            'saveContinueCmdBottom',
            'legalDisclaimerContinue',
            'disclaimerContinue'
          ];
          for (const partial of byId) {
            const el = root.querySelector?.(`input[id*="${partial}"], button[id*="${partial}"], a[id*="${partial}"]`);
            if (el && el.offsetParent !== null) return el;
          }
          const byValue = [
            "J'ai lu", "I have read", "I've read", "I read", "Read", "Lu", "Gelesen", "He leído", "Ho letto",
            'Continue', 'Continuer', 'Accept', 'Accepter', "J'accepte", 'I accept', 'Next', 'Suivant'
          ];
          for (const val of byValue) {
            const el = root.querySelector?.(`input[value="${val}"], input[value*="${val}"], button[value="${val}"], button[value*="${val}"]`);
            if (el && el.offsetParent !== null) return el;
          }
          const readBtns = root.querySelectorAll?.('input[type="button"][value], input[type="submit"][value], button[value]') || [];
          for (const b of readBtns) {
            const t = ((b.value || b.textContent || b.title || '') + '').toLowerCase();
            if (/\b(read|lu|gelesen|leído|letto)\b/i.test(t) && b.offsetParent !== null) return b;
          }
          const btns = root.querySelectorAll?.('input[type="submit"], input[type="button"], button, a[role="button"]') || [];
          for (const b of btns) {
            const t = ((b.value || b.textContent || '').trim()).toLowerCase();
            if (/continue|continuer|accept|accepter|suivant|next|valider|submit/i.test(t) && b.offsetParent !== null) return b;
          }
          const exactTexts = ["j'ai lu", "i have read", "gelesen", "he leído", "ho letto"];
          const clickables = root.querySelectorAll?.('a, button, input[type="button"], input[type="submit"], span[role="button"]') || [];
          for (const el of clickables) {
            if (!el || el.offsetParent === null) continue;
            const t = ((el.value || el.textContent || el.innerText || '').trim()).toLowerCase();
            if (exactTexts.some(txt => t.includes(txt))) return el;
          }
        }
        return null;
      }

      function navigateToProfileStep() {
        for (const root of getSearchRoots()) {
          const links = root.querySelectorAll?.('a[id*="dtGotoPageLink"]') || [];
          for (const a of links) {
            const t = ((a.title || a.textContent || '').toLowerCase());
            if (!/informations personnelles|personal information|persönliche|información personal/i.test(t) || a.offsetParent === null) continue;
            const formId = a.id?.includes('editTemplateMultipart') ? 'editTemplateMultipart-editForm' : 'et-ef';
            if (typeof window.cmdSubmit === 'function' && a.id) {
              try { window.cmdSubmit(formId, a.id, null, null, true); } catch (_) { a.click(); }
            } else {
              a.click();
            }
            return true;
          }
        }
        return false;
      }

      const currentStep = getCurrentStepFromNav();
      const profileFilled = !!sessionStorage.getItem('taleos_sg_profile_filled');
      let navigateAttempted = !!sessionStorage.getItem('taleos_sg_navigate_profile_attempted');
      if (!navigateAttempted) {
        const stored = await chrome.storage.local.get('taleos_sg_navigate_profile_attempted');
        navigateAttempted = !!stored.taleos_sg_navigate_profile_attempted;
      }
      const onPiecesJointes = currentStep === 'pieces' || (isPiecesJointesPage() && !hasProfileFormVisible());
      const shouldTryNavigate = isFlowPage() && isPiecesJointesPage() && !hasProfileFormVisible() && !profileFilled && !navigateAttempted && !onPiecesJointes;
      if (shouldTryNavigate) {
        try {
          sessionStorage.setItem('taleos_sg_navigate_profile_attempted', '1');
          await chrome.storage.local.set({ taleos_sg_navigate_profile_attempted: '1' });
        } catch (_) {}
        if (navigateToProfileStep()) {
          log('📑 Navigation vers "Informations personnelles"...');
          await delay(4000);
        }
      } else if (isFlowPage() && isPiecesJointesPage() && !hasProfileFormVisible() && !profileFilled) {
        const onStep2 = currentStep === 'informations' || isInformationsPersonnellesPage();
        if (!onStep2) {
          try {
            sessionStorage.setItem('taleos_sg_navigate_profile_attempted', '1');
            await chrome.storage.local.set({ taleos_sg_navigate_profile_attempted: '1' });
          } catch (_) {}
          log('   ⏭️  [2/4] Informations personnelles : Non atteinte (draft) — passage direct au CV.');
        }
      }
      if (DEBUG) log('📋 Validation disclaimer...');
      for (let wait = 0; wait < 8; wait++) {
        if (hasProfileFormVisible() || isPiecesJointesPage()) break;
        await delay(1500);
      }
      let confidentialityPageLogged = false;
      for (let i = 0; i < 25; i++) {
        if (step2Active) break;
        const firstNameInput = findByIdContains('personal_info_FirstName') || findByIdContains('FirstName');
        if (firstNameInput && firstNameInput.offsetParent !== null) break;
        if (isPiecesJointesPage()) break;
        if (isConfidentialityAgreementPage() && !confidentialityPageLogged) {
          confidentialityPageLogged = true;
        }
        const chk = findDisclaimerCheckbox();
        if (chk && !chk.checked) {
          chk.click();
          await delay(1500);
        }
        const btn = findDisclaimerOrContinueBtn();
        if (btn) {
          if (DEBUG) log(`   Clic disclaimer ${i + 1}`);
          btn.scrollIntoView({ behavior: 'instant', block: 'center' });
          await delay(300);
          btn.focus();
          if (typeof window.cmdSubmit === 'function' && btn.id) {
            try { window.cmdSubmit('et-ef', btn.id, null, null, true); } catch (_) { btn.click(); }
          } else {
            btn.click();
          }
          await delay(4500);
        } else {
          await delay(2000);
        }
      }

      function auditAndSetInput(inputEl, firebaseVal, label) {
        if (!inputEl) {
          if (firebaseVal) log(`   ⚠️ ${label} : Champ non trouvé (Firebase: "${firebaseVal}")`);
          return;
        }
        const current = (inputEl.value || '').trim();
        const expected = (firebaseVal || '').trim();
        const fb = expected || '(vide)';
        const form = current || '(vide)';
        if (expected === current) {
          log(`   ✅ ${label} : Déjà correct (Firebase identique) → Skip`);
          return;
        }
        log(`   ✏️ ${label} : Remplacer "${form}" par "${fb}" (Firebase)`);
        inputEl.value = expected;
        inputEl.dispatchEvent(new Event('input', { bubbles: true }));
        inputEl.dispatchEvent(new Event('change', { bubbles: true }));
      }

      async function auditAndSetCivility(expectedVal) {
        const expected = (expectedVal || '').trim();
        if (!expected) return;
        let chosenSpan = null, sel = null;
        for (const root of getSearchRoots()) {
          chosenSpan = chosenSpan || root.querySelector?.('#select2-chosen-1, .select2-chosen[id*="chosen"], span.select2-chosen');
          sel = sel || root.querySelector?.('select[id*="PersonalTitle"], select[name*="PersonalTitle"], select[id*="Title"][id*="personal"], select[id*="civility"]') ||
            Array.from(root.querySelectorAll?.('select') || []).find(s => {
              const opts = Array.from(s.options || []);
              return opts.some(o => /monsieur|madame/i.test(o.text || o.value));
            });
        }
        const current = (chosenSpan?.textContent || '').trim();
        const fb = expected || '(vide)';
        const form = current || '(vide)';
        if (current && expected && (current.toLowerCase() === expected.toLowerCase() || current.toLowerCase().includes(expected.toLowerCase()))) {
          log(`   ✅ Civilité : Déjà correct (${form} = Firebase) → Skip`);
          return;
        }
        if (!sel) {
          if (DEBUG) log('   Civilité: select non trouvé');
          return;
        }
        const opt = Array.from(sel.options || []).find(o =>
          (o.text || '').trim().toLowerCase() === expected.toLowerCase() ||
          (o.value || '').trim().toLowerCase() === expected.toLowerCase()
        );
        if (opt) {
          log(`   ✏️ Civilité : Remplacer "${form}" par "${fb}" (Firebase)`);
          sel.value = opt.value;
          sel.dispatchEvent(new Event('change', { bubbles: true }));
          if (typeof jQuery !== 'undefined' && jQuery(sel).data('select2')) {
            try { jQuery(sel).trigger('change'); } catch (_) {}
          }
        } else {
          const container = sel.closest('.select2-container') || document.querySelector('.select2-container');
          if (container) {
            log(`   ✏️ Civilité : Remplacer "${form}" par "${expected}" (Firebase, select2)`);
            container.click();
            await delay(600);
            const li = Array.from(document.querySelectorAll('.select2-results li, .select2-result, [id*="select2-result"]')).find(el =>
              (el.textContent || '').trim().toLowerCase().includes(expected.toLowerCase())
            );
            if (li) {
              li.click();
              await delay(400);
            } else {
              if (DEBUG) log(`   Civilité: option "${expected}" non trouvée`);
            }
          } else {
            if (DEBUG) log(`   Civilité: option "${expected}" non trouvée`);
          }
        }
      }

      async function syncJobFamilyField(sgFamilies) {
        if (!sgFamilies || sgFamilies.length === 0) return;
        const targets = sgFamilies.map(s => String(s || '').trim().toUpperCase()).filter(Boolean);
        const norm = (t) => (t || '').replace(/\s+/g, ' ').trim().toUpperCase();
        for (const root of getSearchRoots()) {
          const selects = root.querySelectorAll?.('select') || [];
          for (const sel of selects) {
            if (sel.offsetParent === null) continue;
            const opts = Array.from(sel.options || []).filter(o => o.value && !/^--|sélectionnez|select\s/i.test((o.text || o.value || '')));
            const hasSgOption = opts.some(o => {
              const t = norm(o.text || o.value);
              return targets.some(tg => t.includes(tg) || tg.includes(t) || t.replace(/[^\w\s]/g, ' ').includes(tg.replace(/[^\w\s]/g, ' ')));
            });
            if (!hasSgOption) continue;
            const labelTxt = (sel.id || sel.name || '') + ' ' + (sel.closest?.('tr')?.querySelector?.('td:first-child, th')?.textContent || '') + ' ' + (sel.getAttribute?.('aria-label') || '');
            if (!/famille|emploi|family|job|occupation|métier/i.test(labelTxt) && opts.length > 30) continue;
            let found = 0;
            for (const target of targets) {
              const opt = opts.find(o => {
                const t = norm(o.text || o.value);
                return t === target || t.includes(target) || target.includes(t) || t.replace(/[^\w\s]/g, ' ').includes(target.replace(/[^\w\s]/g, ' '));
              });
              if (opt && opt.value) {
                if (sel.multiple) {
                  opt.selected = true;
                  found++;
                  log(`   ✏️ Famille de métier : ${(opt.text || opt.value).trim()} (Taleos → SG)`);
                } else {
                  sel.value = opt.value;
                  found = 1;
                  log(`   ✏️ Famille de métier : ${(opt.text || opt.value).trim()} (Taleos → SG)`);
                  break;
                }
                await delay(200);
              }
            }
            if (found > 0) {
              sel.dispatchEvent(new Event('change', { bubbles: true }));
              if (typeof jQuery !== 'undefined' && jQuery(sel).data?.('select2')) {
                try { jQuery(sel).trigger('change'); } catch (_) {}
              }
              return;
            }
          }
          const cbs = root.querySelectorAll?.('input[type="checkbox"]') || [];
          for (const cb of cbs) {
            const lbl = cb.closest?.('label') || root.querySelector?.(`label[for="${cb.id}"]`);
            const txt = norm(lbl?.textContent || cb.value || cb.title || '');
            if (!txt || cb.offsetParent === null) continue;
            const match = targets.find(tg => txt.includes(tg) || tg.includes(txt) || txt.replace(/[^\w\s]/g, ' ').includes(tg.replace(/[^\w\s]/g, ' ')));
            if (match && !cb.checked) {
              cb.click();
              log(`   ✏️ Famille de métier : ${(lbl?.textContent || cb.value || '').trim()} (Taleos → SG)`);
              await delay(200);
            }
          }
        }
      }

      const onStep2NotStep3 = (hasProfileFormVisible() || isInformationsPersonnellesPage()) && currentStep !== 'pieces' && !isPiecesJointesPage() && !profileFilled;
      if (onStep2NotStep3) {
      log('📂 [2/4] Informations personnelles');
      let fn = findByIdContains('personal_info_FirstName') || findByIdContains('FirstName');
      if (!fn) {
        for (let w = 0; w < 20; w++) {
          await delay(800);
          fn = findByIdContains('personal_info_FirstName') || findByIdContains('FirstName');
          if (fn && fn.offsetParent !== null) break;
          if (w === 5 || w === 12) log('   ⏳ Attente chargement formulaire...');
        }
      }
      if (!fn) fn = findInputByLabelPatterns(['prénom', 'prenom', 'first name', 'vorname']);
      let ln = findByIdContains('personal_info_LastName') || findByIdContains('LastName') || findInputByLabelPatterns(['nom de famille', 'nom', 'last name', 'nachname']);
      let email = findByIdContains('personal_info_EmailAddress') || findByIdContains('EmailAddress') || findInputByLabelPatterns(['e-mail', 'email', 'courriel']);
      let phone = findByIdContains('personal_info_MobilePhone') || findByIdContains('MobilePhone') || findInputByLabelPatterns(['téléphone', 'telephone', 'phone', 'mobile']);
      if (!fn || !ln) {
        const blockInputs = findProfileInputsInStep2Block();
        if (blockInputs && blockInputs.length >= 2) {
          if (!fn) fn = blockInputs.find(i => /first|prénom|prenom|given/i.test(i.id || i.name || '')) || blockInputs[0];
          if (!ln) ln = blockInputs.find(i => /last|nom|family|nachname/i.test(i.id || i.name || '')) || blockInputs[1];
          if (!email) email = blockInputs.find(i => /email|mail|courriel/i.test(i.id || i.name || '')) || blockInputs[2];
          if (!phone) phone = blockInputs.find(i => /phone|mobile|tél|tel/i.test(i.id || i.name || '')) || blockInputs[3];
          if (fn && ln) log('   📍 Champs trouvés via bloc "Informations personnelles"');
        }
      }
      let formComplete = !!(fn && ln);
      if (!formComplete) {
        log('   ⏳ Champs Prénom/Nom incomplets — attente 4s puis nouvel essai...');
        await delay(4000);
        fn = fn || findByIdContains('personal_info_FirstName') || findByIdContains('FirstName') || findInputByLabelPatterns(['prénom', 'prenom', 'first name']);
        ln = ln || findByIdContains('personal_info_LastName') || findByIdContains('LastName') || findInputByLabelPatterns(['nom de famille', 'nom', 'last name']);
        const blockInputs = findProfileInputsInStep2Block();
        if (blockInputs && blockInputs.length >= 2) {
          if (!fn) fn = blockInputs.find(i => /first|prénom|prenom|given/i.test(i.id || i.name || '')) || blockInputs[0];
          if (!ln) ln = blockInputs.find(i => /last|nom|family|nachname/i.test(i.id || i.name || '')) || blockInputs[1];
          if (!email) email = blockInputs.find(i => /email|mail|courriel/i.test(i.id || i.name || '')) || blockInputs[2];
          if (!phone) phone = blockInputs.find(i => /phone|mobile|tél|tel/i.test(i.id || i.name || '')) || blockInputs[3];
          if (fn && ln) log('   📍 Champs trouvés au 2e essai');
        }
        formComplete = !!(fn && ln);
      }
      if (!formComplete) {
        log('   ❌ Prénom et/ou Nom non trouvés — NE PAS cliquer. Vérification Firebase impossible.');
        return;
      }
      const valPrenom = (fn?.value || '').trim();
      if (valPrenom) {
        log(`   🔵 MODE : VÉRIFICATION (Prénom détecté: '${valPrenom}')`);
      } else {
        log('   🟢 MODE : REMPLISSAGE NEUF (Formulaire vide)');
      }

      await auditAndSetCivility(profile.civility);
      auditAndSetInput(fn, profile.firstname || profile.first_name, 'Prénom');
      auditAndSetInput(ln, profile.lastname || profile.last_name, 'Nom');
      auditAndSetInput(email, profile.email || profile.auth_email || '', 'Email');
      auditAndSetInput(phone, profile['phone-number'] || profile.phone || '', 'Téléphone');

      if (typeof mapTaleosToSgFamilies === 'function') {
        const taleosJobs = profile.job_families || profile.jobs || [];
        const sgFamilies = mapTaleosToSgFamilies(taleosJobs);
        if (sgFamilies.length > 0) {
          await syncJobFamilyField(sgFamilies);
        }
      }

      for (const root of getSearchRoots()) {
        const personalInputs = root.querySelectorAll?.('input[id*="personal_info"], input[name*="personal_info"], input[id*="candidate_personal"]') || [];
        for (const inp of personalInputs) {
          if (inp.type === 'text' && !inp.value?.trim() && inp.offsetParent !== null) {
            const isRequired = inp.required || inp.getAttribute?.('aria-required') === 'true' ||
              (inp.closest?.('label')?.textContent || '').includes('*') ||
              /obligatoire|required/i.test(inp.getAttribute?.('aria-label') || '');
            if (isRequired || /maiden|jeunefille|formerlast|secondname|middlename/i.test((inp.id || inp.name || '').toLowerCase())) {
              inp.value = '-';
              inp.dispatchEvent(new Event('input', { bubbles: true }));
              inp.dispatchEvent(new Event('blur', { bubbles: true }));
              const lbl = (inp.id || inp.name || 'champ').replace(/.*personal_info_|.*candidate_/i, '');
              log(`   ✏️ ${lbl || 'Champ'} : Remplacer "(vide)" par "-" (obligatoire)`);
            }
          }
        }
      }
      [fn, ln, email, phone].filter(Boolean).forEach(el => {
        el.dispatchEvent(new Event('blur', { bubbles: true }));
      });
      await delay(500);
      let saveProfile = null;
      for (const root of getSearchRoots()) {
        saveProfile = root.getElementById?.('et-ef-content-ftf-saveContinueCmdBottom') ||
          root.querySelector?.('input[id*="saveContinueCmdBottom"]') ||
          Array.from(root.querySelectorAll?.('input[type="button"], input[type="submit"], button') || []).find(el =>
            /sauvegarder et continuer|save and continue|save.*continue/i.test((el.value || el.textContent || '').trim())
          );
        if (saveProfile) break;
      }
      if (saveProfile) {
        log('   🖱️ Clic "Sauvegarder et continuer"...');
        saveProfile.scrollIntoView({ behavior: 'instant', block: 'center' });
        await delay(300);
        if (typeof window.cmdSubmit === 'function') {
          try { window.cmdSubmit('et-ef', 'et-ef-content-ftf-saveContinueCmdBottom', 'getOnClickAction()', null, true); } catch (_) { saveProfile.click(); }
        } else {
          saveProfile.click();
        }
        log('   ✅ [2/4] Informations personnelles : Vérifiées/corrigées et validées.');
        try { sessionStorage.setItem('taleos_sg_profile_filled', '1'); } catch (_) {}
      }
      await delay(5000);
      }

      const postulerBtn = (() => {
        for (const root of getSearchRoots()) {
          const btn = root.getElementById?.('et-ef-content-ftf-submitCmdBottom') ||
            root.querySelector?.('input[id*="submitCmdBottom"][value*="Postuler"]') ||
            root.querySelector?.('input[value="Postuler"]') ||
            Array.from(root.querySelectorAll?.('input[type="button"], input[type="submit"], button') || []).find(el =>
              /^postuler$/i.test((el.value || el.textContent || '').trim())
            );
          if (btn && btn.offsetParent !== null) return btn;
        }
        return null;
      })();
      if (postulerBtn || currentStep === 'verifier') {
        const btn = postulerBtn || (() => {
          for (const root of getSearchRoots()) {
            const b = root.querySelector?.('input[id*="submitCmdBottom"]') ||
              Array.from(root.querySelectorAll?.('input[type="button"], button') || []).find(el =>
                /postuler|submit|terminer/i.test((el.value || el.textContent || el.title || '').trim())
              );
            if (b && b.offsetParent !== null) return b;
          }
          return null;
        })();
        if (btn) {
          log('📂 [4/4] Validation finale — Clic "Postuler"');
          btn.scrollIntoView({ behavior: 'instant', block: 'center' });
          await delay(500);
          if (typeof window.cmdSubmit === 'function' && btn.id) {
            try { window.cmdSubmit('et-ef', btn.id, null, null, true); } catch (_) { btn.click(); }
          } else {
            btn.click();
          }
          log('   ✅ Candidature envoyée.');
          await delay(8000);
          const successMsg = (document.body?.textContent || '').toLowerCase();
          if (successMsg.includes('submitted') || successMsg.includes('envoyée') || successMsg.includes('success') || successMsg.includes('merci') || successMsg.includes("c'est dans la boîte") || successMsg.includes('thank you for your submission') || successMsg.includes('your submission')) {
            log('🎉 Candidature envoyée avec succès !');
            if (jobId && offerUrl) {
              try {
                chrome.runtime.sendMessage({ action: 'candidature_success', jobId, jobTitle, companyName, offerUrl });
              } catch (_) {}
            }
          }
          return;
        }
      }

      if (!isPiecesJointesPage()) return;
      log('📂 [3/4] Pièces jointes (CV)');
      if (window !== window.top) return;
      const cvInput = document.querySelector('input[id*="AttachedFilesBlock-uploadedFile"]') ||
        document.querySelector('input[id*="uploadedFile"]');
      await delay(3000);

      // Si on ne télécharge pas de CV : cocher "Télécharger mon CV PLUS TARD" puis sauvegarder
      if (!profile.cv_storage_path) {
        let skipRadio = null, saveCvBtn = null;
        for (const root of getSearchRoots()) {
          if (!skipRadio) skipRadio = root.querySelector?.('input[id*="skipResumeUploadRadio"][value="1"]') || root.querySelector?.('input[name*="skipResumeUploadRadio"][value="1"]');
          if (!saveCvBtn) {
            saveCvBtn = root.getElementById?.('editTemplateMultipart-editForm-content-ftf-saveContinueCmdBottom') || root.querySelector?.('input[id*="saveContinueCmdBottom"]');
            if (!saveCvBtn) {
              saveCvBtn = Array.from(root.querySelectorAll?.('input[type="button"], input[type="submit"], button') || []).find(el =>
                /sauvegarder et continuer|save and continue/i.test((el.value || el.textContent || '').trim())
              );
            }
          }
        }
        if (skipRadio && !skipRadio.checked) {
          log('   ☑️ Coche "Télécharger mon CV PLUS TARD"...');
          skipRadio.click();
          await delay(800);
        } else if (skipRadio) {
          log('   ✅ "Télécharger mon CV PLUS TARD" déjà coché.');
        }
        if (saveCvBtn) {
          log('   🖱️ Clic "Sauvegarder et continuer"...');
          saveCvBtn.scrollIntoView({ behavior: 'instant', block: 'center' });
          await delay(300);
          if (typeof window.cmdSubmit === 'function') {
            try { window.cmdSubmit('editTemplateMultipart-editForm', 'editTemplateMultipart-editForm-content-ftf-saveContinueCmdBottom', 'getOnClickAction()', null, true); } catch (_) { saveCvBtn.click(); }
          } else {
            saveCvBtn.click();
          }
          await delay(5000);
        }
      } else {
        // skipDeleteAndUpload : évite de re-supprimer le CV qu'on vient d'uploader si la page recharge (boucle)
        const UPLOAD_COOLDOWN = 120000;
        const lastUpload = parseInt(sessionStorage.getItem('taleos_cv_uploaded_at') || '0', 10);
        const skipDeleteAndUpload = (Date.now() - lastUpload < UPLOAD_COOLDOWN);

        // Supprimer TOUS les documents existants – le clic Supprimer provoque une nav, le popup Oui/NON apparaît sur la page suivante
        const MAX_DELETE = 20;
        if (skipDeleteAndUpload) log('   ⏭️ CV déjà uploadé récemment – skip suppression.');
        for (let d = 0; d < MAX_DELETE && !skipDeleteAndUpload; d++) {
          // 1. Si le popup de confirmation est visible (FR ou EN), cliquer Oui/Yes en priorité
          let yesBtn = null;
          let hasConfirmPopup = false;
          for (const root of getSearchRoots()) {
            const bodyText = (root.body?.innerText || root.body?.innerHTML || '').toLowerCase();
            hasConfirmPopup = hasConfirmPopup || /voulez-vous vraiment supprimer/i.test(bodyText) || /are you sure that you want to delete/i.test(bodyText);
            if (hasConfirmPopup) {
              const dialog = root.querySelector?.('.alert-title')?.closest?.('[role="dialog"], .ui-dialog, .modal, .alert-box, div[class*="alert"]');
              const searchIn = dialog || root;
              yesBtn = searchIn.querySelector?.('input[id*="YesDeleteAttachedFileCommand"]') ||
                searchIn.querySelector?.('input[value="Oui"]') ||
                searchIn.querySelector?.('input[value="OUI"]') ||
                searchIn.querySelector?.('input[value="Yes"]') ||
                searchIn.querySelector?.('input[value="YES"]') ||
                searchIn.querySelector?.('input[type="button"][value*="ui"]') ||
                Array.from(searchIn.querySelectorAll?.('input[type="button"], input[type="submit"], button, a[role="button"]') || []).find(el =>
                  /^(oui|yes)$/i.test((el.value || el.textContent || el.innerText || '').trim())
                );
              if (!yesBtn) {
                yesBtn = Array.from(root.querySelectorAll?.('input, button') || []).find(el =>
                  /^(oui|yes)$/i.test((el.value || el.textContent || '').trim()) && el.offsetParent !== null
                );
              }
              if (!yesBtn) {
                const alertTitle = root.querySelector?.('.alert-title');
                const dialogContainer = alertTitle?.closest?.('[role="dialog"], .ui-dialog, div[class*="dialog"], div[class*="alert"]') || alertTitle?.parentElement;
                if (dialogContainer) {
                  yesBtn = Array.from(dialogContainer.querySelectorAll?.('input, button, a[role="button"]') || []).find(el =>
                    /^(oui|yes)$/i.test((el.value || el.textContent || '').trim()) && el.offsetParent !== null
                  );
                  if (!yesBtn) {
                    const btns = Array.from(dialogContainer.querySelectorAll?.('input[type="button"], input[type="submit"], button, a[role="button"]') || []).filter(el => el.offsetParent !== null);
                    yesBtn = btns.find(el => /^(oui|yes)$/i.test((el.value || el.textContent || '').trim()));
                  }
                }
              }
              if (yesBtn && yesBtn.offsetParent !== null) {
                log('   ✅ Popup suppression → Clic Oui/Yes');
                yesBtn.scrollIntoView({ behavior: 'instant', block: 'center' });
                await delay(500);
                if (typeof window.cmdSubmit === 'function' && yesBtn.id) {
                  try { window.cmdSubmit('editTemplateMultipart-editForm', yesBtn.id, null, null, true); } catch (_) { yesBtn.click(); }
                } else {
                  yesBtn.click();
                }
                await delay(5000);
                break;
              }
            }
          }
          if (yesBtn) continue;

          if (hasConfirmPopup && !yesBtn) {
            if (DEBUG) log('   Popup: recherche élargie Oui/Yes');
            for (const root of getSearchRoots()) {
              const all = root.querySelectorAll?.('input[type="button"], input[type="submit"], button');
              for (const el of all || []) {
                if (el.offsetParent === null) continue;
                const v = (el.value || el.textContent || '').trim();
                if (/^(oui|yes)$/i.test(v) || (v.length <= 5 && /(oui|yes)/i.test(v))) {
                  yesBtn = el;
                  if (DEBUG) log('   Bouton Oui trouvé');
                  yesBtn.scrollIntoView({ behavior: 'instant', block: 'center' });
                  await delay(300);
                  el.click();
                  await delay(5000);
                  break;
                }
              }
              if (yesBtn) break;
            }
            if (yesBtn) continue;
            await delay(2000);
            continue;
          }

          // 2. Chercher le lien Supprimer – on supprime TOUS les documents (y compris cv.pdf) pour les remplacer par celui de Firebase
          let deleteLink = null;
          for (const root of getSearchRoots()) {
            const spans = root.querySelectorAll?.('span[id*="attachmentFileDelete"], a[id*="DeleteAttachedFile"]') || [];
            for (const el of spans) {
              if (el.offsetParent !== null && !/YesDelete/i.test(el.id || '')) {
                deleteLink = el;
                break;
              }
            }
            if (!deleteLink) {
              const links = root.querySelectorAll?.('a, span[role="button"]') || [];
              for (const el of links) {
                const txt = (el.textContent || '').trim().toLowerCase();
                if ((txt === 'supprimer' || txt === 'delete') && el.offsetParent !== null && el.closest?.('table')) {
                  deleteLink = el;
                  break;
                }
              }
            }
            if (deleteLink) break;
          }
          if (!deleteLink) {
            if (d > 0 && DEBUG) log(`   ${d} document(s) supprimé(s)`);
            break;
          }
          if (DEBUG) log(`   Suppression document ${d + 1}`);
          deleteLink.scrollIntoView({ behavior: 'instant', block: 'center' });
          await delay(300);
          deleteLink.click();
          await delay(2000);
        }

        let fileInput = cvInput;
        if (!fileInput) {
          for (const root of getSearchRoots()) {
            fileInput = root.querySelector?.('input[id*="AttachedFilesBlock-uploadedFile"]') || root.querySelector?.('input[id*="uploadedFile"]');
            if (fileInput) break;
          }
        }
        if (fileInput) {
          let uploadOk = false;
          if (!skipDeleteAndUpload) {
            log('   📤 Upload CV depuis Firebase...');
            const cvName = profile.cv_filename || profile.cv_storage_path?.split('/').pop() || 'cv.pdf';
            uploadOk = await setFileInputFromStorage(fileInput, profile.cv_storage_path, cvName);
            if (uploadOk) {
              try { sessionStorage.setItem('taleos_cv_uploaded_at', String(Date.now())); } catch (_) {}
              let attachBtn = null;
              for (const root of getSearchRoots()) {
                attachBtn = root.querySelector?.('input[id*="AttachedFilesBlock-attachFileCommand"]') || root.querySelector?.('input[id*="attachFileCommand"]');
                if (attachBtn) break;
              }
              if (attachBtn) {
                attachBtn.click();
                await delay(10000);
              }
            }
          }

          if (skipDeleteAndUpload || uploadOk) {
            async function checkResumeInAttachmentTable() {
              for (const root of getSearchRoots()) {
                const resumeChecks = root.querySelectorAll?.('input[id*="resumeselectionid"]') || [];
                const rowText = (r) => (r?.textContent || '').toLowerCase();
                const preferCv = (chk) => {
                  const row = chk.closest?.('tr');
                  return /résumé|resume|cv\.pdf|cv\.doc/i.test(rowText(row));
                };
                const unchecked = Array.from(resumeChecks).filter(chk => !chk.checked && chk.offsetParent !== null);
                const chk = unchecked.find(preferCv) || unchecked[0];
                if (!chk) return false;
                chk.scrollIntoView({ behavior: 'instant', block: 'center' });
                await delay(200);
                const label = root.querySelector?.(`label[for="${chk.id}"]`);
                const styled = chk.nextElementSibling;
                if (label && label.offsetParent !== null) {
                  label.click();
                } else if (styled?.classList?.contains?.('styled-checkbox')) {
                  styled.click();
                } else {
                  chk.click();
                }
                await delay(300);
                if (!chk.checked) {
                  chk.checked = true;
                  chk.dispatchEvent(new Event('change', { bubbles: true }));
                  chk.dispatchEvent(new Event('input', { bubbles: true }));
                }
                return chk.checked;
              }
              return false;
            }

            log('   ☑️ Coche case "CV" dans le tableau des pièces jointes...');
            let checked = false;
            for (let i = 0; i < 6; i++) {
              let tableEl = null;
              for (const root of getSearchRoots()) {
                tableEl = root.querySelector?.('table.attachment-list');
                if (tableEl) break;
              }
              if (tableEl) {
                tableEl.scrollIntoView({ behavior: 'instant', block: 'center' });
                await delay(800);
              }
              checked = await checkResumeInAttachmentTable();
              if (checked) {
                log('   ✅ Case "Résumé" cochée.');
                await delay(1000);
                break;
              }
              let links = [];
              for (const root of getSearchRoots()) {
                links = Array.from(root.querySelectorAll?.('a[id*="dtGotoPageLink"]') || []);
                if (links.length) break;
              }
              if (i < links.length && links[i]?.offsetParent !== null) {
                const lbl = (links[i].title || links[i].textContent || '').trim().slice(0, 50);
                if (DEBUG) log(`   Navigation onglet "${lbl}"`);
                links[i].click();
                await delay(2500);
              } else {
                break;
              }
            }
            if (!checked) log('   ⚠️ Aucune case "Résumé" non cochée trouvée.');

            await delay(1000);
            let finalSave = null;
            for (const root of getSearchRoots()) {
              finalSave = root.getElementById?.('editTemplateMultipart-editForm-content-ftf-saveContinueCmdBottom') || root.querySelector?.('input[id*="saveContinueCmdBottom"]');
              if (finalSave) break;
            }
            if (finalSave) {
              if (typeof window.cmdSubmit === 'function') {
                try { window.cmdSubmit('editTemplateMultipart-editForm', 'editTemplateMultipart-editForm-content-ftf-saveContinueCmdBottom', 'getOnClickAction()', null, true); } catch (_) { finalSave.click(); }
              } else {
                finalSave.click();
              }
              log('   ✅ [3/4] Pièces jointes : Sauvegardé → étape 4.');
            }
          }
        }
      }

      log('🏁 Automatisation terminée. Vérifiez la page.');
      await delay(10000);

      const successMsg = document.body?.textContent?.toLowerCase() || '';
      if (successMsg.includes('submitted') || successMsg.includes('envoyée') || successMsg.includes('success') || successMsg.includes('thank you for your submission') || successMsg.includes('your submission')) {
        log('🎉 Candidature envoyée avec succès !');
        if (jobId && offerUrl) {
          chrome.runtime.sendMessage({
            action: 'candidature_success',
            jobId,
            jobTitle,
            companyName,
            offerUrl
          });
        }
      }

    } catch (e) {
      log(`❌ Erreur : ${e.message}`);
      console.error(e);
      if (jobId && window === window.top) {
        try {
          chrome.runtime.sendMessage({
            action: 'candidature_failure',
            jobId,
            error: e.message || 'Erreur lors de l\'automatisation'
          });
        } catch (_) {}
      }
    } finally {
      document.getElementById(BANNER_ID)?.remove();
    }
  }

  window.__taleosRun = function(profile) {
    main(profile || {}).catch(e => console.error('[Taleos SG]', e));
  };
})();
