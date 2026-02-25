/**
 * Taleos - Automatisation Société Générale (socgen.taleo.net)
 * Flux : Login → Reset draft (si présent) → Skip étapes → Profil → CV → Envoi
 */
(function() {
  'use strict';

  const delay = ms => new Promise(r => setTimeout(r, ms));

  function log(msg) {
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

  function findByIdContains(partialId) {
    const el = document.querySelector(`input[id*="${partialId}"], input[name*="${partialId}"], a[id*="${partialId}"]`);
    return el || null;
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

  async function main(profile) {
    showBanner();
    const jobId = profile.__jobId || '';
    const jobTitle = profile.__jobTitle || '';
    const companyName = profile.__companyName || 'Société Générale';
    const offerUrl = profile.__offerUrl || '';

    log('🚀 DÉMARRAGE AUTOMATISATION SOCIÉTÉ GÉNÉRALE');

    try {
      dismissCookieBanner();
      await delay(2000);

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

      const resetLink = document.querySelector('a[id*="dtGotoPageLink"]');
      if (resetLink && resetLink.offsetParent !== null) {
        log('🔄 Reset du formulaire (draft détecté)...');
        resetLink.click();
        await delay(5000);
      }

      function findDisclaimerCheckbox() {
        const cbs = document.querySelectorAll('input[type="checkbox"]');
        for (const cb of cbs) {
          const label = cb.closest('label') || document.querySelector(`label[for="${cb.id}"]`);
          const txt = ((label?.textContent || cb.getAttribute('aria-label') || '') + (cb.value || '')).toLowerCase();
          if (/accept|accepter|agree|consent|disclaimer|déclaration|confirm/i.test(txt) && cb.offsetParent !== null) return cb;
        }
        return null;
      }

      function findDisclaimerOrContinueBtn() {
        const byId = [
          'legalDisclaimerContinueButton',
          'legalDisclaimerAcceptButton',
          'legalDisclaimer',
          'saveContinueCmdBottom',
          'legalDisclaimerContinue',
          'disclaimerContinue'
        ];
        for (const partial of byId) {
          const el = document.querySelector(`input[id*="${partial}"], button[id*="${partial}"], a[id*="${partial}"]`);
          if (el && el.offsetParent !== null) return el;
        }
        const byValue = ['Continue', 'Continuer', 'Accept', 'Accepter', 'I accept', 'J\'accepte', 'Next', 'Suivant'];
        for (const val of byValue) {
          const el = document.querySelector(`input[value*="${val}"], button[value*="${val}"], input[value="${val}"]`);
          if (el && el.offsetParent !== null) return el;
        }
        const btns = document.querySelectorAll('input[type="submit"], input[type="button"], button, a[role="button"]');
        for (const b of btns) {
          const t = ((b.value || b.textContent || '').trim()).toLowerCase();
          if (/continue|continuer|accept|accepter|suivant|next|valider|submit/i.test(t) && b.offsetParent !== null) return b;
        }
        return null;
      }

      log('📋 Validation du disclaimer de candidature (peut apparaître 2 fois)...');
      for (let i = 0; i < 25; i++) {
        const firstNameInput = findByIdContains('personal_info_FirstName') || findByIdContains('FirstName');
        if (firstNameInput && firstNameInput.offsetParent !== null) {
          log('   ✅ Formulaire profil atteint.');
          break;
        }
        const chk = findDisclaimerCheckbox();
        if (chk && !chk.checked) {
          log('   ☑️ Coche du disclaimer...');
          chk.click();
          await delay(1500);
        }
        const btn = findDisclaimerOrContinueBtn();
        if (btn) {
          log(`   🖱️ Clic disclaimer ${i + 1} (${(btn.value || btn.textContent || '').trim().slice(0, 30)})...`);
          btn.scrollIntoView({ behavior: 'instant', block: 'center' });
          await delay(300);
          btn.click();
          await delay(4500);
        } else {
          await delay(2000);
        }
      }

      log('📝 Remplissage du profil...');
      const fn = findByIdContains('personal_info_FirstName') || findByIdContains('FirstName');
      const ln = findByIdContains('personal_info_LastName') || findByIdContains('LastName');
      const email = findByIdContains('personal_info_EmailAddress') || findByIdContains('EmailAddress');
      const phone = findByIdContains('personal_info_MobilePhone') || findByIdContains('MobilePhone');

      if (fn) { fn.value = profile.firstname || ''; fn.dispatchEvent(new Event('input', { bubbles: true })); }
      if (ln) { ln.value = profile.lastname || ''; ln.dispatchEvent(new Event('input', { bubbles: true })); }
      const contactEmail = profile.email || profile.auth_email || '';
      if (email) { email.value = contactEmail; email.dispatchEvent(new Event('input', { bubbles: true })); }
      if (phone) { phone.value = profile['phone-number'] || profile.phone || ''; phone.dispatchEvent(new Event('input', { bubbles: true })); }

      await delay(500);
      const saveProfile = document.querySelector('#et-ef-content-ftf-saveContinueCmdBottom') ||
        document.querySelector('input[id*="saveContinueCmdBottom"]');
      if (saveProfile) {
        saveProfile.click();
        log('   ✅ Profil validé.');
      }
      await delay(5000);

      log('📤 Étape CV...');
      const cvInput = document.querySelector('input[id*="AttachedFilesBlock-uploadedFile"]') ||
        document.querySelector('input[id*="uploadedFile"]');
      await delay(3000);

      const deleteBtn = document.querySelector('span[id*="attachmentFileDelete"]');
      if (deleteBtn && deleteBtn.offsetParent !== null) {
        log('   🗑️ Suppression ancien CV...');
        deleteBtn.click();
        await delay(1500);
        const yesBtn = document.querySelector('input[id*="YesDeleteAttachedFileCommand"]');
        if (yesBtn) {
          yesBtn.click();
          await delay(6000);
        }
      }

      if (profile.cv_storage_path && cvInput) {
        log('   📤 Upload CV depuis Firebase...');
        const ok = await setFileInputFromStorage(cvInput, profile.cv_storage_path, 'cv.pdf');
        if (ok) {
          const attachBtn = document.querySelector('input[id*="AttachedFilesBlock-attachFileCommand"]') ||
            document.querySelector('input[id*="attachFileCommand"]');
          if (attachBtn) {
            attachBtn.click();
            await delay(10000);
          }

          const resumeChk = document.querySelector('input[id*="resumeselectionid"]');
          if (resumeChk && !resumeChk.checked) {
            resumeChk.click();
            log('   ✅ Case "is a resume" cochée.');
            await delay(500);
          }

          const finalSave = document.querySelector('#editTemplateMultipart-editForm-content-ftf-saveContinueCmdBottom') ||
            document.querySelector('input[id*="saveContinueCmdBottom"]');
          if (finalSave) {
            finalSave.click();
            log('   ✅ Validation finale.');
          }
        }
      }

      log('🏁 Automatisation terminée. Vérifiez la page.');
      await delay(10000);

      const successMsg = document.body?.textContent?.toLowerCase() || '';
      if (successMsg.includes('submitted') || successMsg.includes('envoyée') || successMsg.includes('success')) {
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
    } finally {
      document.getElementById(BANNER_ID)?.remove();
    }
  }

  window.__taleosRun = function(profile) {
    main(profile || {}).catch(e => console.error('[Taleos SG]', e));
  };
})();
