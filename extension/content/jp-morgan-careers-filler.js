(function () {
  'use strict';

  if (!/jpmc\.fa\.oraclecloud\.com$/i.test(location.hostname || '')) return;

  const BANNER_ID = 'taleos-jp-morgan-banner';
  const PENDING_KEY = 'taleos_pending_jp_morgan';
  const TAB_KEY = 'taleos_jp_morgan_tab_id';
  const LOG_PREFIX = '[Taleos JP Morgan]';
  const blueprint = globalThis.__TALEOS_JP_MORGAN_BLUEPRINT__ || null;
  let isRunning = false;
  let currentTabIdPromise = null;
  let logged = new Set();
  let state = {
    termsAccepted: false,
    emailSubmitted: false,
    pinSubmitted: false,
    nextSection1: false,
    nextSection2: false,
    nextSection3: false,
    educationFilled: false,
    submitSection4: false,
    reviewStartedAt: 0,
    successSent: false,
    resumeUploadToken: '',
    coverUploadToken: ''
  };

  function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  function log(message, indent = 0) {
    const text = `${'   '.repeat(indent)}${message}`;
    if (logged.has(text)) return;
    logged.add(text);
    console.log(`${LOG_PREFIX} ${text}`);
  }

  function norm(value) {
    return String(value || '').replace(/\s+/g, ' ').trim().toLowerCase();
  }

  function normalizeNationalPhoneDigits(rawPhone, countryCode) {
    let d = String(rawPhone || '').replace(/\D/g, '');
    const cc = String(countryCode || '+33').trim().replace(/\s/g, '');
    if (cc === '+33' || cc === '33') {
      if (d.length >= 10 && d.startsWith('0')) d = d.slice(1);
      if (d.length >= 11 && d.startsWith('33')) d = d.slice(2);
    }
    return d;
  }

  function getBannerApi() {
    return globalThis.__TALEOS_AUTOMATION_BANNER__ || null;
  }

  function ensureBanner(text) {
    let banner = document.getElementById(BANNER_ID);
    if (!banner) {
      banner = document.createElement('div');
      banner.id = BANNER_ID;
      const api = getBannerApi();
      if (api) api.applyStyle(banner);
      document.body?.insertBefore(banner, document.body.firstChild);
    }
    banner.textContent = text || (getBannerApi()?.getText() || '⏳ Automatisation Taleos en cours — Ne touchez à rien.');
  }

  async function getCurrentTabId() {
    if (!currentTabIdPromise) {
      currentTabIdPromise = chrome.runtime.sendMessage({ action: 'taleos_get_current_tab_id' })
        .then((res) => res?.tabId || null)
        .catch(() => null);
    }
    return currentTabIdPromise;
  }

  async function getPending() {
    const currentTabId = await getCurrentTabId();
    const local = await chrome.storage.local.get([PENDING_KEY, TAB_KEY]);
    const pending = local[PENDING_KEY];
    const expectedTabId = pending?.tabId || local[TAB_KEY] || null;
    if (!pending || !expectedTabId || !currentTabId || currentTabId !== expectedTabId) return null;
    return pending;
  }

  function visible(selector, root = document) {
    try {
      return Array.from(root.querySelectorAll(selector)).find((el) => {
        const rect = typeof el.getBoundingClientRect === 'function' ? el.getBoundingClientRect() : null;
        const style = globalThis.getComputedStyle ? getComputedStyle(el) : null;
        return !!rect && rect.width > 0 && rect.height > 0 && style?.display !== 'none' && style?.visibility !== 'hidden';
      }) || null;
    } catch (_) {
      return null;
    }
  }

  function isElementVisible(el) {
    if (!el) return false;
    const rect = typeof el.getBoundingClientRect === 'function' ? el.getBoundingClientRect() : null;
    const style = globalThis.getComputedStyle ? getComputedStyle(el) : null;
    return !!rect && rect.width > 0 && rect.height > 0 && style?.display !== 'none' && style?.visibility !== 'hidden';
  }

  function getValue(el) {
    if (!el) return '';
    return String(el.value || el.textContent || '').trim();
  }

  function setInputValue(el, value) {
    if (!el) return false;
    const next = String(value ?? '').trim();
    const current = getValue(el);
    if (current === next) return 'skip';
    const proto = el instanceof HTMLTextAreaElement ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype;
    const setter = Object.getOwnPropertyDescriptor(proto, 'value')?.set;
    if (setter) setter.call(el, next);
    else el.value = next;
    el.dispatchEvent(new Event('input', { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
    el.blur();
    return 'updated';
  }

  function auditAndFill(label, el, desiredValue, { transformCurrent = (v) => v, transformDesired = (v) => v } = {}) {
    if (!el) {
      log(`⚠️ ${label} : champ introuvable`, 1);
      return false;
    }
    const currentRaw = getValue(el);
    const current = transformCurrent(currentRaw);
    const desired = transformDesired(desiredValue);
    if (String(current) === String(desired)) {
      log(`✅ ${label} : formulaire='${currentRaw || '(vide)'}' | Firebase='${desiredValue || '(vide)'}' -> Skip`, 1);
      return true;
    }
    log(`✏️ ${label} : formulaire='${currentRaw || '(vide)'}' | Firebase='${desiredValue || '(vide)'}' -> Correction`, 1);
    setInputValue(el, desired);
    return true;
  }

  function auditAndSelectButton(label, container, desiredText) {
    if (!container || !desiredText) return false;
    const target = norm(desiredText);
    const options = Array.from(container.querySelectorAll('button, [role="radio"], [aria-pressed], [aria-checked]'));
    for (const option of options) {
      const text = norm(option.textContent || option.getAttribute('aria-label') || '');
      if (!text || text !== target) continue;
      const selected = option.getAttribute('aria-checked') === 'true' ||
        option.getAttribute('aria-pressed') === 'true' ||
        option.classList.contains('cx-select-pill-section--selected') ||
        option.classList.contains('selected') ||
        option.classList.contains('oj-selected') ||
        option.parentElement?.classList?.contains?.('cx-select-pill-section--selected');
      if (selected) {
        log(`✅ ${label} : formulaire='${option.textContent.trim()}' | Firebase='${desiredText}' -> Skip`, 1);
        return true;
      }
      log(`✏️ ${label} : formulaire='${option.textContent.trim() || '(autre)'}' | Firebase='${desiredText}' -> Correction`, 1);
      option.click();
      return true;
    }
    log(`⚠️ ${label} : option '${desiredText}' introuvable`, 1);
    return false;
  }

  function findBySelectors(selectors) {
    for (const selector of selectors) {
      const el = visible(selector) || document.querySelector(selector);
      if (el) return el;
    }
    return null;
  }

  function findFieldByLabel(labelNeedle) {
    const target = norm(labelNeedle);
    const labels = Array.from(document.querySelectorAll('label, legend, p, span, div')).filter((el) => {
      const text = norm(el.textContent || '');
      return text && text.includes(target);
    });
    const candidates = [];
    for (const label of labels) {
      const forId = label.getAttribute?.('for');
      if (forId) {
        const direct = document.getElementById(forId);
        if (direct) {
          candidates.push({ field: direct, score: 1000 });
          continue;
        }
      }
      let current = label;
      for (let depth = 0; current && depth < 6; depth += 1, current = current.parentElement) {
        const fields = Array.from(current.querySelectorAll('input, textarea, select, [role="combobox"] input'))
          .filter((el) => isElementVisible(el) || el === document.activeElement);
        if (!fields.length) continue;
        const currentText = norm(current.textContent || '');
        if (!currentText.includes(target)) continue;
        const field = fields[0];
        const exact = currentText === target ? 100 : 0;
        const score = exact + Math.max(0, 40 - currentText.length) + Math.max(0, 20 - fields.length * 4) - depth;
        candidates.push({ field, score });
      }
    }
    candidates.sort((a, b) => b.score - a.score);
    return candidates[0]?.field || null;
  }

  function findQuestionRow(textNeedle) {
    const target = norm(textNeedle);
    const nodes = Array.from(document.querySelectorAll('label, legend, h1, h2, h3, h4, h5, h6, p, span, div'));
    const candidates = [];
    for (const node of nodes) {
      const text = norm(node.textContent || '');
      if (!text || !text.includes(target)) continue;
      let current = node;
      for (let depth = 0; current && depth < 6; depth += 1, current = current.parentElement) {
        const row = current.closest?.('.input-row, .oj-form-layout, .oj-flex-item, .oj-form, .oj-panel, .oj-flex');
        const scoped = row || current;
        const fields = scoped.querySelectorAll('input, textarea, select, [role="combobox"]');
        if (!fields.length) continue;
        const currentText = norm(scoped.textContent || '');
        if (!currentText.includes(target)) continue;
        candidates.push({ node: scoped, score: Math.max(0, 60 - currentText.length) - depth });
      }
    }
    candidates.sort((a, b) => b.score - a.score);
    return candidates[0]?.node || null;
  }

  function findPhoneInputs() {
    const row = findQuestionRow('phone number') || findQuestionRow('phone');
    if (!row) return { countryCodeInput: null, phoneInput: null };
    const inputs = Array.from(row.querySelectorAll('input')).filter((el) => isElementVisible(el) || el === document.activeElement);
    if (!inputs.length) return { countryCodeInput: null, phoneInput: null };
    const countryCodeInput = inputs.find((el) => /country code/i.test(el.getAttribute('aria-label') || '') || /country code/i.test(el.placeholder || '')) || inputs[0];
    const phoneInput = inputs.find((el) => el !== countryCodeInput) || inputs[inputs.length - 1];
    return { countryCodeInput, phoneInput };
  }

  async function selectDropdownValue(label, desiredValue, aliases = []) {
    const row = findQuestionRow(label);
    if (!row || !desiredValue) {
      log(`⚠️ ${label} : menu déroulant introuvable`, 1);
      return false;
    }
    const input = row.querySelector('input[role="combobox"], input[type="text"], select');
    if (!input) {
      log(`⚠️ ${label} : champ dropdown introuvable`, 1);
      return false;
    }
    const desiredNorm = norm(desiredValue);
    const currentRaw = getValue(input);
    if (norm(currentRaw) === desiredNorm) {
      log(`✅ ${label} : formulaire='${currentRaw || '(vide)'}' | Firebase='${desiredValue}' -> Skip`, 1);
      return true;
    }
    log(`✏️ ${label} : formulaire='${currentRaw || '(vide)'}' | Firebase='${desiredValue}' -> Correction`, 1);
    // Oracle JET uses aria-label button; Oracle CX uses the input itself as toggle
    const toggleBtn = row.querySelector('button[aria-label*="Open the drop-down list" i], button.icon-dropdown-arrow');
    if (toggleBtn) toggleBtn.click();
    else { input.click(); input.focus?.(); }
    await sleep(200);
    setInputValue(input, desiredValue);
    input.focus?.();
    await sleep(200);
    for (const candidate of [desiredValue, ...aliases]) {
      if (await pickVisibleOption(candidate)) return true;
    }
    input.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', code: 'Enter', bubbles: true }));
    input.dispatchEvent(new Event('change', { bubbles: true }));
    return true;
  }

  function getDropdownField(selectors = [], label = '') {
    const direct = selectors.length ? findBySelectors(selectors) : null;
    if (direct) {
      const row = direct.closest?.('.input-row, .oj-form-layout, .oj-flex-item, .oj-form, .oj-panel, .oj-flex') || direct.parentElement || document;
      return { row, input: direct };
    }
    const row = label ? findQuestionRow(label) : null;
    if (!row) return { row: null, input: null };
    const input = row.querySelector('input[role="combobox"], input[type="text"], select');
    return { row, input };
  }

  async function selectDropdownValueWithSelectors(label, selectors, desiredValue, aliases = []) {
    const { row, input } = getDropdownField(selectors, label);
    if (!row || !input || !desiredValue) {
      log(`⚠️ ${label} : menu déroulant introuvable`, 1);
      return false;
    }
    const desiredNorm = norm(desiredValue);
    const currentRaw = getValue(input);
    if (norm(currentRaw) === desiredNorm) {
      log(`✅ ${label} : formulaire='${currentRaw || '(vide)'}' | Firebase='${desiredValue}' -> Skip`, 1);
      return true;
    }
    log(`✏️ ${label} : formulaire='${currentRaw || '(vide)'}' | Firebase='${desiredValue}' -> Correction`, 1);
    // Oracle JET uses aria-label button; Oracle CX uses the input itself as toggle
    const toggleBtn = row.querySelector('button[aria-label*="Open the drop-down list" i], button.icon-dropdown-arrow');
    if (toggleBtn) toggleBtn.click();
    else { input.click(); input.focus?.(); }
    await sleep(200);
    setInputValue(input, desiredValue);
    input.focus?.();
    await sleep(200);
    for (const candidate of [desiredValue, ...aliases]) {
      if (await pickVisibleOption(candidate)) return true;
    }
    input.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', code: 'Enter', bubbles: true }));
    input.dispatchEvent(new Event('change', { bubbles: true }));
    return true;
  }

  function mapEducationLevelToDegree(educationLevel, schoolType = '') {
    const lvl = norm(educationLevel);
    const school = norm(schoolType);
    if (!lvl && !school) return '';
    if (school.includes('engineer')) return "Engineer's Degree";
    if (lvl.includes('bac + 5') || lvl.includes('m2') || lvl.includes('master')) return "Master's Degree";
    if (lvl.includes('bac + 4') || lvl.includes('bac + 3') || lvl.includes('l3') || lvl.includes('l4') || lvl.includes('bachelor')) return "Bachelor's Degree";
    if (lvl.includes('bac + 2') || lvl.includes('l2') || lvl.includes('associate')) return "Associate's Degree";
    if (lvl === 'bac' || lvl.includes('high school')) return 'High School Diploma/GED';
    return '';
  }

  function getEducationBlockForField(field) {
    let current = field;
    let best = null;
    for (let depth = 0; current && depth < 8; depth += 1, current = current.parentElement) {
      const inputs = current.querySelectorAll('input, textarea, select, [role="combobox"]');
      const text = norm(current.textContent || '');
      if (!inputs.length || !text.includes('degree')) continue;
      best = current;
      const degreeInputs = current.querySelectorAll('input[name*="DEGREE" i], input[id*="DEGREE" i]');
      if (degreeInputs.length > 1) break;
    }
    return best || field.parentElement || null;
  }

  async function removeEducationEntry(block, degreeLabel) {
    if (!block) return false;
    const btn = Array.from(block.querySelectorAll('button, [role="button"], a')).find((el) => {
      const hint = `${el.textContent || ''} ${el.getAttribute('aria-label') || ''} ${el.getAttribute('title') || ''}`;
      return /remove|delete|trash|supprimer|retirer/i.test(hint);
    });
    if (!btn) {
      log(`⚠️ Education (${degreeLabel || 'bloc'}) : bouton supprimer introuvable`, 1);
      return false;
    }
    btn.click();
    await sleep(500);
    log(`🗑️ Education : bloc '${degreeLabel || 'autre diplôme'}' supprimé`, 1);
    return true;
  }

  function findQuestionContainer(textNeedle) {
    const target = norm(textNeedle);
    const nodes = Array.from(document.querySelectorAll('label, legend, h1, h2, h3, h4, h5, h6, p, span, div'));
    const candidates = [];
    for (const node of nodes) {
      const text = norm(node.textContent || '');
      if (!text || !text.includes(target)) continue;
      let current = node;
      for (let depth = 0; current && depth < 6; depth += 1, current = current.parentElement) {
        const hasButtons = current.querySelector('button, [role="radio"], [aria-pressed], [aria-checked]');
        if (!hasButtons) continue;
        const currentText = norm(current.textContent || '');
        if (!currentText.includes(target)) continue;
        const optionCount = current.querySelectorAll('button, [role="radio"], [aria-pressed], [aria-checked]').length;
        candidates.push({ node: current, textLength: currentText.length, optionCount });
      }
    }
    candidates.sort((a, b) => {
      if (a.optionCount !== b.optionCount) return a.optionCount - b.optionCount;
      return a.textLength - b.textLength;
    });
    return candidates[0]?.node || null;
  }

  function findButtonByText(text) {
    const target = norm(text);
    return Array.from(document.querySelectorAll('button, [role="button"], input[type="button"], input[type="submit"]')).find((el) => {
      const content = norm(el.textContent || el.value || el.getAttribute('aria-label') || '');
      return content === target || content.includes(target);
    }) || null;
  }

  async function pickVisibleOption(textNeedle) {
    const target = norm(textNeedle);
    // Oracle HCM CX uses .cx-select__list-item--content (no role="option"), OJet uses .oj-listbox-result
    const options = Array.from(document.querySelectorAll(
      '[role="option"], li[role="option"], .oj-listbox-result, .oj-listview-item, .cx-select__list-item--content, [class*="cx-select__list-item"]'
    ));
    const option = options.find((el) => {
      const text = norm(el.textContent || '');
      return text === target || text.includes(target) || target.includes(text);
    });
    if (option) {
      // Walk up to find clickable ancestor if needed
      let clickTarget = option;
      for (let i = 0; i < 4 && clickTarget; i++) {
        if (clickTarget.tagName === 'LI' || clickTarget.getAttribute('role') === 'option' || clickTarget.getAttribute('tabindex') !== null) break;
        const parent = clickTarget.parentElement;
        if (!parent || parent.tagName === 'UL' || parent.tagName === 'BODY') break;
        clickTarget = parent;
      }
      clickTarget.click();
      await sleep(300);
      return true;
    }
    return false;
  }

  async function setFileInputFromStorage(inputEl, storagePath, filename) {
    if (!inputEl || !storagePath) return false;
    const r = await chrome.runtime.sendMessage({ action: 'fetch_storage_file', storagePath }).catch(() => null);
    if (!r || r.error || !r.base64) {
      log(`❌ Fichier Firebase introuvable : ${filename || storagePath}`, 1);
      return false;
    }
    const bin = atob(r.base64);
    const arr = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i += 1) arr[i] = bin.charCodeAt(i);
    const blob = new Blob([arr], { type: r.type || 'application/pdf' });
    const file = new File([blob], filename || 'document.pdf', { type: blob.type });
    const dt = new DataTransfer();
    dt.items.add(file);
    inputEl.files = dt.files;
    inputEl.dispatchEvent(new Event('input', { bubbles: true }));
    inputEl.dispatchEvent(new Event('change', { bubbles: true }));
    return true;
  }

  function findAttachmentRoot(keyword) {
    const target = norm(keyword);
    const nodes = document.querySelectorAll('section, fieldset, .oj-form-layout, .oj-panel, .oj-flex, div');
    for (const node of nodes) {
      const text = norm(node.textContent || '');
      if (!text || !text.includes(target)) continue;
      if (node.querySelector('input[type="file"], button, [role="button"]')) return node;
    }
    return null;
  }

  async function removeExistingAttachment(root, kinds) {
    if (!root) return false;
    const buttons = Array.from(root.querySelectorAll('button, [role="button"], a'));
    for (const btn of buttons) {
      const text = `${btn.textContent || ''} ${btn.getAttribute('aria-label') || ''} ${btn.getAttribute('title') || ''}`;
      const normalized = norm(text);
      if (!/remove attachment|remove|delete|supprimer|retirer/.test(normalized)) continue;
      if (kinds.some((kind) => normalized.includes(norm(kind))) || !kinds.length) {
        btn.click();
        await sleep(400);
        return true;
      }
    }
    return false;
  }

  async function ensureAttachment({ label, storagePath, filename, rootKeywords, uploadButtonText, token }) {
    if (!storagePath) {
      log(`⏭️ ${label} : aucun fichier Firebase`, 1);
      return false;
    }
    if (state[token] === `${storagePath}|done`) return true;

    let root = null;
    for (const keyword of rootKeywords) {
      root = findAttachmentRoot(keyword);
      if (root) break;
    }
    if (!root) root = document;

    const removed = await removeExistingAttachment(root, rootKeywords);
    if (removed) log(`🗑️ ${label} : ancienne pièce supprimée`, 1);

    let input = visible('input[type="file"]', root) || root.querySelector('input[type="file"]');
    if (!input && uploadButtonText) {
      const uploadBtn = findButtonByText(uploadButtonText);
      if (uploadBtn) {
        uploadBtn.click();
        await sleep(500);
        input = visible('input[type="file"]', root) || visible('input[type="file"]');
      }
    }
    if (!input) {
      log(`⚠️ ${label} : champ upload introuvable`, 1);
      return false;
    }
    const ok = await setFileInputFromStorage(input, storagePath, filename);
    if (ok) {
      state[token] = `${storagePath}|done`;
      log(`✅ ${label} : ${filename || storagePath.split('/').pop()} (Firebase)`, 1);
      await sleep(700);
      return true;
    }
    return false;
  }

  function deriveGender(profile) {
    const civ = norm(profile.civility || '');
    if (civ.includes('monsieur')) return 'Male';
    if (civ.includes('madame')) return 'Female';
    return '';
  }

  function extractCountryFromLocation(locationValue) {
    const raw = String(locationValue || '').trim();
    if (!raw) return '';
    const parts = raw.split('-').map((part) => part.trim()).filter(Boolean);
    return parts.length ? parts[parts.length - 1] : raw;
  }

  const EUROPEAN_UNION_COUNTRIES = new Set([
    'allemagne', 'autriche', 'belgique', 'bulgarie', 'chypre', 'croatie', 'danemark',
    'espagne', 'estonie', 'finlande', 'france', 'grece', 'hongrie', 'irlande', 'italie',
    'lettonie', 'lituanie', 'luxembourg', 'malte', 'pays-bas', 'pologne', 'portugal',
    'republique tcheque', 'roumanie', 'slovaquie', 'slovenie', 'suede'
  ]);

  function resolveJpMorganWorkAuth(profile, pending) {
    const rows = Array.isArray(profile.jp_morgan_work_authorizations) ? profile.jp_morgan_work_authorizations : [];
    const targetCountry = extractCountryFromLocation(pending?.location || '') || 'France';
    const normCountry = norm(targetCountry);
    const exact = rows.find((row) => norm(row?.country || '') === normCountry);
    const euFallback = EUROPEAN_UNION_COUNTRIES.has(normCountry)
      ? rows.find((row) => norm(row?.country || '') === 'union europeenne')
      : null;
    const fallback = euFallback || rows.find((row) => norm(row?.country || '') === 'france') || rows[0] || null;
    const selected = exact || fallback;
    return {
      country: targetCountry,
      workAuthorized: selected?.work_authorized || 'Yes',
      sponsorshipRequired: selected?.sponsorship_required || 'No'
    };
  }

  async function handleSuccess(pending) {
    if (state.successSent) return;
    const text = norm(document.body?.innerText || '');
    const hasSuccessText = text.includes('thank you for your job application');
    const hasAlreadyApplied = text.includes('you already applied for this job') || text.includes('you may also view other jobs');
    const hasMyApplications = /\/my-profile/i.test(location.pathname || '') && text.includes('under consideration');
    if (!hasSuccessText && !hasAlreadyApplied && !hasMyApplications) return;
    state.successSent = true;
    const successLabel = hasSuccessText
      ? 'Thank you for your job application.'
      : hasAlreadyApplied
        ? 'You already applied for this job.'
        : 'My Applications / Under Consideration';
    log(`🎉 Succès JP Morgan détecté : ${successLabel}`);
    await chrome.runtime.sendMessage({
      action: 'candidature_success',
      bankId: 'jp_morgan',
      jobId: pending.jobId || pending.profile?.__jobId || '',
      jobTitle: pending.jobTitle || pending.profile?.__jobTitle || '',
      companyName: pending.companyName || pending.profile?.__companyName || 'J.P. Morgan',
      offerUrl: pending.offerUrl || pending.profile?.__offerUrl || location.href,
      successType: hasSuccessText ? 'toast' : (hasAlreadyApplied ? 'already_applied' : 'my_applications'),
      successMessage: hasSuccessText ? 'Thank you for your job application.' : (hasAlreadyApplied ? 'You already applied for this job.' : 'Under Consideration')
    }).catch(() => null);
    await chrome.storage.local.remove([PENDING_KEY, TAB_KEY]);
  }

  async function handleTermsAndConditions() {
    ensureBanner(getBannerApi()?.getText() || '⏳ Automatisation Taleos en cours — Ne touchez à rien.');
    const report = blueprint?.getStructureReport?.('terms');
    if (report) log(`Blueprint JP Morgan terms: ${report.ok ? 'OK' : 'KO'} (${report.matchedSelectors.length} sélecteurs)`);
    log('📋 JP Morgan → page Conditions générales');
    if (state.termsAccepted) return;
    // Bouton AGREE : texte exact "AGREE" (majuscules dans l'UI Oracle)
    const agreeBtn = Array.from(document.querySelectorAll('button')).find(
      (b) => /^agree$/i.test(b.textContent.trim())
    );
    if (agreeBtn) {
      state.termsAccepted = true;
      agreeBtn.click();
      log('✅ JP Morgan : Conditions générales acceptées (AGREE)');
    } else {
      log('⚠️ JP Morgan : bouton AGREE introuvable sur la page T&C', 1);
    }
  }

  async function handleEmailStep(profile) {
    ensureBanner(getBannerApi()?.getText() || '⏳ Automatisation Taleos en cours — Ne touchez à rien.');
    const report = blueprint?.getStructureReport?.('email');
    if (report) log(`Blueprint JP Morgan email: ${report.ok ? 'OK' : 'KO'} (${report.matchedSelectors.length} sélecteurs)`);
    const emailInput = findBySelectors([
      'input[type="email"]',
      'input[aria-label*="Email Address" i]',
      'input[id*="email" i]'
    ]);
    auditAndFill('Email', emailInput, profile.email || profile.auth_email);

    const checkbox = findBySelectors([
      'input[type="checkbox"]',
      '[role="checkbox"]',
      'label input[type="checkbox"]'
    ]);
    if (checkbox) {
      const checked = checkbox.checked || checkbox.getAttribute('aria-checked') === 'true';
      if (!checked) {
        checkbox.click();
        log('✅ Terms and conditions : case cochée sans ouvrir le lien', 1);
      } else {
        log('✅ Terms and conditions : case déjà cochée -> Skip', 1);
      }
    } else {
      log('⚠️ Terms and conditions : checkbox introuvable', 1);
    }

    const nextBtn = findButtonByText('Next');
    if (nextBtn && !state.emailSubmitted) {
      state.emailSubmitted = true;
      nextBtn.click();
      log('➡️ JP Morgan : clic sur Next après email/consentement');
    }
  }

  async function handlePinStep() {
    ensureBanner('⏳ Code JP Morgan requis — renseignez les 6 chiffres reçus par email, puis laissez Taleos reprendre automatiquement.');
    const report = blueprint?.getStructureReport?.('pin');
    if (report) log(`Blueprint JP Morgan code: ${report.ok ? 'OK' : 'KO'} (${report.matchedSelectors.length} sélecteurs)`);
    const digits = Array.from({ length: 6 }, (_, idx) => findBySelectors([`#pin-code-${idx + 1}`, `input[id*="pin-code-${idx + 1}"]`]));
    const values = digits.map((el) => String(el?.value || '').trim());
    const filled = values.filter((v) => /^\d$/.test(v)).length;
    log(`🔐 JP Morgan → code email : ${filled}/6 chiffre(s) saisi(s)`);
    if (filled === 6 && !state.pinSubmitted) {
      const verifyBtn = findButtonByText('Verify');
      if (verifyBtn) {
        state.pinSubmitted = true;
        verifyBtn.click();
        log('✅ JP Morgan : clic sur Verify après saisie complète du code');
      }
    }
  }

  async function selectPostalSuggestion() {
    const option = Array.from(document.querySelectorAll('[role="option"], li[role="option"], .oj-listbox-result')).find((el) => {
      const text = norm(el.textContent || '');
      return text.includes('95110') && text.includes('sannois');
    });
    if (option) {
      option.click();
      await sleep(500);
      log('✅ Code postal : suggestion 95110, Sannois, Val-d\'Oise sélectionnée', 1);
      return true;
    }
    return false;
  }

  async function handleSection1(profile) {
    ensureBanner(getBannerApi()?.getText() || '⏳ Automatisation Taleos en cours — Ne touchez à rien.');
    const report = blueprint?.getStructureReport?.('section_1');
    if (report) log(`Blueprint JP Morgan section 1: ${report.ok ? 'OK' : 'KO'} (${report.matchedSelectors.length} sélecteurs)`);
    log('🧾 JP Morgan → audit détaillé Firebase vs formulaire (section 1)');

    // --- Title (Doctor / Miss / Mr. / Mrs. / Ms.) ---
    const civility = norm(profile.civility || '');
    const titleMap = { monsieur: 'Mr.', madame: 'Mrs.', mme: 'Mrs.', miss: 'Miss', ms: 'Ms.' };
    const desiredTitle = titleMap[civility] || (civility.includes('monsieur') ? 'Mr.' : civility.includes('madame') ? 'Mrs.' : '');
    if (desiredTitle) {
      const titleBtns = Array.from(document.querySelectorAll('button.cx-select-pill-section, button[class*="cx-select-pill"]'));
      const titleBtn = titleBtns.find((b) => norm(b.textContent) === norm(desiredTitle));
      if (titleBtn) {
        const alreadySelected = titleBtn.getAttribute('aria-pressed') === 'true' || titleBtn.classList.contains('cx-select-pill-section--selected');
        if (!alreadySelected) { titleBtn.click(); log(`✏️ Titre : → ${desiredTitle}`, 1); }
        else { log(`✅ Titre : ${desiredTitle} -> Skip`, 1); }
      } else {
        log(`⚠️ Titre : bouton '${desiredTitle}' introuvable`, 1);
      }
    }

    // --- Prénom / Middle Name (vider) / Nom ---
    // Firebase uses first_name / last_name (snake_case), legacy: firstname / lastname
    const firstName = profile.first_name || profile.firstname || '';
    const lastName = profile.last_name || profile.lastname || '';
    auditAndFill('Prénom', findBySelectors(['input[name="firstName"]', 'input[id*="firstName" i]', 'input[name*="firstName" i]', 'input[aria-label*="First Name" i]']), firstName);
    // Middle Name MUST be empty — a previous bug could have filled it with the phone number
    const middleNameEl = findBySelectors(['input[name="middleNames"]', 'input[id*="middleNames" i]', 'input[name*="middleNames" i]']);
    if (middleNameEl && getValue(middleNameEl) !== '') {
      log(`🗑️ Middle Name : '${getValue(middleNameEl)}' → vidé (champ non utilisé)`, 1);
      setInputValue(middleNameEl, '');
    }
    auditAndFill('Nom', findBySelectors(['input[name="lastName"]', 'input[id*="lastName" i]', 'input[name*="lastName" i]', 'input[aria-label*="Last Name" i]']), lastName);
    auditAndFill('Email', findBySelectors(['input[name="email"]', 'input[id*="email" i]', 'input[name*="email" i]', 'input[aria-label*="Email" i]']), profile.email || profile.auth_email);

    // --- Téléphone ---
    // The phone field: country code combobox id="country-codes-dropdownphoneNumber" (name="phoneNumber")
    // The digits input has NO id/name — class="input-row__control phone-row__input"
    // Using findPhoneInputs() is reliable; fallback to .phone-row__input class selector
    const { countryCodeInput: phoneCcEl, phoneInput: phoneDigitsEl } = findPhoneInputs();
    const rawPhone = profile.phone || profile['phone-number'] || profile.phone_number || '';
    const phoneNational = normalizeNationalPhoneDigits(rawPhone, profile.phone_country_code || '+33');
    auditAndFill('Indicatif pays', phoneCcEl, profile.phone_country_code || '+33');
    await pickVisibleOption(profile.phone_country_code || '+33');
    // Prefer DOM-traversal result, fallback to class selector — NEVER use id*="phoneNumber" which matches country code combobox
    const phoneInputEl = phoneDigitsEl || findBySelectors(['input.phone-row__input', 'input[aria-label*="Phone Number" i]']);
    auditAndFill('Téléphone', phoneInputEl, phoneNational);

    // --- Adresse ---
    auditAndFill('Pays', findBySelectors(['input[name="country"]', 'input[id*="country-" i]']), profile.country || 'France');
    await pickVisibleOption(profile.country || 'France');
    auditAndFill('Numéro', findBySelectors(['input[name="addressLine1"]', 'input[id*="addressLine1" i]']), (profile.address || '').match(/^\s*(\d+[A-Za-z\-]*)/)?.[1] || '30');
    auditAndFill('Rue', findBySelectors(['input[name="addressLine2"]', 'input[id*="addressLine2" i]']), (profile.address || '').replace(/^\s*\d+[A-Za-z\-]*\s+/, '') || 'rue des Garonnes');
    // postal_code (Firebase snake_case) or legacy zipcode
    const postalCode = profile.postal_code || profile.zipcode || '';
    auditAndFill('Code postal', findBySelectors(['input[name="postalCode"]', 'input[id*="postalCode" i]']), postalCode);
    await sleep(300);
    await selectPostalSuggestion();
    auditAndFill('Ville', findBySelectors(['input[name="city"]', 'input[id*="city-" i]']), profile.city);
    const departmentEl = findBySelectors(['input[name="region2"]', 'input[id*="region2" i]', 'input[name*="region" i]', 'input[id*="region" i]']);
    if (departmentEl) {
      log(`ℹ️ Département : formulaire='${getValue(departmentEl) || '(vide)'}' | Firebase='(piloté via code postal)' -> Skip`, 1);
    }

    const nextBtn = findButtonByText('Next');
    if (nextBtn && !state.nextSection1) {
      state.nextSection1 = true;
      nextBtn.click();
      log('➡️ JP Morgan : section 1 validée, clic sur Next');
    }
  }

  async function handleSection2(profile, pending) {
    ensureBanner(getBannerApi()?.getText() || '⏳ Automatisation Taleos en cours — Ne touchez à rien.');
    const report = blueprint?.getStructureReport?.('section_2');
    if (report) log(`Blueprint JP Morgan section 2: ${report.ok ? 'OK' : 'KO'} (${report.matchedSelectors.length} sélecteurs)`);
    const workAuth = resolveJpMorganWorkAuth(profile, pending);

    auditAndSelectButton('At least 18 years of age', findQuestionContainer('are you at least 18 years of age'), 'Yes');
    auditAndSelectButton(
      'Legally authorized to work in this country',
      findQuestionContainer('for the position you are applying to, are you legally authorized to work in this country'),
      workAuth.workAuthorized
    );
    auditAndSelectButton(
      'Require sponsorship',
      findQuestionContainer('will you now or in the future require sponsorship for an employment-based visa status'),
      workAuth.sponsorshipRequired
    );

    const nextBtn = findButtonByText('Next');
    if (nextBtn && !state.nextSection2) {
      state.nextSection2 = true;
      nextBtn.click();
      log('➡️ JP Morgan : section 2 validée, clic sur Next');
    }
  }

  // ──────────────────────────────────────────────────────────────────────────────
  // Helpers spécifiques au formulaire inline Education / Experience (section 3)
  // ──────────────────────────────────────────────────────────────────────────────

  /**
   * Ouvre un cx-select dans le formulaire inline et sélectionne l'option souhaitée.
   * Gère les deux cas :
   *  - disabled (ex. Degree "contentItemId") → clic sur .cx-select-container
   *  - normal (Month, Year, Country) → clic sur l'input puis typing pour filtrer
   */
  async function selectCxDropdownInForm(label, input, desiredValue, aliases = []) {
    if (!input || !desiredValue) {
      log(`⚠️ ${label} : champ cx-select introuvable dans le formulaire inline`, 1);
      return false;
    }
    const currentRaw = getValue(input);
    if (norm(currentRaw) === norm(desiredValue)) {
      log(`✅ ${label} : '${currentRaw}' -> Skip`, 1);
      return true;
    }
    log(`✏️ ${label} : '${currentRaw}' → '${desiredValue}'`, 1);
    const isDisabled = input.classList.contains('cx-select-input--disabled') || input.readOnly || input.disabled;
    const cxContainer = input.closest('.cx-select-container');
    if (isDisabled && cxContainer) {
      cxContainer.click();
    } else {
      input.click();
      input.focus?.();
    }
    await sleep(400);
    if (!isDisabled) {
      setInputValue(input, desiredValue);
      await sleep(300);
    }
    for (const candidate of [desiredValue, ...aliases]) {
      if (await pickVisibleOption(candidate)) return true;
    }
    if (!isDisabled) {
      input.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', code: 'Enter', bubbles: true }));
      input.dispatchEvent(new Event('change', { bubbles: true }));
    }
    return false;
  }

  /**
   * Remplit le formulaire inline d'éducation une fois qu'il est ouvert.
   * Champs Oracle CX confirmés en production (session 2026-05-12) :
   *   input[name="contentItemId"]          — Diplôme (cx-select disabled)
   *   input[name="educationalEstablishment"] — École (cx-select autocomplete)
   *   input[name="endDate"][0] (id=month-endDate-N) — Mois de fin
   *   input[name="endDate"][1] (id=year-endDate-N)  — Année de fin
   *   input[name="countryCode"]            — Pays
   *   input[name="areaOfStudy"]            — Domaine d'études (texte libre)
   *   button.save-btn                      — Sauvegarder
   */
  async function fillEducationInlineForm(degree, school, gradMonth, gradYear, country, areaOfStudy) {
    await sleep(300);
    const formEl = document.querySelector('.profile-item-content--form');
    if (!formEl) {
      log('⚠️ JP Morgan : formulaire inline éducation non apparu', 1);
      return false;
    }

    // ── Diplôme (cx-select disabled → clic sur .cx-select-container) ───────
    if (degree) {
      const degreeInput = formEl.querySelector('input[name="contentItemId"]') ||
        document.querySelector('input[name="contentItemId"]');
      await selectCxDropdownInForm('Diplôme', degreeInput, degree, [degree.replace(/'/g, '’')]);
    }

    // ── École (cx-select autocomplete serveur) ───────────────────────────────
    if (school) {
      const schoolInput = formEl.querySelector('input[name="educationalEstablishment"]') ||
        document.querySelector('input[name="educationalEstablishment"]');
      if (schoolInput) {
        const currentSchool = getValue(schoolInput);
        if (norm(currentSchool) !== norm(school)) {
          schoolInput.click();
          schoolInput.focus?.();
          await sleep(200);
          setInputValue(schoolInput, school);
          await sleep(700); // attendre suggestions serveur
          const picked = await pickVisibleOption(school);
          if (!picked) {
            // Aucune suggestion : confirmer la valeur tapée
            schoolInput.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', code: 'Enter', bubbles: true }));
            schoolInput.dispatchEvent(new Event('change', { bubbles: true }));
          }
          log(`✏️ École : → ${school}`, 1);
        } else {
          log(`✅ École : '${currentSchool}' -> Skip`, 1);
        }
      }
    }

    // ── Mois de fin (1er input[name="endDate"]) ─────────────────────────────
    if (gradMonth) {
      const endDateInputs = (formEl || document).querySelectorAll('input[name="endDate"]');
      const monthInput = endDateInputs[0] || document.querySelector('input[id^="month-endDate"]');
      await selectCxDropdownInForm('Mois de diplôme', monthInput, gradMonth);
    }

    // ── Année de fin (2e input[name="endDate"]) ──────────────────────────────
    if (gradYear) {
      const endDateInputs = (formEl || document).querySelectorAll('input[name="endDate"]');
      const yearInput = endDateInputs[1] || document.querySelector('input[id^="year-endDate"]');
      await selectCxDropdownInForm('Année de diplôme', yearInput, String(gradYear));
    }

    // ── Pays (cx-select) ────────────────────────────────────────────────────
    if (country) {
      const countryInput = formEl.querySelector('input[name="countryCode"]') ||
        document.querySelector('input[name="countryCode"]');
      await selectCxDropdownInForm('Pays (éducation)', countryInput, country);
    }

    // ── Domaine d'études (texte libre) ──────────────────────────────────────
    const areaInput = formEl.querySelector('input[name="areaOfStudy"]') ||
      document.querySelector('input[name="areaOfStudy"]');
    if (areaInput) {
      if (areaOfStudy) auditAndFill("Domaine d'études", areaInput, areaOfStudy);
      else log("ℹ️ Domaine d'études : non renseigné dans Firebase -> Skip", 1);
    }

    // ── Sauvegarder ──────────────────────────────────────────────────────────
    const saveBtn = document.querySelector('button.save-btn');
    if (saveBtn) {
      saveBtn.click();
      await sleep(800);
      log('💾 JP Morgan : formulaire éducation sauvegardé', 1);
      return true;
    }
    log('⚠️ JP Morgan : bouton Save introuvable dans le formulaire éducation', 1);
    return false;
  }

  async function handleSection3(profile) {
    ensureBanner(getBannerApi()?.getText() || ‘⏳ Automatisation Taleos en cours — Ne touchez à rien.’);
    const report = blueprint?.getStructureReport?.(‘section_3’);
    if (report) log(`Blueprint JP Morgan section 3: ${report.ok ? ‘OK’ : ‘KO’} (${report.matchedSelectors.length} sélecteurs)`);
    log(‘🧾 JP Morgan → audit éducation & expérience (section 3)’);

    // ── Trouver les conteneurs Education / Experience ────────────────────────
    // Chaque section est dans un .profile-item-container distinct identifié par
    // le texte de son bouton "Add Education" ou "Add Experience".
    const allContainers = document.querySelectorAll(‘[class*="standard-apply-flow-profile-item-"]’);
    let eduContainer = null;
    allContainers.forEach((c) => {
      const addBtn = c.querySelector(‘button[class*="new-tile"]’);
      if (norm(addBtn?.textContent || ‘’).includes(‘add education’)) eduContainer = c;
    });

    // ── Paramètres éducation depuis Firebase ────────────────────────────────
    const degreeValue = mapEducationLevelToDegree(profile.education_level, profile.school_type);
    const school = profile.school || profile.university || profile.education_school || ‘’;
    const gradYear = String(profile.graduation_year || profile.grad_year || ‘’);
    const gradMonth = profile.graduation_month || profile.grad_month || ‘’;
    const eduCountry = profile.education_country || profile.country || ‘France’;
    const areaOfStudy = profile.area_of_study || profile.major || profile.field_of_study || ‘’;

    // ── Remplissage éducation ────────────────────────────────────────────────
    if (!eduContainer) {
      log(‘⚠️ JP Morgan section 3 : conteneur Education introuvable’, 1);
    } else if (!state.educationFilled) {
      // Vérifier si un formulaire inline est déjà ouvert (save-btn visible)
      const isEditOpen = !!document.querySelector(‘button.save-btn’);
      if (isEditOpen) {
        log(‘ℹ️ JP Morgan section 3 : formulaire éducation déjà ouvert -> attente’, 1);
      } else {
        const tiles = eduContainer.querySelectorAll(‘.apply-flow-profile-item-tile’);
        if (tiles.length === 0) {
          // Aucun diplôme → cliquer "Add Education"
          const addBtn = eduContainer.querySelector(‘button[class*="new-tile"]’);
          if (addBtn) {
            addBtn.click();
            await sleep(500);
            log(‘➕ JP Morgan : ajout d\’une entrée éducation’, 1);
            const ok = await fillEducationInlineForm(degreeValue, school, gradMonth, gradYear, eduCountry, areaOfStudy);
            if (ok) state.educationFilled = true;
          }
        } else {
          // Éditer la première carte (diplôme principal)
          const firstTile = tiles[0];
          const editBtn = firstTile.querySelector(‘button[aria-label="Edit"]’);
          if (editBtn) {
            editBtn.click();
            await sleep(500);
            log(‘✏️ JP Morgan : édition du diplôme existant (tile[0])’, 1);
            const ok = await fillEducationInlineForm(degreeValue, school, gradMonth, gradYear, eduCountry, areaOfStudy);
            if (ok) state.educationFilled = true;
          } else {
            log(‘⚠️ JP Morgan section 3 : bouton Edit introuvable sur la carte éducation’, 1);
          }
        }
      }
    } else {
      log(‘✅ JP Morgan section 3 : éducation déjà remplie -> Skip’, 1);
    }

    // ── Expérience : laisser inchangé (Oracle HCM récupère le profil existant) ─
    const expTiles = document.querySelectorAll(‘.apply-flow-profile-item-tile’).length - (eduContainer?.querySelectorAll(‘.apply-flow-profile-item-tile’).length || 0);
    log(`ℹ️ JP Morgan → section 3 : ${expTiles} carte(s) expérience laissée(s) inchangée(s)`, 1);

    const nextBtn = findButtonByText(‘Next’);
    if (nextBtn && !state.nextSection3) {
      state.nextSection3 = true;
      nextBtn.click();
      log(‘➡️ JP Morgan : section 3 validée, clic sur Next’);
    }
  }

  async function handleSection4(profile) {
    ensureBanner(getBannerApi()?.getText() || '⏳ Automatisation Taleos en cours — Ne touchez à rien.');
    const report = blueprint?.getStructureReport?.('section_4');
    if (report) log(`Blueprint JP Morgan section 4: ${report.ok ? 'OK' : 'KO'} (${report.matchedSelectors.length} sélecteurs)`);
    log('🧾 JP Morgan → audit détaillé Firebase vs formulaire (section 4)');

    await ensureAttachment({
      label: 'CV',
      storagePath: profile.cv_storage_path,
      filename: profile.cv_filename,
      rootKeywords: ['resume', 'cv'],
      uploadButtonText: 'Upload Resume',
      token: 'resumeUploadToken'
    });
    // Firebase uses letter_storage_path / letter_filename (snake_case); legacy: lm_storage_path / lm_filename
    await ensureAttachment({
      label: 'Lettre de motivation',
      storagePath: profile.letter_storage_path || profile.lm_storage_path,
      filename: profile.letter_filename || profile.lm_filename,
      rootKeywords: ['cover letter', 'motivation'],
      uploadButtonText: 'Upload Cover Letter',
      token: 'coverUploadToken'
    });

    auditAndFill('LinkedIn', findBySelectors(['input[id*="siteLink" i]', 'input[aria-label*="Link 1" i]']), profile.linkedin_url || '');

    const gender = deriveGender(profile) || profile.gender || '';
    if (gender) {
      await selectDropdownValueWithSelectors('Gender', ['input[name*="ORA_GENDER" i]', 'input[id*="ORA_GENDER" i]'], gender, [gender === 'Male' ? 'Male' : 'Female']);
    } else {
      log('⚠️ Gender : impossible à déduire depuis Firebase', 1);
    }
    const militaryTarget = profile.jp_morgan_military_service || 'No';
    await selectDropdownValueWithSelectors(
      'Have you ever served as a member of the armed forces of any country?',
      ['input[name*="emeaMilitaryStatus" i]', 'input[id*="emeaMilitaryStatus" i]'],
      militaryTarget,
      [militaryTarget]
    );

    // Firebase snake_case (first_name/last_name) avec fallback legacy (firstname/lastname)
    const fullName = `${profile.first_name || profile.firstname || ''} ${profile.last_name || profile.lastname || ''}`.trim();
    auditAndFill('E-signature', findBySelectors(['input[name="fullName"]', 'input[id*="fullName" i]', 'input[aria-label*="Full Name" i]']), fullName);

    const submitBtn = findButtonByText('Submit');
    if (submitBtn && !state.submitSection4) {
      if (!state.reviewStartedAt) {
        state.reviewStartedAt = Date.now();
        log('⏳ JP Morgan : pause de 60 secondes pour relecture avant soumission');
        ensureBanner('⏳ Relecture JP Morgan en cours — 60 secondes avant soumission automatique.');
        return;
      }
      const elapsed = Date.now() - state.reviewStartedAt;
      if (elapsed < 60000) {
        const remaining = Math.max(1, Math.ceil((60000 - elapsed) / 1000));
        ensureBanner(`⏳ Relecture JP Morgan en cours — soumission automatique dans ${remaining}s.`);
        return;
      }
      state.submitSection4 = true;
      submitBtn.click();
      log('🚀 JP Morgan : clic final sur Submit après 60 secondes de relecture');
    }
  }

  async function run() {
    if (isRunning) return;
    isRunning = true;
    try {
      const pending = await getPending();
      if (!pending) return;
      const profile = pending.profile || {};
      const detected = blueprint?.detectPage?.() || { key: 'unknown', label: 'Inconnue' };
      log(`🚀 Démarrage JP Morgan sur ${detected.key} (${location.pathname})`);
      await blueprint?.recordLog?.({ page: detected.key, href: location.href });

      await handleSuccess(pending);
      if (state.successSent) return;

      if (detected.key === 'terms') return handleTermsAndConditions();
      if (detected.key === 'email') return handleEmailStep(profile);
      if (detected.key === 'pin') return handlePinStep();
      if (detected.key === 'section_1') return handleSection1(profile);
      if (detected.key === 'section_2') return handleSection2(profile, pending);
      if (detected.key === 'section_3') return handleSection3(profile);
      if (detected.key === 'section_4') return handleSection4(profile);
      if (detected.key === 'offer') {
        ensureBanner(getBannerApi()?.getText() || '⏳ Automatisation Taleos en cours — Ne touchez à rien.');
        const applyBtn = Array.from(document.querySelectorAll('a, button')).find((el) => /apply now/i.test(el.textContent || ''));
        if (applyBtn) {
          applyBtn.click();
          log('🔗 JP Morgan → clic sur Apply Now');
        }
      }
      if (detected.key === 'my_profile_success' || detected.key === 'already_applied' || detected.key === 'success') {
        return handleSuccess(pending);
      }
    } catch (e) {
      log(`❌ Erreur JP Morgan : ${e?.message || e}`);
    } finally {
      isRunning = false;
    }
  }

  function init() {
    if (window.__taleosJpMorganInit) return;
    window.__taleosJpMorganInit = true;
    setInterval(run, 1500);
    const observer = new MutationObserver(() => {
      clearTimeout(window.__taleosJpMorganDebounce);
      window.__taleosJpMorganDebounce = setTimeout(run, 300);
    });
    observer.observe(document.body, { childList: true, subtree: true });
    run();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init, { once: true });
  } else {
    init();
  }
})();
