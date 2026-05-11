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
    emailSubmitted: false,
    pinSubmitted: false,
    nextSection1: false,
    nextSection2: false,
    nextSection3: false,
    submitSection4: false,
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
        option.classList.contains('selected') ||
        option.classList.contains('oj-selected');
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
    const labels = Array.from(document.querySelectorAll('label, span, div')).filter((el) => {
      const text = norm(el.textContent || '');
      return text && text.includes(target);
    });
    for (const label of labels) {
      const forId = label.getAttribute?.('for');
      if (forId) {
        const direct = document.getElementById(forId);
        if (direct) return direct;
      }
      const root = label.closest('.oj-form-layout, .oj-flex-item, .oj-form, .oj-panel, .oj-flex, div');
      const field = root?.querySelector?.('input, textarea, select, [role="combobox"] input');
      if (field) return field;
    }
    return null;
  }

  function findQuestionContainer(textNeedle) {
    const target = norm(textNeedle);
    const nodes = document.querySelectorAll('section, fieldset, .oj-form-layout, .oj-panel, .oj-flex, [data-testid], div');
    for (const node of nodes) {
      const text = norm(node.textContent || '');
      if (!text || !text.includes(target)) continue;
      const hasButtons = node.querySelector('button, [role="radio"], [aria-pressed], [aria-checked]');
      if (hasButtons) return node;
    }
    return null;
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
    const options = Array.from(document.querySelectorAll('[role="option"], li[role="option"], .oj-listbox-result, .oj-listview-item'));
    const option = options.find((el) => {
      const text = norm(el.textContent || '');
      return text === target || text.includes(target) || target.includes(text);
    });
    if (option) {
      option.click();
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

    auditAndFill('Prénom', findBySelectors(['input[id*="firstName" i]', 'input[name*="firstName" i]', 'input[aria-label*="First Name" i]']), profile.firstname);
    auditAndFill('Nom', findBySelectors(['input[id*="lastName" i]', 'input[name*="lastName" i]', 'input[aria-label*="Last Name" i]']), profile.lastname);
    auditAndFill('Email', findBySelectors(['input[id*="email" i]', 'input[name*="email" i]', 'input[aria-label*="Email" i]']), profile.email || profile.auth_email);

    const phoneCcEl = findFieldByLabel('Phone country code') || findBySelectors(['input[aria-label*="Phone country code" i]', 'input[id*="countryCode" i]', 'input[id*="country-code" i]', '[role="combobox"][aria-label*="country code" i] input']);
    const phoneNational = normalizeNationalPhoneDigits(profile['phone-number'] || profile.phone_number || '', profile.phone_country_code || '+33');
    auditAndFill('Indicatif pays', phoneCcEl, profile.phone_country_code || '+33');
    await pickVisibleOption(profile.phone_country_code || '+33');
    auditAndFill('Téléphone', findFieldByLabel('Phone number') || findBySelectors(['input[type="tel"]', 'input[aria-label*="Phone Number" i]', 'input[id*="phoneNumber" i]']), phoneNational);

    auditAndFill('Pays', findFieldByLabel('Country') || findBySelectors(['input[aria-label*="Country" i]', '[role="combobox"][aria-label*="Country" i] input']), profile.country || 'France');
    await pickVisibleOption(profile.country || 'France');
    auditAndFill('Numéro', findFieldByLabel('House Number') || findBySelectors(['input[aria-label*="House Number" i]', 'input[id*="houseNumber" i]']), (profile.address || '').match(/^\s*(\d+[A-Za-z\-]*)/)?.[1] || '30');
    auditAndFill('Rue', findFieldByLabel('Street Name') || findBySelectors(['input[aria-label*="Street Name" i]', 'input[id*="streetName" i]']), (profile.address || '').replace(/^\s*\d+[A-Za-z\-]*\s+/, '') || 'rue des Garonnes');
    auditAndFill('Code postal', findFieldByLabel('Postal Code') || findBySelectors(['input[aria-label*="Postal Code" i]', 'input[id*="postalCode" i]', '[role="combobox"][aria-label*="Postal Code" i] input']), profile.zipcode);
    await sleep(300);
    await selectPostalSuggestion();
    auditAndFill('Ville', findFieldByLabel('City') || findBySelectors(['input[aria-label*="City" i]', 'input[id*="city" i]', '[role="combobox"][aria-label*="City" i] input']), profile.city);
    const departmentEl = findFieldByLabel('Department') || findFieldByLabel('State') || findBySelectors(['input[aria-label*="Department" i]', 'input[aria-label*="State" i]', 'input[id*="region" i]', 'input[id*="department" i]']);
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

    auditAndSelectButton('At least 18 years of age', findQuestionContainer('at least 18 years of age'), 'Yes');
    auditAndSelectButton('Legally authorized to work in this country', findQuestionContainer('legally authorized to work in this country'), workAuth.workAuthorized);
    auditAndSelectButton('Require sponsorship', findQuestionContainer('require sponsorship'), workAuth.sponsorshipRequired);

    const nextBtn = findButtonByText('Next');
    if (nextBtn && !state.nextSection2) {
      state.nextSection2 = true;
      nextBtn.click();
      log('➡️ JP Morgan : section 2 validée, clic sur Next');
    }
  }

  async function handleSection3() {
    ensureBanner(getBannerApi()?.getText() || '⏳ Automatisation Taleos en cours — Ne touchez à rien.');
    const report = blueprint?.getStructureReport?.('section_3');
    if (report) log(`Blueprint JP Morgan section 3: ${report.ok ? 'OK' : 'KO'} (${report.matchedSelectors.length} sélecteurs)`);
    const educationCards = document.querySelectorAll('[data-testid*="education"], [id*="education"], .education-card').length;
    const experienceCards = document.querySelectorAll('[data-testid*="experience"], [id*="experience"], .experience-card').length;
    log(`ℹ️ JP Morgan → section 3 : ${educationCards} bloc(s) éducation et ${experienceCards} bloc(s) expérience visibles -> Skip (pas de référentiel Firebase détaillé)`);
    const nextBtn = findButtonByText('Next');
    if (nextBtn && !state.nextSection3) {
      state.nextSection3 = true;
      nextBtn.click();
      log('➡️ JP Morgan : section 3 laissée inchangée, clic sur Next');
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
    await ensureAttachment({
      label: 'Lettre de motivation',
      storagePath: profile.lm_storage_path,
      filename: profile.lm_filename,
      rootKeywords: ['cover letter', 'motivation'],
      uploadButtonText: 'Upload Cover Letter',
      token: 'coverUploadToken'
    });

    auditAndFill('LinkedIn', findBySelectors(['input[id*="siteLink" i]', 'input[aria-label*="Link 1" i]']), profile.linkedin_url || '');

    const gender = deriveGender(profile);
    if (gender) {
      const genderContainer = findQuestionContainer('gender') || document;
      auditAndSelectButton('Gender', genderContainer, gender);
    } else {
      log('⚠️ Gender : impossible à déduire depuis Firebase', 1);
    }
    const militaryTarget = profile.jp_morgan_military_service || 'No';
    const militaryContainer = findQuestionContainer('served in the armed forces') ||
      findQuestionContainer('military') ||
      findQuestionContainer('veteran');
    auditAndSelectButton('Military service', militaryContainer, militaryTarget);

    const fullName = `${profile.firstname || ''} ${profile.lastname || ''}`.trim();
    auditAndFill('E-signature', findBySelectors(['input[id*="fullName" i]', 'input[aria-label*="Full Name" i]']), fullName);

    const submitBtn = findButtonByText('Submit');
    if (submitBtn && !state.submitSection4) {
      state.submitSection4 = true;
      submitBtn.click();
      log('🚀 JP Morgan : clic final sur Submit');
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

      if (detected.key === 'email') return handleEmailStep(profile);
      if (detected.key === 'pin') return handlePinStep();
      if (detected.key === 'section_1') return handleSection1(profile);
      if (detected.key === 'section_2') return handleSection2(profile, pending);
      if (detected.key === 'section_3') return handleSection3();
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
