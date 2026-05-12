/**
 * Taleos - Blueprint J.P. Morgan
 * Cartographie le flux Oracle Candidate Experience observé en production.
 */
(function () {
  'use strict';

  if (globalThis.__TALEOS_JP_MORGAN_BLUEPRINT__) return;

  const LAST_CHECK_KEY = 'taleos_jp_morgan_blueprint_last_check';
  const LOG_KEY = 'taleos_jp_morgan_blueprint_log';
  const MAX_LOG_ENTRIES = 120;

  const TEXT = {
    offer: ['apply now', 'job identification', 'job description', 'prime financial services'],
    terms: ['terms and conditions', 'candidate recruitment privacy policies', 'employment and workforce related privacy notice', 'agree'],
    email: ['email address', 'terms and conditions', 'next'],
    pin: ['confirm your identity', 'send new code', 'verify'],
    section1: ['personal information', 'phone country code', 'house number', 'postal code'],
    section2: ['job application questions', 'at least 18 years of age', 'legally authorized'],
    section3: ['experience', 'education', 'work experience', 'add education', 'add experience'],
    section4: ['more about you', 'resume or additional documents', 'upload cover letter', 'e-signature'],
    success: ['thank you for your job application'],
    alreadyApplied: ['you already applied for this job', 'you may also view other jobs'],
    myProfile: ['my applications', 'under consideration', 'active job applications']
  };

  const PAGE_DEFS = {
    offer: {
      label: 'Offre J.P. Morgan',
      hostIncludes: ['jpmc.fa.oraclecloud.com'],
      pathMatches: [/\/job\//],
      selectorsAny: ['a[href*="/apply/email"]', 'button', 'h1'],
      textPatterns: TEXT.offer
    },
    // Interstitielle Conditions générales — apparaît au DÉBUT d'une NOUVELLE candidature
    // avant la section 1. Même URL que section 1 (/apply/section/1/).
    // Détectée par texte unique "candidate recruitment privacy policies" + bouton "AGREE".
    // Bouton: <button class="button app-dialog__footer-button"> AGREE </button>
    terms: {
      label: 'Conditions générales',
      hostIncludes: ['jpmc.fa.oraclecloud.com'],
      pathMatches: [/\/apply\//],
      selectorsAny: ['button'],
      textPatterns: TEXT.terms
    },
    email: {
      label: 'Email / consentement',
      hostIncludes: ['jpmc.fa.oraclecloud.com'],
      pathMatches: [/\/apply\/email/],
      selectorsAny: ['input[type="email"]', 'input[type="checkbox"]', 'button'],
      textPatterns: TEXT.email
    },
    pin: {
      label: 'Code email',
      hostIncludes: ['jpmc.fa.oraclecloud.com'],
      pathMatches: [/\/apply\/email/],
      selectorsAny: ['#pin-code-1', '#pin-code-6', 'button'],
      textPatterns: TEXT.pin
    },
    section_1: {
      label: 'Section 1 - Personal Info',
      hostIncludes: ['jpmc.fa.oraclecloud.com'],
      pathMatches: [/\/apply\/section\/1/],
      // Confirmed IDs (suffixes vary per session, use name= selectors):
      // firstName-N (name=firstName), middleNames-N (name=middleNames), lastName-N (name=lastName)
      // email-N (name=email), country-codes-dropdownphoneNumber (name=phoneNumber, country code combobox)
      // phone digits: no id/name — class="input-row__control phone-row__input"
      // country-N (name=country), addressLine1-N, addressLine2-N, postalCode-N, city-N, region2-N
      // Title: buttons.cx-select-pill-section (Doctor/Miss/Mr./Mrs./Ms.)
      selectorsAny: ['input[name="firstName"]', 'input[name="lastName"]', 'input[name="postalCode"]', 'button.cx-select-pill-section'],
      textPatterns: TEXT.section1
    },
    section_2: {
      label: 'Section 2 - Questions',
      hostIncludes: ['jpmc.fa.oraclecloud.com'],
      pathMatches: [/\/apply\/section\/2/],
      selectorsAny: ['button', '[role="radio"]', '[aria-pressed]'],
      textPatterns: TEXT.section2
    },
    section_3: {
      label: 'Section 3 - Education & Experience',
      hostIncludes: ['jpmc.fa.oraclecloud.com'],
      pathMatches: [/\/apply\/section\/3/],
      // Structure des cartes (état FERMÉ) :
      //   .apply-flow-profile-item-tile                     — carte fermée
      //   .apply-flow-profile-item-tile__summary-title      — titre (ex. "Master's Degree" ou "Analyst")
      //   .apply-flow-profile-item-tile__summary-subtitle   — sous-titre (ex. "12/2018" ou "NATIXIS CIB …")
      //   button[aria-label="Edit"]  .apply-flow-profile-item-tile__edit-item-icon  — crayon edit
      //   button[aria-label="Delete"] .apply-flow-profile-item-tile__delete-icon    — croix supprimer
      //   button.apply-flow-profile-item-tile__new-tile     — "Add Education" / "Add Experience"
      //
      // Différencier Education vs Expérience :
      //   container Education  → button[class*="new-tile"] dont textContent = "Add Education"
      //   container Experience → button[class*="new-tile"] dont textContent = "Add Experience"
      //   Les deux sont dans des .profile-item-container[class*="standard-apply-flow-profile-item-"]
      //
      // Formulaire inline ÉDUCATION (s'ouvre dans .profile-item-content--form) :
      //   input[name="contentItemId"]          — Diplôme (cx-select, DISABLED → cliquer .cx-select-container)
      //   input[name="educationalEstablishment"] — École (cx-select autocomplete, role=combobox)
      //   input[name="endDate"] [0] (id=month-endDate-N) — Mois de fin (cx-select, role=combobox)
      //   input[name="endDate"] [1] (id=year-endDate-N)  — Année de fin (cx-select, role=combobox)
      //   input[name="countryCode"]            — Pays (cx-select, role=combobox)
      //   input[name="areaOfStudy"]            — Domaine d'études (texte libre, class=input-row__control)
      //   button.save-btn                      — Sauvegarder
      //   button.cancel-btn                    — Annuler
      //
      // Formulaire inline EXPÉRIENCE (même structure) :
      //   input[name="employerName"]           — Entreprise (texte libre)
      //   input[name="jobTitle"]               — Intitulé de poste (texte libre)
      //   input[name="startDate"] [0/1]        — Début Mois / Année (cx-select)
      //   input[name="endDate"] [0/1]          — Fin Mois / Année (cx-select)
      //   input[name="countryCode"]            — Pays (cx-select)
      //   input[name="employerCity"]           — Ville (texte libre)
      //   input[name="achievements"]           — Description (textarea-like)
      //   button.save-btn / button.cancel-btn
      //
      // Options cx-select : .cx-select__list-item--content (PAS role="option")
      selectorsAny: [
        'button[aria-label="Edit"]',
        '.apply-flow-profile-item-tile',
        'button[class*="new-tile"]',
        '.apply-flow-profile-item-tile__summary-title'
      ],
      textPatterns: TEXT.section3
    },
    section_4: {
      label: 'Section 4 - More About You',
      hostIncludes: ['jpmc.fa.oraclecloud.com'],
      pathMatches: [/\/apply\/section\/4/],
      // Confirmed IDs: siteLink-1-N (name=siteLink-1), fullName-N (name=fullName)
      // Gender: id=FR-STANDARD-ORA_GENDER-STANDARD-N (name=FR-STANDARD-ORA_GENDER-STANDARD) — cx-select dropdown
      // Military: id=FR-DFF-emeaMilitaryStatus-ATTRIBUTE13-N (name=FR-DFF-emeaMilitaryStatus-ATTRIBUTE13) — cx-select dropdown
      // cx-select options: .cx-select__list-item--content (NOT role="option")
      // Military options: "Yes, I am a UK Veteran" / "Yes, I am a Veteran of a country other than the UK" /
      //   "Yes, I am a UK Reservist" / "No" / "I do not wish to answer" / "Yes, I am a Reservist of a country other than the UK"
      selectorsAny: ['input[type="file"]', 'input[name="siteLink-1"]', 'input[name="fullName"]', 'input[name*="ORA_GENDER"]'],
      textPatterns: TEXT.section4
    },
    success: {
      label: 'Succès explicite',
      hostIncludes: ['jpmc.fa.oraclecloud.com'],
      selectorsAny: ['div', 'main'],
      textPatterns: TEXT.success
    },
    already_applied: {
      label: 'Déjà candidaté',
      hostIncludes: ['jpmc.fa.oraclecloud.com'],
      selectorsAny: ['div', 'main'],
      textPatterns: TEXT.alreadyApplied
    },
    my_profile_success: {
      label: 'My Applications',
      hostIncludes: ['jpmc.fa.oraclecloud.com'],
      pathMatches: [/\/my-profile/],
      selectorsAny: ['main', 'table', 'section'],
      textPatterns: TEXT.myProfile
    }
  };

  function normalizeText(value) {
    return String(value || '')
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .toLowerCase()
      .replace(/\s+/g, ' ')
      .trim();
  }

  function getPageText(doc = document) {
    return normalizeText(doc.body?.innerText || doc.body?.textContent || '');
  }

  function isVisible(el) {
    if (!el) return false;
    const style = globalThis.getComputedStyle ? getComputedStyle(el) : null;
    const rect = typeof el.getBoundingClientRect === 'function' ? el.getBoundingClientRect() : null;
    return !!rect && rect.width > 0 && rect.height > 0 && style?.display !== 'none' && style?.visibility !== 'hidden';
  }

  function queryVisible(doc, selector) {
    try {
      return Array.from(doc.querySelectorAll(selector)).find(isVisible) || null;
    } catch (_) {
      return null;
    }
  }

  function hostMatches(def, host) {
    return (def.hostIncludes || []).every((part) => host.includes(part));
  }

  function pathMatches(def, pathname, href) {
    return (def.pathMatches || []).some((re) => re.test(pathname) || re.test(href));
  }

  function countTextMatches(text, patterns) {
    return (patterns || []).filter((pattern) => text.includes(normalizeText(pattern))).length;
  }

  function detectPage(doc = document, href = location.href) {
    const url = new URL(href, location.origin);
    const host = String(url.hostname || '').toLowerCase();
    const pathname = String(url.pathname || '').toLowerCase();
    const fullHref = String(url.href || '').toLowerCase();
    const text = getPageText(doc);

    if (text.includes(normalizeText(TEXT.success[0]))) {
      return { key: 'success', score: 100, label: PAGE_DEFS.success.label };
    }
    if (text.includes(normalizeText(TEXT.alreadyApplied[0]))) {
      return { key: 'already_applied', score: 100, label: PAGE_DEFS.already_applied.label };
    }
    // Interstitielle T&C : texte unique "candidate recruitment privacy policies" + bouton AGREE
    // (même URL que section 1, doit être détectée en priorité)
    if (text.includes(normalizeText('candidate recruitment privacy policies'))) {
      return { key: 'terms', score: 100, label: PAGE_DEFS.terms.label };
    }

    let best = { key: 'unknown', score: 0, label: 'Inconnue' };
    for (const [key, def] of Object.entries(PAGE_DEFS)) {
      let score = 0;
      if (hostMatches(def, host)) score += 2;
      if (pathMatches(def, pathname, fullHref)) score += 2;
      score += countTextMatches(text, def.textPatterns);
      if ((def.selectorsAny || []).some((selector) => queryVisible(doc, selector))) score += 2;
      if (score > best.score) best = { key, score, label: def.label };
    }
    return best;
  }

  function validateCurrentPage(expected) {
    const detected = detectPage();
    const targets = Array.isArray(expected) ? expected : [expected];
    const ok = targets.includes(detected.key);
    const result = {
      ok,
      detected: detected.key,
      label: detected.label,
      href: location.href,
      checkedAt: new Date().toISOString()
    };
    try {
      chrome.storage.local.set({ [LAST_CHECK_KEY]: result });
    } catch (_) {}
    return result;
  }

  function getStructureReport(expectedKey, doc = document) {
    const def = PAGE_DEFS[expectedKey];
    if (!def) return { ok: false, expectedKey, error: 'unknown blueprint page' };
    const matchedSelectors = (def.selectorsAny || []).filter((selector) => !!queryVisible(doc, selector));
    return {
      expectedKey,
      ok: matchedSelectors.length > 0,
      matchedSelectors,
      missingSelectors: (def.selectorsAny || []).filter((selector) => !matchedSelectors.includes(selector)),
      textMatches: (def.textPatterns || []).filter((pattern) => getPageText(doc).includes(normalizeText(pattern))),
      href: location.href
    };
  }

  async function recordLog(entry) {
    try {
      const out = await chrome.storage.local.get([LOG_KEY]);
      const current = Array.isArray(out[LOG_KEY]) ? out[LOG_KEY] : [];
      current.push({
        at: new Date().toISOString(),
        href: location.href,
        entry
      });
      while (current.length > MAX_LOG_ENTRIES) current.shift();
      await chrome.storage.local.set({ [LOG_KEY]: current });
    } catch (_) {}
  }

  globalThis.__TALEOS_JP_MORGAN_BLUEPRINT__ = {
    detectPage,
    validateCurrentPage,
    getStructureReport,
    recordLog
  };
})();
