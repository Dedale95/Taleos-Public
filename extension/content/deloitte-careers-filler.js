/**
 * Taleos - Remplissage automatique Deloitte (Workday)
 * Flux: Connexion → Utiliser ma dernière candidature → Formulaire complet
 *
 * — Méthodologie Workday (étape 1 "Mes données personnelles") —
 * 1. Validation des champs : Workday ne met à jour l'état React qu'après focus puis blur.
 *    On simule le comportement utilisateur : pour chaque champ obligatoire, scroll en vue,
 *    clic sur le champ (focus), puis clic sur un élément neutre (ex. titre "Mes données personnelles")
 *    pour provoquer le blur. Voir workdayClickThenClickAway() et refreshWorkdayRequiredFields().
 * 2. Menus déroulants (listbox) : toujours SÉLECTIONNER une option (ouvrir le bouton, cliquer
 *    l'option) au lieu de remplir un input en dur. Ex. "Comment nous avez-vous connus" :
 *    ouvrir le champ → choisir "Site Deloitte Careers" dans la liste ; "Type d'appareil téléphonique" :
 *    ouvrir → "Mobile Personnel" ; "Indicatif de pays" : ouvrir → "France (+33)" ou "Royaume-Uni (+44)"
 *    selon Firebase. Un simple fillInput() sans sélection ne valide pas côté Workday.
 * 3. Indicatif pays téléphone : pris depuis Firebase (phone_country_code ou phoneCountryCode),
 *    ex. +33 = France (+33), +44 = Royaume-Uni (+44). Pas de défaut +33 si l'utilisateur a saisi +44.
 */
(function() {
  'use strict';

  const BANNER_ID = 'taleos-deloitte-automation-banner';
  const MAX_PENDING_AGE = 10 * 60 * 1000;
  const SITE_DELOITTE_CAREERS = 'Site Deloitte Careers';

  const STEP = (n, msg) => `[STEP ${n}] ${msg}`;
  function log(msg, stepNum) {
    const prefix = stepNum != null ? STEP(stepNum, '') : '';
    console.log(`[${new Date().toLocaleTimeString('fr-FR')}] [Taleos Deloitte] ${prefix}${msg}`);
  }

  function showBanner() {
    if (document.getElementById(BANNER_ID)) return;
    const banner = document.createElement('div');
    banner.id = BANNER_ID;
    banner.textContent = '⏳ Automatisation Taleos en cours — Ne touchez à rien.';
    Object.assign(banner.style, {
      position: 'fixed', top: '0', left: '0', right: '0', zIndex: '2147483647',
      background: 'linear-gradient(135deg, #86bc25 0%, #43b02a 100%)', color: 'white',
      padding: '10px 20px', fontSize: '14px', fontWeight: '600', textAlign: 'center',
      fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
      boxShadow: '0 2px 10px rgba(0,0,0,0.2)'
    });
    document.body?.insertBefore(banner, document.body.firstChild);
  }

  function hideBanner() {
    document.getElementById(BANNER_ID)?.remove();
  }

  function fillInput(el, value) {
    if (!el || value == null || value === '') return;
    const str = String(value).trim();
    el.focus();
    const nativeSetter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set;
    if (nativeSetter) nativeSetter.call(el, str);
    else el.value = str;
    el.dispatchEvent(new Event('input', { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
    el.blur();
  }

  /**
   * Valider les champs Workday comme en manuel : clic sur la case puis clic ailleurs.
   * Pour chaque champ obligatoire : clic sur l'input (focus) puis clic sur un élément neutre (blur).
   */
  function workdayClickThenClickAway() {
    try {
      const firstnameEl = document.getElementById('name--legalName--firstName') || document.querySelector('input[name="legalName--firstName"]');
      const lastnameEl = document.getElementById('name--legalName--lastName') || document.querySelector('input[name="legalName--lastName"]');
      const addressEl = document.querySelector('input[id*="address"], input[name*="address"]') || findInputByLabel(['nature et nom de la voie', 'address', 'adresse']);
      const cityEl = findInputByLabel(['ville', 'city']);
      const zipEl = findInputByLabel(['code postal', 'postal code', 'zip']);
      const phoneEl = document.getElementById('phoneNumber--phoneNumber') || document.querySelector('input[name="phoneNumber"][id*="phoneNumber"]') || document.querySelector('input[name="phoneNumber"]');
      const hearInput = findInputByLabel(['comment nous avez-vous connus', 'how did you hear about us']);
      const fields = [
        { el: firstnameEl, label: 'Prénom(s)' },
        { el: lastnameEl, label: 'Nom de famille' },
        { el: addressEl, label: 'Nature et nom de la voie' },
        { el: cityEl, label: 'Ville' },
        { el: zipEl, label: 'Code postal' },
        { el: phoneEl, label: 'Numéro de téléphone' },
        { el: hearInput, label: 'Comment nous avez-vous connus ?' }
      ].filter(function (x) { return x.el && x.el.offsetParent; });
      const elsewhere = document.querySelector('h2[data-automation-id="sectionHeader"], [role="heading"][aria-level="2"], h2') || document.body;
      fields.forEach(function (item, index) {
        const delay = index * 280;
        setTimeout(function () {
          try {
            scrollIntoViewIfNeeded(item.el);
            item.el.focus();
            item.el.click();
            log('   🔁 ' + item.label + ' : clic sur la case (validation Workday)', 5);
          } catch (_) {}
        }, delay);
        setTimeout(function () {
          try {
            elsewhere.click();
          } catch (_) {}
        }, delay + 120);
        setTimeout(function () {
          try {
            if (document.activeElement === item.el) {
              item.el.blur();
            }
          } catch (_) {}
        }, delay + 180);
      });
    } catch (e) {
      log('   ❌ workdayClickThenClickAway: ' + (e && e.message), 5);
    }
  }

  /** Remplir seulement si vide ou différent de Firebase (log skip ou remplacer). Écrit la valeur telle quelle (avec espaces). */
  function fillInputIfNeeded(el, value, label) {
    if (!el) return false;
    const valueTrimmed = value != null ? String(value).trim() : '';
    if (!valueTrimmed) {
      log('   ⏭️  ' + label + ' : pas de valeur Firebase → Skip', 5);
      return false;
    }
    const current = (el.value || '').trim();
    const currentNorm = current.replace(/\s/g, '');
    const targetNorm = valueTrimmed.replace(/\s/g, '');
    if (currentNorm === targetNorm || (currentNorm.length >= 10 && targetNorm.length >= 10 && currentNorm.slice(-10) === targetNorm.slice(-10))) {
      log('   ✅ ' + label + ' : Déjà correct (Firebase identique) → Skip', 5);
      return false;
    }
    log('   ✏️  ' + label + ' : Remplacer "' + (current || '(vide)') + '" par "' + valueTrimmed + '" (Firebase)', 5);
    fillInput(el, valueTrimmed);
    return true;
  }

  /** Scroll élément en vue (évite "click intercepted" sur Workday). */
  function scrollIntoViewIfNeeded(el) {
    if (!el || typeof el.scrollIntoView !== 'function') return;
    try {
      el.scrollIntoView({ behavior: 'instant', block: 'center' });
    } catch (_) {}
  }

  /** Workday : ouvrir un bouton listbox (id, name ou selector) et cliquer l'option dont le label correspond (option cliquée après 400ms). */
  function clickWorkdayListboxOption(buttonSelector, optionLabelOrValue, label) {
    let btn = null;
    if (typeof buttonSelector === 'string') {
      if (!buttonSelector.includes(' ') && buttonSelector.length <= 50) {
        btn = document.getElementById(buttonSelector) || document.querySelector('button[name="' + buttonSelector + '"]');
        if (!btn && buttonSelector.indexOf('--') >= 0) {
          const namePart = buttonSelector.replace(/^[^-]+--/, '');
          if (namePart) btn = document.querySelector('button[name="' + namePart + '"]');
        }
      }
      if (!btn) btn = document.querySelector(buttonSelector);
    } else {
      btn = buttonSelector;
    }
    if (!btn || !btn.offsetParent) {
      log('   ⏭️  ' + label + ' : bouton non trouvé → Skip', 5);
      return false;
    }
    scrollIntoViewIfNeeded(btn);
    const currentAria = (btn.getAttribute('aria-label') || '').trim();
    const target = (optionLabelOrValue || '').trim().toLowerCase();
    const isPlaceholder = /sélectionnez une valeur|select a value/i.test(currentAria) || (btn.getAttribute('value') === '' || !btn.getAttribute('value'));
    if (!isPlaceholder && currentAria && target && (currentAria.toLowerCase().includes(target) || (target.includes('monsieur') && currentAria.includes('Monsieur')) || (target.includes('madame') && currentAria.includes('Madame')))) {
      log('   ✅ ' + label + ' : Déjà sélectionné (' + currentAria + ') → Skip', 5);
      return false;
    }
    try {
      btn.click();
    } catch (e) {
      log('   ❌ ' + label + ' : erreur clic bouton ' + e.message, 5);
      return false;
    }
    const name = btn.getAttribute('name');
    setTimeout(function tryClickOption() {
      // NB: pas de sélecteur [value!=""] car non supporté par querySelectorAll → provoquait un SyntaxError et des boucles infinies
      const opts = document.querySelectorAll('[data-automation-id="promptOption"], [data-automation-id="menuItem"], [data-automation-id="selectedItem"], [role="option"]');
      for (const opt of opts) {
        const t = (opt.textContent || opt.getAttribute('aria-label') || opt.getAttribute('data-automation-label') || '').trim().toLowerCase();
        const v = (opt.getAttribute && opt.getAttribute('value')) || '';
        if (!t && !v) continue;
        // Pour "Mobile Personnel" : exiger "mobile" dans l'option (éviter de cocher "Fixe Personnel")
        const match = t.includes(target) || (target.includes('monsieur') && t.includes('monsieur')) || (target.includes('madame') && t.includes('madame')) ||
            (target.includes('mobile') && t.includes('mobile')) ||
            (target.includes('personnel') && t.includes('personnel') && !target.includes('mobile')) ||
            (target.includes('+33') && t.includes('+33')) || (target.includes('+44') && t.includes('+44')) || (target.includes('france') && t.includes('france')) || (target.includes('royaume') && t.includes('royaume'));
        if (match && opt.offsetParent !== null) {
          opt.click();
          log('   ✏️  ' + label + ' : Sélectionné "' + (opt.textContent || opt.getAttribute('aria-label') || opt.getAttribute('data-automation-label') || t).trim() + '" (Firebase)', 5);
          return;
        }
      }
      log('   ⏭️  ' + label + ' : option non trouvée après ouverture → Skip', 5);
    }, 400);
    return true;
  }

  function fillSelect(el, value) {
    if (!el || value == null || value === '') return;
    const str = String(value).trim().toLowerCase();
    const opt = Array.from(el.options || []).find(o => {
      const v = (o.value || '').toLowerCase();
      const t = (o.textContent || '').trim().toLowerCase();
      return v === str || t === str || t.includes(str) || str.includes(t);
    });
    if (opt) {
      el.value = opt.value;
      el.dispatchEvent(new Event('change', { bubbles: true }));
    }
  }

  // « Rafraîchir » un champ comme si l'utilisateur avait cliqué dedans
  function touchField(el, label) {
    if (!el || !el.offsetParent) return;
    try {
      const rect = el.getBoundingClientRect();
      const cx = rect.left + rect.width / 2;
      const cy = rect.top + rect.height / 2;
      const mouseOpts = { bubbles: true, cancelable: true, clientX: cx, clientY: cy };
      el.dispatchEvent(new MouseEvent('mousedown', mouseOpts));
      el.dispatchEvent(new MouseEvent('mouseup', mouseOpts));
      el.click();
      el.focus();
      el.dispatchEvent(new Event('input', { bubbles: true }));
      el.dispatchEvent(new Event('change', { bubbles: true }));
      el.blur();
      log('   🔁 ' + label + ' : champ actualisé par clic simulé (Workday validation)', 5);
    } catch (_) {}
  }

  // Certains champs obligatoires gardent l'erreur tant qu'il n'y a pas eu de « vrai » clic / blur.
  // On simule un touch (focus + input/change + blur) sur tous les champs obligatoires pour que Workday valide.
  function refreshWorkdayRequiredFields() {
    try {
      const firstnameEl = document.getElementById('name--legalName--firstName') || document.querySelector('input[name="legalName--firstName"]');
      const lastnameEl = document.getElementById('name--legalName--lastName') || document.querySelector('input[name="legalName--lastName"]');
      const addressEl = document.querySelector('input[id*="address"], input[name*="address"]') || findInputByLabel(['nature et nom de la voie', 'address', 'adresse']);
      const cityEl = findInputByLabel(['ville', 'city']);
      const zipEl = findInputByLabel(['code postal', 'postal code', 'zip']);
      const phoneEl = document.getElementById('phoneNumber--phoneNumber') || document.querySelector('input[name="phoneNumber"][id*="phoneNumber"]') || document.querySelector('input[name="phoneNumber"]');
      const hearInput = findInputByLabel(['comment nous avez-vous connus', 'how did you hear about us']);

      if (firstnameEl) touchField(firstnameEl, 'Prénom (refresh)');
      if (lastnameEl) touchField(lastnameEl, 'Nom de famille (refresh)');
      if (addressEl) touchField(addressEl, 'Adresse (refresh)');
      if (cityEl) touchField(cityEl, 'Ville (refresh)');
      if (zipEl) touchField(zipEl, 'Code postal (refresh)');
      if (phoneEl) touchField(phoneEl, 'Numéro de téléphone (refresh)');
      if (hearInput) touchField(hearInput, 'Comment nous avez-vous connus ? (refresh)');

      // Cas Workday avec liste déroulante (screenshot) : bouton listbox + pill sélectionné.
      const hearTrigger = document.querySelector(
        'button[aria-haspopup="listbox"][aria-label*="Comment nous avez-vous connus"], ' +
        'button[aria-haspopup="listbox"][aria-label*="How did you hear about us"]'
      );
      if (hearTrigger && hearTrigger.offsetParent !== null) {
        try {
          // Premier clic : ouvrir la liste si besoin
          hearTrigger.click();
          setTimeout(function() {
            const selectedPill = document.querySelector(
              '[data-automation-id="selectedItem"][title*="Site Deloitte Careers"], ' +
              '[data-automation-id="selectedItem"][aria-label*="Site Deloitte Careers"]'
            );
            if (selectedPill && selectedPill.offsetParent !== null) {
              // Clic sur le pill (sélection réelle)
              selectedPill.click();
            }
            // Validation/fermeture exactement comme quand tu appuies sur Entrée.
            const enterEvent = new KeyboardEvent('keydown', {
              key: 'Enter',
              code: 'Enter',
              keyCode: 13,
              which: 13,
              bubbles: true
            });
            (document.activeElement || hearTrigger).dispatchEvent(enterEvent);
            log('   🔁 Comment nous avez-vous connus ? (listbox refresh) : sélection + touche Entrée simulée', 5);
          }, 250);
        } catch (_) {}
      }
    } catch (_) {}
  }

  function findLabelAndInput(labelTexts) {
    const labels = Array.from(document.querySelectorAll('label, [data-automation-id="label"], span[role="presentation"]'));
    for (const label of labels) {
      const text = (label.textContent || '').trim();
      const match = labelTexts.some(t => text.toLowerCase().includes(t.toLowerCase()));
      if (match) {
        const forId = label.getAttribute('for');
        const input = forId ? document.getElementById(forId) : null;
        if (input) return input;
        const parent = label.closest('div[data-automation-id], div[class*="input"], li');
        if (parent) {
          const inp = parent.querySelector('input, select, textarea');
          if (inp) return inp;
        }
        const next = label.nextElementSibling || label.parentElement?.querySelector('input, select, textarea');
        if (next) return next;
      }
    }
    return null;
  }

  function findAndClickByText(texts, contextLabel) {
    const all = Array.from(document.querySelectorAll('button, a, span[role="button"], div[role="button"], [data-automation-id="promptOption"], [data-automation-id="compositeHeader"], label'));
    for (const el of all) {
      const t = (el.textContent || '').trim();
      if (texts.some(x => t.toLowerCase().includes(x.toLowerCase()))) {
        if (el.offsetParent !== null) {
          log(`[${contextLabel}] findAndClickByText: clic sur élément texte="${t}"`);
          el.click();
          return true;
        }
      }
    }
    const reuseLink = document.querySelector('a[href*="reuse"], [data-automation-id*="reuse"], [data-automation-id*="lastApplication"]');
    if (reuseLink?.offsetParent !== null) {
      log(`[${contextLabel}] findAndClickByText: clic sur lien de réutilisation (reuse/lastApplication)`);
      reuseLink.click();
      return true;
    }
    log(`[${contextLabel}] findAndClickByText: aucun élément trouvé pour textes="${texts.join(', ')}"`);
    return false;
  }

  function findSelectByLabel(labelTexts) {
    const inp = findLabelAndInput(labelTexts);
    return inp && inp.tagName === 'SELECT' ? inp : null;
  }

  function findInputByLabel(labelTexts) {
    const inp = findLabelAndInput(labelTexts);
    return inp && (inp.tagName === 'INPUT' || inp.tagName === 'TEXTAREA') ? inp : null;
  }

  /** Workday : cliquer une option de liste (menuItem / promptOption) dont le libellé correspond */
  function clickWorkdayOptionByLabelAndValue(labelKeywords, valueText) {
    const labels = Array.from(document.querySelectorAll('label, [data-automation-id="label"], span[role="presentation"]'));
    for (const label of labels) {
      const text = (label.textContent || '').trim().toLowerCase();
      if (!labelKeywords.some(k => text.includes(k.toLowerCase()))) continue;
      const container = label.closest('li, div[data-automation-id], section, [role="listbox"]') || document.body;
      const options = container.querySelectorAll('[data-automation-id="promptOption"], [data-automation-id="menuItem"], [role="option"]');
      const target = (valueText || '').trim().toLowerCase();
      for (const opt of options) {
        const t = (opt.textContent || opt.getAttribute('aria-label') || '').trim().toLowerCase();
        if (t === target || t.includes(target) || (target === 'oui' && /^oui$/i.test(t)) || (target === 'non' && /^non$/i.test(t))) {
          if (opt.offsetParent !== null) {
            opt.click();
            return true;
          }
        }
      }
    }
    return false;
  }

  /**
   * Remplir l'étape 2 Workday "Mon expérience" (Études) depuis Firebase.
   * - Établissement : profile.establishment (Firebase) ; "Autre" uniquement si vide → option "Autre établissement".
   * - Diplôme : mapping education_level → option listbox (ex. Bac+5 → Master 2 / Master Spé...).
   * - Domaine d'études : non obligatoire (*) → on ne remplit que si une valeur existe en profil (sinon on laisse vide).
   * - Année de fin : profile.diploma_year (Firebase graduation_year, ex. 2018) ; année de début : diploma_year - 4.
   */
  function fillWorkdayStep2Education(profile) {
    const establishmentVal = (profile.establishment || '').trim() || 'Autre';
    const diplomaYearRaw = profile.diploma_year != null ? profile.diploma_year : '';
    const diplomaYear = String(diplomaYearRaw).replace(/\D/g, '');
    const yearEnd = diplomaYear.length === 4 ? parseInt(diplomaYear, 10) : null;
    const yearStart = yearEnd != null && yearEnd >= 4 ? yearEnd - 4 : null;

    const establishmentInput = findInputByLabel(['établissement ou université', 'institution']);
    if (establishmentInput && establishmentInput.offsetParent !== null) {
      scrollIntoViewIfNeeded(establishmentInput);
      establishmentInput.focus();
      establishmentInput.click();
      fillInput(establishmentInput, establishmentVal);
      setTimeout(function() {
        const enterEv = new KeyboardEvent('keydown', { key: 'Enter', code: 'Enter', keyCode: 13, which: 13, bubbles: true });
        establishmentInput.dispatchEvent(enterEv);
        if (establishmentVal === 'Autre') {
          setTimeout(function() {
            const opts = document.querySelectorAll('[role="option"]');
            for (const o of opts) {
              const t = (o.textContent || o.getAttribute('aria-label') || '').trim();
              if (/autre établissement/i.test(t) && o.offsetParent !== null) {
                o.click();
                log('   ✏️  Établissement : Sélectionné "Autre établissement" (Firebase vide)', 5);
                break;
              }
            }
          }, 450);
        } else {
          log('   ✏️  Établissement : ' + establishmentVal + ' + Entrée (Firebase)', 5);
        }
      }, 400);
    }

    const diplomaMap = {
      'bac + 5': 'Master 2 / Master Spé / DSCG (BAC+5)',
      'bac+5': 'Master 2 / Master Spé / DSCG (BAC+5)',
      'm2': 'Master 2 / Master Spé / DSCG (BAC+5)',
      'bac + 3': 'Licence / Bachelor / BUT (BAC+3)',
      'bac+3': 'Licence / Bachelor / BUT (BAC+3)',
      'licence': 'Licence / Bachelor / BUT (BAC+3)',
      'bac': 'BAC+1 / Baccalauréat / infra-BAC'
    };
    const eduLevel = (profile.education_level || '').trim().toLowerCase();
    let diplomaOption = 'Master 2 / Master Spé / DSCG (BAC+5)';
    for (const [k, v] of Object.entries(diplomaMap)) {
      if (eduLevel.includes(k)) {
        diplomaOption = v;
        break;
      }
    }
    const diplomaBtn = document.querySelector('button[aria-haspopup="listbox"][aria-label*="Diplôme"], button[aria-label*="Diplôme"]') ||
      Array.from(document.querySelectorAll('button')).find(b => /diplôme.*sélectionnez|diplôme.*obligatoire/i.test((b.getAttribute('aria-label') || '') + (b.textContent || '')));
    if (diplomaBtn && diplomaBtn.offsetParent !== null) {
      scrollIntoViewIfNeeded(diplomaBtn);
      if (clickWorkdayListboxOption(diplomaBtn, diplomaOption, 'Diplôme')) {
        log('   ✏️  Diplôme : ' + diplomaOption + ' (Firebase education_level)', 5);
      }
    }

    const domainVal = (profile.study_domain || profile.field_of_study || '').trim();
    const domainInput = findInputByLabel(["domaine d'études", 'domaine d\'études', 'field of study']);
    if (domainInput && domainInput.offsetParent !== null && domainVal) {
      scrollIntoViewIfNeeded(domainInput);
      domainInput.focus();
      domainInput.click();
      fillInput(domainInput, domainVal);
      setTimeout(function() {
        domainInput.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', code: 'Enter', keyCode: 13, which: 13, bubbles: true }));
        log('   ✏️  Domaine d\'études : ' + domainVal + ' (Firebase, optionnel)', 5);
      }, 350);
    }

    const yearInputs = document.querySelectorAll('input[type="number"][aria-label*="Year"], input[aria-label="Year"], [role="spinbutton"][name="Year"], input[name*="year"]');
    const spinbuttons = Array.from(document.querySelectorAll('[role="spinbutton"]')).filter(s => /year|année/i.test(s.getAttribute('aria-label') || s.getAttribute('name') || ''));
    const yearFields = spinbuttons.length >= 2 ? spinbuttons : Array.from(document.querySelectorAll('input[type="number"]')).filter(i => (i.getAttribute('placeholder') || '').match(/^\d{4}$/) || (i.getAttribute('aria-label') || '').toLowerCase().includes('year'));
    if (yearEnd != null && yearFields.length >= 2) {
      try {
        const label0 = (yearFields[0].getAttribute('aria-label') || yearFields[0].getAttribute('name') || '').toLowerCase();
        const label1 = (yearFields[1].getAttribute('aria-label') || yearFields[1].getAttribute('name') || '').toLowerCase();
        const isEndFirst = /fin|end|to|obtention/.test(label0) || /année de fin|year end/.test(label0);
        const isStartFirst = /début|start|from/.test(label0) || /année de début|year start/.test(label0);
        const startField = (isEndFirst ? yearFields[1] : yearFields[0]);
        const endField = (isEndFirst ? yearFields[0] : yearFields[1]);
        if (yearStart != null) {
          startField.focus();
          fillInput(startField, String(yearStart));
        }
        endField.focus();
        fillInput(endField, String(yearEnd));
        log('   ✏️  Années études : De ' + (yearStart != null ? yearStart : '?') + ' À ' + yearEnd + ' (Firebase graduation_year, aucune modification)', 5);
      } catch (e) {
        log('   ⏭️  Années : ' + (e && e.message), 5);
      }
    } else if (yearEnd != null && yearFields.length === 1) {
      try {
        fillInput(yearFields[0], String(yearEnd));
        log('   ✏️  Année de fin : ' + yearEnd + ' (Firebase graduation_year)', 5);
      } catch (_) {}
    }
  }

  async function notifyOfferUnavailable(jobId, jobTitle) {
    try {
      const { taleos_pending_tab } = await chrome.storage.local.get('taleos_pending_tab');
      let taleosTab = taleos_pending_tab;
      if (!taleosTab) {
        const tabs = await chrome.tabs.query({ url: ['*://*.taleos.co/*', '*://*.github.io/*', 'http://localhost/*'] });
        taleosTab = tabs[0]?.id;
      }
      if (taleosTab) {
        chrome.tabs.sendMessage(taleosTab, { action: 'taleos_offer_unavailable', jobId, jobTitle }).catch(() => {});
      }
      chrome.storage.local.remove('taleos_pending_deloitte');
      hideBanner();
      setTimeout(() => { try { chrome.tabs.remove(chrome.tabs.TAB_ID_NONE); } catch(_){} }, 4000);
    } catch (_) {}
  }

  async function runAutomation() {
    const { taleos_pending_deloitte, taleos_deloitte_did_login_click } = await chrome.storage.local.get(['taleos_pending_deloitte', 'taleos_deloitte_did_login_click']);
    if (!taleos_pending_deloitte) {
      log('Pending absent → skip', 0);
      return;
    }

    const age = Date.now() - (taleos_pending_deloitte.timestamp || 0);
    if (age > MAX_PENDING_AGE) {
      // Revivifier une fois : si on a un profil (ex. onglet ouvert via window.open, ancien pending en storage), mettre à jour le timestamp
      if (taleos_pending_deloitte.profile && (taleos_pending_deloitte.profile.auth_email || taleos_pending_deloitte.profile.email)) {
        log('Pending expiré → revivification du timestamp (une fois)', 0);
        chrome.storage.local.set({
          taleos_pending_deloitte: { ...taleos_pending_deloitte, timestamp: Date.now() }
        });
        setTimeout(runAutomation, 800);
        return;
      }
      log('Pending expiré (>10 min) et pas de profil → skip', 0);
      chrome.storage.local.remove(['taleos_pending_deloitte', 'taleos_deloitte_did_login_click']);
      return;
    }

    window.__taleosDeloitteDidLoginClick = !!taleos_deloitte_did_login_click;
    const url = window.location.href;
    log('URL: ' + url.replace(/^https?:\/\/[^/]+/, ''), 0);

    const { profile, tabId } = taleos_pending_deloitte;
    const jobId = taleos_pending_deloitte.jobId || '';
    const jobTitle = taleos_pending_deloitte.jobTitle || '';
    const email = profile?.auth_email || profile?.email || '';
    const password = profile?.auth_password || '';

    // Détection "Offre introuvable"
    const pageText = (document.body?.innerText || '').toLowerCase();
    if (pageText.includes('offre introuvable') || pageText.includes('job not found') || pageText.includes('this position is no longer available') || pageText.includes('cette offre est peut-être expirée')) {
      log('Offre introuvable → notification Taleos', 0);
      await notifyOfferUnavailable(jobId, jobTitle);
      return;
    }

    if (!email || !password) {
      log('Identifiants manquants → arrêt', 0);
      chrome.storage.local.remove('taleos_pending_deloitte');
      return;
    }

    showBanner();

    // Sur deloitte.com : d'abord essayer un lien direct Workday, sinon chercher le bouton Postuler
    if (url.includes('deloitte.com') && !url.includes('myworkdayjobs.com')) {
      const workdayLink = document.querySelector('a[href*="myworkdayjobs.com"][href*="apply"], a[href*="myworkdayjobs.com"]');
      if (workdayLink?.href) {
        log('Lien Workday trouvé → redirection', 1);
        window.location.href = workdayLink.href;
        return;
      }
    }

    // Étape 1 : Page offre sans /apply → cliquer Postuler (plusieurs sélecteurs, chargement dynamique possible)
    if (!url.includes('/apply') && !url.includes('/apply/')) {
      const bySelector = document.querySelector('a.deloitte-green-button.deloitte-banner-apply-button, a[href*="/apply"], a[href*="myworkdayjobs.com"]');
      const byText = Array.from(document.querySelectorAll('a, button, [role="button"]')).find(el => {
        const t = (el.textContent || '').trim();
        const isPostuler = /^postuler(\s|$)/i.test(t) || (t.toLowerCase().includes('postuler') && t.length < 60);
        return isPostuler && el.offsetParent !== null;
      });
      const btn = bySelector || byText;
      if (btn && btn.offsetParent !== null) {
        log('Clic sur Postuler', 1);
        try { btn.click(); } catch (e) { log('Erreur clic Postuler: ' + e.message, 1); }
        setTimeout(runAutomation, 2000);
        return;
      }
      if (url.includes('deloitte.com')) {
        log('Bouton Postuler non trouvé → retry dans 2s', 1);
        maybeRetryForPostuler();
        return;
      }
    }

    // Étape 2 / 3 : Connexion d'abord (bouton ou formulaire), JAMAIS "Utiliser ma dernière candidature" avant
    const emailInput = document.querySelector('input[data-automation-id="email"]');
    const passwordInput = document.querySelector('input[data-automation-id="password"]');

    // 2a-bis. Si on est sur le formulaire de CRÉATION de compte (pas connexion), cliquer d'abord sur "Connexion" pour afficher le formulaire de connexion
    const isCreationForm = document.querySelector('input[data-automation-id="confirmPassword"], input[name*="confirmPassword"], input[aria-label*="Confirmer"], input[aria-label*="Confirm "]') ||
      Array.from(document.querySelectorAll('button, [role="button"]')).some(el => /^créer un compte$/i.test((el.textContent || el.getAttribute('aria-label') || '').trim()));
    if (isCreationForm) {
      const connexionBtnToShow = document.querySelector('button[aria-label="Connexion"], [data-automation-id="click_filter"][aria-label="Connexion"], [role="button"][aria-label="Connexion"]') ||
        Array.from(document.querySelectorAll('button, [role="button"]')).find(el => /^connexion$/i.test((el.textContent || el.getAttribute('aria-label') || '').trim()));
      if (connexionBtnToShow && connexionBtnToShow.offsetParent !== null) {
        log('Formulaire création de compte détecté → clic sur Connexion pour afficher le formulaire de connexion', 2);
        try { connexionBtnToShow.click(); } catch (e) { log('Erreur clic Connexion: ' + e.message, 2); }
        setTimeout(runAutomation, 2000);
        return;
      }
    }

    // 2a. Si le formulaire CONNEXION est visible (sans champ "Confirmer mot de passe") → on le remplit et on envoie
    if (emailInput && passwordInput && !isCreationForm) {
      log('Formulaire connexion visible → remplissage email/mot de passe', 2);
      fillInput(emailInput, email);
      fillInput(passwordInput, password);

      const submitBtn = document.querySelector('[data-automation-id="click_filter"][aria-label="Connexion"], [aria-label="Connexion"][role="button"], button[data-automation-id="signInSubmitButton"]');
      if (submitBtn && submitBtn.offsetParent !== null) {
        log('Clic sur Connexion (soumission formulaire)', 2);
        chrome.storage.local.set({ taleos_deloitte_did_login_click: true });
        window.__taleosDeloitteDidLoginClick = true;
        try { submitBtn.click(); } catch (e) { log('Erreur clic submit: ' + e.message, 2); }
        setTimeout(runAutomation, 3000);
        return;
      }
    } else {
      // 2b. Sinon, on cherche le bouton / span "Connexion" pour afficher le formulaire
      const connexionSpan = Array.from(document.querySelectorAll('span')).find(s => /^connexion$/i.test((s.textContent || '').trim()));
      const connexionBtn = document.querySelector('[aria-label="Connexion"][role="button"], [data-automation-id="click_filter"][aria-label="Connexion"]');
      const btn = connexionSpan || connexionBtn;
      if (btn && btn.offsetParent !== null) {
        log('Clic sur bouton Connexion (affichage formulaire)', 2);
        chrome.storage.local.set({ taleos_deloitte_did_login_click: true });
        window.__taleosDeloitteDidLoginClick = true;
        try { btn.click(); } catch (e) { log('Erreur clic Connexion: ' + e.message, 2); }
        setTimeout(runAutomation, 2000);
        return;
      }
      log('Aucun bouton Connexion visible', 2);
    }

    // Étape 4 : "Utiliser ma dernière candidature" — seulement sur /apply (pas sur /apply/useMyLastApplication)
    if (!url.includes('useMyLastApplication')) {
      const hasConnexionUi = document.querySelector('input[data-automation-id="email"]') ||
        document.querySelector('input[data-automation-id="password"]') ||
        document.querySelector('[aria-label="Connexion"][role="button"], [data-automation-id="click_filter"][aria-label="Connexion"]') ||
        Array.from(document.querySelectorAll('span')).some(s => /^connexion$/i.test((s.textContent || '').trim()));

      const useLastAppBtn = document.querySelector('[data-automation-id="useMyLastApplication"]') ||
        document.querySelector('a[href*="useMyLastApplication"]') ||
        document.querySelector('a[role="button"][href*="useMyLastApplication"]');

      const didLogin = !!window.__taleosDeloitteDidLoginClick;
      log(`Connexion visible=${!!hasConnexionUi}, bouton "Utiliser ma dernière candidature"=${!!useLastAppBtn}, flag=${didLogin}`, 4);

      if (!hasConnexionUi && useLastAppBtn) {
        log('Clic sur "Utiliser ma dernière candidature"', 4);
        try {
          useLastAppBtn.scrollIntoView({ behavior: 'instant', block: 'center' });
        } catch (e) {}
        try {
          useLastAppBtn.click();
        } catch (e) {
          log('Erreur clic useMyLastApplication: ' + e.message, 4);
        }
        setTimeout(runAutomation, 2500);
        return;
      }

      if (url.includes('/apply') && !hasConnexionUi && !useLastAppBtn) {
        // Le formulaire peut déjà être visible (navigation en cours ou URL pas encore mise à jour) → passer au remplissage
        const formAlreadyVisible = document.querySelector('input[id="source--source"], input[name="legalName--firstName"], input[name="legalName--lastName"]') ||
          document.querySelector('input[name="candidateIsPreviousWorker"]') ||
          document.querySelector('button[name="legalName--title"]') ||
          document.querySelector('[data-automation-id="searchBox"][id="source--source"]');
        if (formAlreadyVisible && formAlreadyVisible.offsetParent !== null) {
          log('Formulaire déjà visible (bouton absent) → remplissage formulaire', 4);
        } else if (didLogin) {
          // On a déjà cliqué Connexion / "Utiliser ma dernière candidature" → la page est peut-être en train de naviguer, ne pas boucler sur 8 retries
          log('Navigation probable après clic → réessai dans 2s (pas de retry boucle)', 4);
          setTimeout(runAutomation, 2000);
          return;
        } else {
          log('Attente bouton "Utiliser ma dernière candidature" → retry', 4);
          maybeRetryForUseLastApp();
          return;
        }
      }
    } else {
      log('Déjà sur useMyLastApplication → remplissage formulaire', 4);
    }

    // Détection étape 2 "Mon expérience" (Études) : remplir établissement, diplôme, domaine, années depuis Firebase
    const step2EstablishmentInput = findInputByLabel(['établissement ou université', 'institution']);
    const step2DiplomaBtn = Array.from(document.querySelectorAll('button[aria-haspopup="listbox"], button[role="combobox"]')).find(b => {
      const a = (b.getAttribute('aria-label') || b.textContent || '').toLowerCase();
      return a.includes('diplôme') && (a.includes('sélectionnez') || a.includes('select'));
    });
    const isStep2Form = (step2EstablishmentInput && step2EstablishmentInput.offsetParent !== null) ||
      (step2DiplomaBtn && step2DiplomaBtn.offsetParent !== null);
    if (isStep2Form && url.includes('useMyLastApplication')) {
      log('📂 [STEP 5b] Formulaire étape 2 "Mon expérience" détecté → remplissage Études depuis Firebase', 5);
      fillWorkdayStep2Education(profile);
      setTimeout(refreshWorkdayRequiredFields, 600);
      setTimeout(hideBanner, 1500);
      return;
    }

    // Étape 5 : Remplir le formulaire de candidature (profil Firebase) — étape 1 "Mes données personnelles"
    // Mapping Workday (étape 1 "Mes données personnelles") :
    // - Comment nous avez-vous connus : input searchBox id="source--source" → remplir "Site Deloitte Careers" puis Enter
    // - Déjà travaillé Deloitte : radios Oui/Non (value true/false), souvent opacity 0 → cliquer le label associé
    // - Titre : bouton listbox name="legalName--title" → options "Monsieur" / "Madame"
    // - Prénom/Nom : id="name--legalName--firstName" / "name--legalName--lastName"
    // - Adresse : input par label "Nature et nom de la voie", Ville, Code postal
    // - Type téléphone : bouton listbox phoneNumber--phoneType → "Mobile Personnel" / "Fixe Personnel"
    // - Indicatif pays : combobox "Rechercher" → "France (+33)", "Royaume-Uni (+44)", etc.
    // - Numéro téléphone : id="phoneNumber--phoneNumber"
    let filled = false;

    log('📂 [STEP 5] Données Firebase utilisées pour le formulaire:', 5);
    log(
      '   civility: ' + (profile.civility != null ? profile.civility : '(vide)') +
      ' | firstname: ' + (profile.firstname != null ? profile.firstname : '(vide)') +
      ' | lastname: ' + (profile.lastname != null ? profile.lastname : '(vide)') +
      ' | address: ' + (profile.address != null ? profile.address : '(vide)') +
      ' | city: ' + (profile.city != null ? profile.city : '(vide)') +
      ' | zipcode: ' + (profile.zipcode != null ? profile.zipcode : '(vide)'),
      5
    );
    var rawWorked = (profile.deloitte_worked != null ? profile.deloitte_worked : profile.deloitteWorked);
    log(
      '   phone_country_code: ' + (profile.phone_country_code != null ? profile.phone_country_code : '(vide)') +
      ' | phone: ' + (profile.phone_number || profile['phone-number'] || profile.phone || '(vide)') +
      ' | deloitte_worked: ' + (rawWorked != null ? rawWorked : '(vide)'),
      5
    );

    // ——— Comment nous avez-vous connus? → "Site Deloitte Careers" ———
    // Workday : champ recherche (placeholder "Rechercher"). Remplir le texte puis Enter valide la sélection.
    log('   🔵 Comment nous avez-vous connus? → cible "Site Deloitte Careers" (Firebase)', 5);
    let hearAboutFilled = false;
    const hearSearchBox = document.querySelector('input[data-automation-id="searchBox"][id="source--source"]') ||
      findInputByLabel(['comment nous avez-vous connus', 'how did you hear about us']);
    if (hearSearchBox && hearSearchBox.offsetParent !== null) {
      scrollIntoViewIfNeeded(hearSearchBox);
      try {
        hearSearchBox.focus();
        hearSearchBox.click();
      } catch (e) {}
      fillInput(hearSearchBox, SITE_DELOITTE_CAREERS);
      const opt = document.querySelector('[data-automation-id="promptOption"][data-automation-label="Site Deloitte Careers"]');
      if (opt && opt.offsetParent !== null) {
        opt.click();
        hearAboutFilled = true;
        filled = true;
        log('   ✏️  Comment nous avez-vous connus? : Sélectionné "Site Deloitte Careers" (option) (Firebase)', 5);
      }
      if (!hearAboutFilled) {
        setTimeout(function() {
          try {
            const enterEv = new KeyboardEvent('keydown', { key: 'Enter', code: 'Enter', keyCode: 13, which: 13, bubbles: true });
            (document.activeElement || hearSearchBox).dispatchEvent(enterEv);
            log('   ✏️  Comment nous avez-vous connus? : Rempli + Entrée (Firebase)', 5);
          } catch (_) {}
        }, 350);
        hearAboutFilled = true;
        filled = true;
      }
    }
    if (!hearAboutFilled) {
      const hearAboutSelect = findSelectByLabel(['comment nous avez-vous connus', 'how did you hear about us']);
      if (hearAboutSelect) {
        fillSelect(hearAboutSelect, SITE_DELOITTE_CAREERS);
        hearAboutFilled = true;
        filled = true;
        log('   ✏️  Comment nous avez-vous connus? : Sélectionné "Site Deloitte Careers" via select (Firebase)', 5);
      }
      const hearAboutInput = findInputByLabel(['comment nous avez-vous connus', 'how did you hear about us']);
      if (hearAboutInput) {
        fillInput(hearAboutInput, SITE_DELOITTE_CAREERS);
        hearAboutFilled = true;
        filled = true;
        log('   ✏️  Comment nous avez-vous connus? : Rempli "Site Deloitte Careers" via input (Firebase)', 5);
      }
    }
    if (!hearAboutFilled && clickWorkdayOptionByLabelAndValue(['comment nous avez-vous connus', 'how did you hear about us'], SITE_DELOITTE_CAREERS)) {
      filled = true;
      log('   ✏️  Comment nous avez-vous connus? : Sélectionné via option Workday "Site Deloitte Careers" (Firebase)', 5);
      setTimeout(function() {
        try {
          const enterEv = new KeyboardEvent('keydown', { key: 'Enter', code: 'Enter', keyCode: 13, which: 13, bubbles: true });
          (document.activeElement || document.body).dispatchEvent(enterEv);
        } catch (_) {}
      }, 250);
    }
    if (!hearAboutFilled) {
      log('   ⏭️  Comment nous avez-vous connus? : champ non trouvé (retry possible)', 5);
    }

    // ——— Avez-vous déjà travaillé pour Deloitte? (Firebase: deloitte_worked / deloitteWorked) ———
    const workedRaw = profile.deloitte_worked || profile.deloitteWorked || 'no';
    const workedYesNo = workedRaw === 'yes' ? 'Oui' : 'Non';
    log('   🔵 Avez-vous déjà travaillé pour Deloitte? → Firebase: ' + workedRaw + ' → ' + workedYesNo, 5);
    const workedSelect = findSelectByLabel(['avez-vous déjà travaillé pour deloitte', 'have you worked for deloitte']);
    if (workedSelect) {
      fillSelect(workedSelect, workedYesNo);
      filled = true;
    }
    const workedRadioValues = workedRaw === 'yes' ? ['yes', '1', 'oui', 'true'] : ['no', '0', 'non', 'false'];
    const workedRadios = document.querySelectorAll('input[type="radio"][name*="worked"], input[type="radio"][name*="deloitte"], input[type="radio"][name*="previous"], input[name="candidateIsPreviousWorker"]');
    for (const r of workedRadios) {
      const v = (r.value || '').toLowerCase();
      if (workedRadioValues.some(x => v === x || v.includes(x))) {
        if (!r.checked) {
          r.click();
          log('   ✏️  Avez-vous déjà travaillé : Coché radio value="' + r.value + '" (Firebase)', 5);
          filled = true;
        } else {
          log('   ✅ Avez-vous déjà travaillé : Déjà coché (value=' + r.value + ') → Skip', 5);
        }
        break;
      }
    }
    if (!filled) {
      const radioYes = document.querySelector('input[name="candidateIsPreviousWorker"][type="radio"][value="true"]') || document.querySelector('input[name="candidateIsPreviousWorker"][type="radio"][value="1"]');
      const radioNo = document.querySelector('input[name="candidateIsPreviousWorker"][type="radio"][value="false"]') || document.querySelector('input[name="candidateIsPreviousWorker"][type="radio"][value="0"]');
      const radio = workedRaw === 'yes' ? radioYes : radioNo;
      if (radio) {
        if (radio.checked) {
          log('   ✅ Avez-vous déjà travaillé : Déjà coché (candidateIsPreviousWorker) → Skip', 5);
        } else {
          const style = typeof getComputedStyle !== 'undefined' ? getComputedStyle(radio) : null;
          const hidden = !radio.offsetParent || (style && (parseFloat(style.opacity) === 0 || style.visibility === 'hidden'));
          if (hidden) {
            const labelToClick = (radio.id && document.querySelector('label[for="' + radio.id + '"]')) || radio.closest('label') ||
              Array.from(document.querySelectorAll('label, span[role="presentation"], [data-automation-id="label"]')).find(el => /^(oui|non)$/i.test((el.textContent || '').trim()) && el.closest('div, li')?.querySelector('input[name="candidateIsPreviousWorker"]') === radio);
            if (labelToClick && labelToClick.offsetParent !== null) {
              scrollIntoViewIfNeeded(labelToClick);
              labelToClick.click();
              log('   ✏️  Avez-vous déjà travaillé : Coché via label (radio masqué) value="' + radio.value + '" (Firebase)', 5);
              filled = true;
            } else {
              radio.click();
              log('   ✏️  Avez-vous déjà travaillé : Coché candidateIsPreviousWorker value="' + radio.value + '" (Firebase)', 5);
              filled = true;
            }
          } else {
            radio.click();
            log('   ✏️  Avez-vous déjà travaillé : Coché candidateIsPreviousWorker value="' + radio.value + '" (Firebase)', 5);
            filled = true;
          }
        }
      } else {
        log('   ⏭️  Avez-vous déjà travaillé : aucun radio candidateIsPreviousWorker trouvé → Skip', 5);
      }
    }
    if (!filled && clickWorkdayOptionByLabelAndValue(['avez-vous déjà travaillé pour deloitte', 'have you worked for deloitte'], workedYesNo)) {
      filled = true;
    }

    // Si oui : ancien bureau, email, pays
    if (workedRaw === 'yes') {
      const oldOffice = findInputByLabel(['votre ancien bureau', 'your previous office', 'ancien bureau']);
      if (oldOffice && profile.deloitte_old_office) {
        if (fillInputIfNeeded(oldOffice, profile.deloitte_old_office, 'Ancien bureau')) filled = true;
      }
      const oldEmail = findInputByLabel(['votre ancienne adresse email', 'your previous email', 'ancienne adresse email']);
      if (oldEmail && profile.deloitte_old_email) {
        if (fillInputIfNeeded(oldEmail, profile.deloitte_old_email, 'Ancienne adresse email')) filled = true;
      }
      const countryVal = profile.deloitte_country || profile.country || '';
      if (countryVal) {
        const countryInput = findInputByLabel(['pays', 'country']);
        const countrySelect = findSelectByLabel(['pays', 'country']);
        if (countryInput && fillInputIfNeeded(countryInput, countryVal, 'Pays')) filled = true;
        if (countrySelect) {
          fillSelect(countrySelect, countryVal);
          filled = true;
        }
      }
    }

    // ——— Titre (préfixe) : bouton Workday id="name--legalName--title" ou name="legalName--title" → Monsieur / Madame ———
    const titleCivility = (profile.civility || '').trim();
    if (titleCivility) {
      const titleOption = /madame|mme|mrs|female/i.test(titleCivility) ? 'Madame' : 'Monsieur';
      if (clickWorkdayListboxOption('name--legalName--title', titleOption, 'Titre (préfixe)')) filled = true;
    } else {
      log('   ⏭️  Titre (préfixe) : pas de civility Firebase → Skip', 5);
    }

    // ——— Prénom : id="name--legalName--firstName" (ne pas inverser avec nom) ———
    const firstnameEl = document.getElementById('name--legalName--firstName') || document.querySelector('input[name="legalName--firstName"]');
    if (firstnameEl && profile.firstname && fillInputIfNeeded(firstnameEl, profile.firstname, 'Prénom')) filled = true;

    // ——— Nom de famille : id="name--legalName--lastName" ———
    const lastnameEl = document.getElementById('name--legalName--lastName') || document.querySelector('input[name="legalName--lastName"]');
    if (lastnameEl && profile.lastname && fillInputIfNeeded(lastnameEl, profile.lastname, 'Nom de famille')) filled = true;

    // ——— Nature et nom de la voie ———
    const addressEl = document.querySelector('input[id*="address"], input[name*="address"]') || findInputByLabel(['nature et nom de la voie', 'address', 'adresse', 'street']);
    if (addressEl && profile.address && fillInputIfNeeded(addressEl, profile.address, 'Nature et nom de la voie')) filled = true;

    // ——— Ville ———
    const cityEl = findInputByLabel(['ville', 'city']);
    if (cityEl && profile.city && fillInputIfNeeded(cityEl, profile.city, 'Ville')) filled = true;

    // ——— Code postal ———
    const zipEl = findInputByLabel(['code postal', 'postal code', 'zip']);
    if (zipEl && profile.zipcode && fillInputIfNeeded(zipEl, profile.zipcode, 'Code postal')) filled = true;

    // ——— Type d'appareil téléphonique : bouton id="phoneNumber--phoneType" ou name="phoneType" → Mobile Personnel ———
    if (clickWorkdayListboxOption('phoneNumber--phoneType', 'Mobile Personnel', 'Type d\'appareil téléphonique')) filled = true;

    // ——— Indicatif de pays (code téléphone) : basé sur Firebase phone_country_code — +33 = France, +44 = Royaume-Uni ———
    const phoneCountryCode = (profile.phone_country_code || '').trim().replace(/\s/g, '');
    if (phoneCountryCode) {
      const wantLabel = phoneCountryCode === '+44' ? 'Royaume-Uni (+44)' : phoneCountryCode === '+33' ? 'France (+33)' : phoneCountryCode;
      log('   🔵 Indicatif de pays (téléphone) : Firebase phone_country_code=' + phoneCountryCode + ' → ' + wantLabel + ' (pour France mettre +33 dans Firebase)', 5);
      let countryCodeDone = false;
      const allListbox = document.querySelectorAll('button[aria-haspopup="listbox"], [data-automation-id="compositeHeader"], [role="combobox"]');
      for (const b of allListbox) {
        const aria = (b.getAttribute('aria-label') || '').toLowerCase();
        const text = (b.textContent || '').toLowerCase().trim();
        const combined = aria + ' ' + text;
        // Ne pas toucher au champ « Pays » (résidence) : label "Pays France" sans "indicatif" ni indicatif téléphone (+33).
        const isPaysResidence = combined.includes('pays') && !combined.includes('indicatif') && !/\+\d{2,4}/.test(combined);
        const isCountryCodeField = !isPaysResidence && (combined.includes('indicatif') || combined.includes('country code') || combined.includes('dialling') || /\+\d{2,4}/.test(combined));
        if (!isCountryCodeField) continue;
        const currentVal = (b.getAttribute('aria-label') || b.textContent || '').trim();
        if (currentVal.includes(phoneCountryCode) && !/sélectionnez|select a value/i.test(currentVal)) {
          log('   ✅ Indicatif de pays : Déjà ' + currentVal + ' (Firebase identique) → Skip', 5);
          countryCodeDone = true;
          break;
        }
        if (clickWorkdayListboxOption(b, wantLabel, 'Indicatif de pays')) {
          filled = true;
          countryCodeDone = true;
        }
        break;
      }
      if (!countryCodeDone) {
        const indicatifInput = findInputByLabel(['indicatif de pays', 'country code', 'indicatif']);
        if (indicatifInput && indicatifInput.offsetParent !== null && (indicatifInput.placeholder || '').toLowerCase().includes('rechercher')) {
          scrollIntoViewIfNeeded(indicatifInput);
          indicatifInput.focus();
          indicatifInput.click();
          fillInput(indicatifInput, wantLabel);
          countryCodeDone = true;
          filled = true;
          setTimeout(function() {
            const opts = document.querySelectorAll('[role="option"], [data-automation-id="promptOption"]');
            let clickable = null;
            for (const o of opts) {
              const t = (o.getAttribute('aria-label') || o.textContent || '').trim();
              if (t.includes(phoneCountryCode) || (phoneCountryCode === '+44' && t.includes('Royaume-Uni')) || (phoneCountryCode === '+33' && t.includes('France'))) {
                clickable = o.closest('[role="option"]') || o.closest('li') || o;
                break;
              }
            }
            if (clickable && clickable.offsetParent !== null) {
              clickable.click();
              log('   ✏️  Indicatif de pays : Sélectionné ' + wantLabel + ' via champ recherche (Firebase)', 5);
            } else {
              try {
                const enterEv = new KeyboardEvent('keydown', { key: 'Enter', code: 'Enter', keyCode: 13, which: 13, bubbles: true });
                indicatifInput.dispatchEvent(enterEv);
                log('   ✏️  Indicatif de pays : Rempli ' + wantLabel + ' + Entrée (Firebase)', 5);
              } catch (_) {}
            }
          }, 450);
        }
      }
      if (!countryCodeDone) {
        const opt = document.querySelector('[data-automation-label="' + wantLabel + '"], [data-automation-label*="' + phoneCountryCode + '"], [aria-label*="' + phoneCountryCode + '"]');
        if (opt && opt.offsetParent !== null) {
          const listbox = opt.closest('[role="listbox"], ul, [data-automation-id="menuItem"]')?.parentElement || opt.closest('li')?.parentElement;
          const trigger = listbox?.previousElementSibling || listbox?.parentElement?.querySelector('button[aria-haspopup="listbox"], [data-automation-id="compositeHeader"], button');
          const labelIndicatif = Array.from(document.querySelectorAll('label, [data-automation-id="label"], span')).find(el => /indicatif|country code|country dial/i.test((el.textContent || '').trim()));
          const triggerNearLabel = labelIndicatif?.closest('div')?.querySelector('button[aria-haspopup="listbox"], [data-automation-id="compositeHeader"], [role="combobox"]');
          const toClick = trigger || triggerNearLabel || document.querySelector('button[aria-label*="+33"], button[aria-label*="+44"], [data-automation-id="compositeHeader"]');
          if (toClick && toClick.offsetParent !== null) {
            scrollIntoViewIfNeeded(toClick);
            try { toClick.click(); } catch (e) {}
            setTimeout(function() {
              const o = document.querySelector('[data-automation-label="' + wantLabel + '"], [data-automation-label*="' + phoneCountryCode + '"], [aria-label*="' + phoneCountryCode + '"]');
              const clickable = o?.closest('[role="option"]') || o?.closest('li') || o;
              if (clickable && clickable.offsetParent !== null) {
                clickable.click();
                log('   ✏️  Indicatif de pays : Sélectionné ' + phoneCountryCode + ' (Firebase)', 5);
              }
            }, 500);
            filled = true;
            countryCodeDone = true;
          }
        }
        if (!countryCodeDone) log('   ⏭️  Indicatif de pays : trigger ou option non trouvé pour ' + phoneCountryCode + ' → Skip', 5);
      }
    } else {
      log('   ⏭️  Indicatif de pays : pas de phone_country_code Firebase → Skip', 5);
    }

    // ——— Numéro de téléphone : id="phoneNumber--phoneNumber" ou name="phoneNumber" ———
    const phoneVal = (profile.phone_number || profile['phone-number'] || profile.phone || '').trim().replace(/\s/g, '');
    const phoneEl = document.getElementById('phoneNumber--phoneNumber') || document.querySelector('input[name="phoneNumber"][id*="phoneNumber"]') || document.querySelector('input[name="phoneNumber"]');
    if (phoneEl && phoneVal && fillInputIfNeeded(phoneEl, phoneVal, 'Numéro de téléphone')) filled = true;

    // Après remplissage : clic sur chaque champ puis clic ailleurs (comme en manuel) pour que Workday valide.
    setTimeout(refreshWorkdayRequiredFields, 800);
    setTimeout(workdayClickThenClickAway, 1500);

    if (filled) {
      formFillRetryCount = 0;
      // Sur useMyLastApplication : si au moins un champ est rempli ou déjà correct, on considère l'automatisation terminée
      if (url.includes('useMyLastApplication')) {
        log('Champs remplis sur useMyLastApplication → fin automatisation (pending supprimé, bandeau masqué)', 5);
        chrome.storage.local.remove(['taleos_pending_deloitte', 'taleos_deloitte_did_login_click']);
        setTimeout(hideBanner, 2000);
        return;
      }
      // Sur /apply (page d'entrée) on peut relancer une fois pour s'assurer que tout est bien pris en compte
      log('Champs remplis → réessai dans 2s', 5);
      setTimeout(runAutomation, 2000);
      return;
    }

    if (url.includes('/apply') && (emailInput || filled)) {
      log('Formulaire en cours → pending conservé', 5);
      setTimeout(runAutomation, 3000);
      return;
    }

    // Sur useMyLastApplication : le formulaire peut se charger en différé → retry sans enlever le bandeau
    if (url.includes('useMyLastApplication') && formFillRetryCount < MAX_FORM_FILL_RETRIES) {
      formFillRetryCount++;
      log('Formulaire pas encore prêt → retry ' + formFillRetryCount + '/' + MAX_FORM_FILL_RETRIES + ' dans 2s (bandeau conservé)', 5);
      setTimeout(runAutomation, 2000);
      return;
    }

    formFillRetryCount = 0;
    chrome.storage.local.remove(['taleos_pending_deloitte', 'taleos_deloitte_did_login_click']);
    setTimeout(hideBanner, 2000);
  }

  let runCount = 0;
  const MAX_RETRIES = 8;
  let postulerRetryCount = 0;
  const MAX_POSTULER_RETRIES = 6;
  let formFillRetryCount = 0;
  const MAX_FORM_FILL_RETRIES = 12;

  function maybeRetryForPostuler() {
    if (postulerRetryCount >= MAX_POSTULER_RETRIES) return;
    if (!window.location.href.includes('deloitte.com') || window.location.href.includes('myworkdayjobs.com')) return;
    postulerRetryCount++;
    log('Retry ' + postulerRetryCount + '/' + MAX_POSTULER_RETRIES + ' (attente bouton Postuler)', 1);
    setTimeout(runAutomation, 2000);
  }

  function scheduleRun(delay) {
    chrome.storage.local.get('taleos_pending_deloitte').then((s) => {
      if (s.taleos_pending_deloitte) {
        runCount = 0;
        setTimeout(runAutomation, delay || 1500);
      }
    });
  }

  chrome.storage.onChanged.addListener((changes, area) => {
    if (area === 'local' && changes.taleos_pending_deloitte?.newValue) {
      runCount = 0;
      postulerRetryCount = 0;
      formFillRetryCount = 0;
      setTimeout(runAutomation, 1000);
    }
  });

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => scheduleRun(2000));
  } else {
    scheduleRun(1500);
  }

  window.addEventListener('pageshow', function(ev) {
    if (ev.persisted) scheduleRun(2000);
  });

  // Retry si on est sur /apply (URL de base) et que le bouton "Utiliser ma dernière candidature" n'est pas encore chargé
  function maybeRetryForUseLastApp() {
    const href = window.location.href;
    if (!href.includes('/apply') || href.includes('useMyLastApplication')) return;
    if (runCount >= MAX_RETRIES) return;
    runCount++;
    log('Retry ' + runCount + '/' + MAX_RETRIES + ' (attente bouton)', 4);
    setTimeout(runAutomation, 2000);
  }

  /**
   * Collecte toutes les valeurs du champ "Établissement ou université" en testant chaque paire
   * de lettres AA, AB, ... AZ, BA, ... ZZ : pour chaque paire on saisit, on envoie Entrée pour
   * charger les écoles, on récupère les options affichées. Doublons retirés à la fin.
   */
  async function collectDeloitteInstitutions() {
    const input = findInputByLabel(['établissement ou université', 'institution']);
    if (!input || !input.offsetParent) {
      return { list: [], error: 'Champ Établissement non trouvé. Ouvrez l\'étape 2 "Mon expérience" du formulaire Deloitte.' };
    }
    const seen = new Set();
    const collectVisibleOptions = () => {
      const opts = document.querySelectorAll('[role="listbox"] [role="option"], [role="option"]');
      for (const o of opts) {
        const t = (o.textContent || o.getAttribute('aria-label') || '').trim();
        if (t && !/sélectionnez une valeur|select a value/i.test(t)) seen.add(t);
      }
    };
    const letters = 'abcdefghijklmnopqrstuvwxyz';
    const pairs = [];
    for (const a of letters) {
      for (const b of letters) {
        pairs.push(a + b);
      }
    }
    const opts = { key: 'Enter', code: 'Enter', keyCode: 13, which: 13, bubbles: true, cancelable: true };
    const sendEnter = () => {
      const el = document.activeElement || input;
      for (const ev of ['keydown', 'keypress', 'keyup']) {
        el.dispatchEvent(new KeyboardEvent(ev, opts));
      }
    };
    const setValueNoBlur = (el, val) => {
      el.focus();
      const str = String(val == null ? '' : val).trim();
      const nativeSetter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set;
      if (nativeSetter) nativeSetter.call(el, str);
      else el.value = str;
      el.dispatchEvent(new Event('input', { bubbles: true }));
      el.dispatchEvent(new Event('change', { bubbles: true }));
    };
    for (let i = 0; i < pairs.length; i++) {
      const pair = pairs[i];
      input.focus();
      input.click();
      setValueNoBlur(input, '');
      await new Promise(r => setTimeout(r, 150));
      setValueNoBlur(input, pair);
      await new Promise(r => setTimeout(r, 450));
      sendEnter();
      await new Promise(r => setTimeout(r, 750));
      collectVisibleOptions();
    }
    await new Promise(r => setTimeout(r, 200));
    input.focus();
    setValueNoBlur(input, 'Autre');
    await new Promise(r => setTimeout(r, 400));
    sendEnter();
    await new Promise(r => setTimeout(r, 700));
    collectVisibleOptions();
    seen.add('Autre établissement');
    const list = Array.from(seen).sort((a, b) => a.localeCompare(b, 'fr'));
    return { list };
  }

  chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
    if (msg.action !== 'collect_deloitte_institutions') return false;
    collectDeloitteInstitutions().then(sendResponse).catch(err => sendResponse({ list: [], error: (err && err.message) || 'Erreur' }));
    return true;
  });
})();
