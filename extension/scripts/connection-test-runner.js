/**
 * Taleos - Script de test de connexion bancaire (injecté dans l'onglet)
 * Gère le remplissage, la soumission et la détection du résultat pour CA, BNP, SG, Deloitte, Bpifrance, AXA, Allianz.
 * JP Morgan est géré côté background car aucune authentification par mot de passe n'est requise ici.
 */
(function() {
  'use strict';

  const CONFIG = {
    credit_agricole: {
      loginUrl: 'https://groupecreditagricole.jobs/fr/connexion/',
      emailSel: '#form-login-email',
      passwordSel: '#form-login-password',
      submitSel: '#form-login-submit',
      cookieSel: 'button.rgpd-btn-refuse',
      successCheck: (url, content) => !url.includes('connexion'),
      failureCheck: (url, content) => url.includes('connexion')
    },
    bnp_paribas: {
      loginUrl: 'https://bwelcome.hr.bnpparibas/fr_FR/externalcareers/Login',
      emailSel: '#tpt_loginUsername, input[name="username"]',
      passwordSel: '#tpt_loginPassword, input[name="password"]',
      submitSel: 'button[type="submit"][name="Connexion"], form[action*="/Login"] button[type="submit"]',
      cookieSel: null,
      successCheck: (url, content) => /\/profile\b/.test(url) || /mon profil/.test(content),
      failureCheck: (url, content) => /il se peut que le nom d'utilisateur ou le mot de passe soit incorrect, ou que l'accès soit limité\./.test(content)
    },
    societe_generale: {
      loginUrl: 'https://socgen.taleo.net/careersection/iam/accessmanagement/login.jsf?lang=fr-FR&redirectionURI=https%3A%2F%2Fsocgen.taleo.net%2Fcareersection%2Fsgcareers%2Fprofile.ftl%3Flang%3Dfr-FR%26src%3DCWS-1%26pcid%3Dmjlsx8hz6i4vn92z&TARGET=https%3A%2F%2Fsocgen.taleo.net%2Fcareersection%2Fsgcareers%2Fprofile.ftl%3Flang%3Dfr-FR%26src%3DCWS-1%26pcid%3Dmjlsx8hz6i4vn92z',
      emailSel: '#dialogTemplate-dialogForm-login-name1, input[id*="login-name"]',
      passwordSel: '#dialogTemplate-dialogForm-login-password',
      submitSel: '#dialogTemplate-dialogForm-login-defaultCmd',
      cookieSel: '#didomi-notice-disagree-button',
      successCheck: (url, content) => /profile|infojob|profil général/i.test(url + content),
      failureCheck: (url, content) => /errorMessageTitle/i.test(content)
    },
    deloitte: {
      loginUrl: 'https://fina.wd103.myworkdayjobs.com/fr-FR/DeloitteRecrute',
      emailSel: 'input[data-automation-id="email"]',
      passwordSel: 'input[data-automation-id="password"]',
      submitSel: '[data-automation-id="click_filter"][aria-label="Connexion"], [aria-label="Connexion"][role="button"], button[data-automation-id="signInSubmitButton"]',
      cookieSel: null,
      successCheck: (url, content) => {
        const html = (document.body?.innerHTML || '').toLowerCase();
        return /\/home\b|accueil candidat|navigationItem-Accueil/i.test(url + content) ||
          !!document.querySelector('[data-automation-id="navigationItem-Accueil candidat"]');
      },
      failureCheck: (url, content) => /errorMessage|data-automation-id="errorMessage"/i.test(content)
    },
    bpifrance: {
      loginUrl: 'https://bpi.tzportal.io//fr/login',
      emailSel: '#email, input[type="text"][name="email"], input[placeholder="Email"]',
      passwordSel: '#password, input[type="password"][name="password"], input[placeholder="Password"]',
      submitSel: 'a.btn.btn-primary, button[type="submit"], button',
      cookieSel: null,
      successCheck: (url, content) => /\/fr\/myaccount\b|se déconnecter|mon profil/i.test(url + content),
      failureCheck: (url, content) => /mot de passe incorrect|identifiants incorrects|se connecter/i.test(content)
    },
    axa: {
      loginUrl: 'https://careers.axa.com/careers-home/auth/1/verify-login-type',
      emailSel: 'input[formcontrolname="email"], input[type="email"], input[name*="email" i], input[id*="email" i], input[name*="username" i], input[id*="username" i]',
      passwordSel: 'input[formcontrolname="password"], input[type="password"], input[name*="password" i], input[id*="password" i]',
      submitSel: 'button[type="submit"], input[type="submit"], button',
      cookieSel: '#onetrust-reject-all-handler, .onetrust-close-btn-handler.banner-close-button, #onetrust-accept-btn-handler',
      successCheck: (url, content) => {
        const html = (document.body?.innerHTML || '').toLowerCase();
        const hasErrorAlert = !!document.querySelector('[data-component="login-error-alert"], .alert--error, [role="alert"]');
        const trustDeviceText = /se connecter plus rapidement sur cet appareil/i.test(document.body?.innerText || '');
        const hasLoginForm = !!document.querySelector('input[type="password"], input[type="email"], input[name*="email" i], input[name*="username" i]');
        return !hasErrorAlert && (
          trustDeviceText ||
          /logout|déconnexion|my applications|candidate home|candidate profile|job alerts|account settings/i.test(content) ||
          /logout|candidate-home|myprofile|my-applications|account-settings/i.test(html) ||
          (!/verify-login-type|login/i.test(url) && !hasLoginForm)
        );
      },
      failureCheck: (url, content) => {
        const errorAlert = document.querySelector('[data-component="login-error-alert"], .alert--error, [role="alert"]');
        const errorText = (errorAlert?.textContent || '').trim();
        return !!errorText ||
          /nom d'utilisateur ou mot de passe incorrect/i.test(content) ||
          /incorrect|invalide|erreur|invalid|failed/i.test(content);
      }
    },
    allianz: {
      loginUrl: 'https://career5.successfactors.eu/career?company=AZGROUPPROD&site=&lang=en_GB&login_ns=login&loginFlowRequired=true&showLogOutMsg=true&brandUrl=&_s.crb=vGbBbLMSiPxDaedOIn8tTt8WApNMjWQcgDbELe1OyzA%253d',
      emailSel: '#username',
      passwordSel: '#password',
      submitSel: 'button[onclick*="validateFields"], .aquabtn.active button, .button_row button',
      cookieSel: null,
      successCheck: (url, content) => {
        const text = (content || '').toLowerCase();
        const html = (document.body?.innerHTML || '').toLowerCase();
        const loginVisible = !!document.querySelector('#username') && !!document.querySelector('#password');
        if (loginVisible) return false;
        return /my profile|jobs applied|saved applications|candidate profile|welcome,|sign out/i.test(text) ||
          /top_nav_my_profile|top_nav_jobs_applied|signout|logoutlink|_signout|loggedinstatus/.test(html) ||
          (!/career opportunities: sign in|already have an account|forgot your password\?/i.test(text) && !loginVisible);
      },
      failureCheck: (url, content) => {
        const text = (content || '').toLowerCase();
        const html = (document.body?.innerHTML || '').toLowerCase();
        return !!document.querySelector('#errorMsg_1, #uiErrorContainer_2, #uiErrorMsg') ||
          /errormsg_1|uierrorcontainer_2|uierrormsg/.test(html) ||
          /incorrect|invalid user id|invalid login|login failed|unable to sign in|wrong email|wrong password/.test(text);
      }
    }
  };

  function findVisibleByText(selectors, regex) {
    const nodes = Array.from(document.querySelectorAll(selectors));
    return nodes.find((el) => {
      const text = String(el.textContent || el.value || '').trim();
      const visible = !!(el.offsetParent !== null || el.getClientRects?.().length);
      return visible && regex.test(text);
    }) || null;
  }

  function isAllianzLoggedInPage() {
    const text = (document.body?.innerText || '').toLowerCase();
    const html = (document.body?.innerHTML || '').toLowerCase();
    const loginVisible = !!document.querySelector('#username') && !!document.querySelector('#password');
    if (loginVisible) return false;
    return /candidate profile|my profile|jobs applied|saved applications|welcome,|sign out/i.test(text) ||
      /top_nav_my_profile|top_nav_jobs_applied|signout|logoutlink|lnklogout|_signout|loggedinstatus/.test(html);
  }
  function fillAndSubmit(bankId, email, password) {
    const cfg = CONFIG[bankId];
    if (!cfg) return { done: false, error: 'Banque non supportée' };

    const qs = (sel) => {
      for (const part of sel.split(',')) {
        const e = document.querySelector(part.trim());
        if (e && e.offsetParent !== null) return e;
      }
      for (const part of sel.split(',')) {
        const e = document.querySelector(part.trim());
        if (e) return e;
      }
      return null;
    };

    try {
      if (cfg.cookieSel) {
        const cookieBtn = qs(cfg.cookieSel);
        if (cookieBtn && cookieBtn.offsetParent !== null) {
          cookieBtn.click();
        }
      }

      if (bankId === 'axa') {
        const trustLaterBtn = findVisibleByText('button, a, [role="button"]', /me rappeler plus tard|pas sur cet appareil/i);
        if (trustLaterBtn) {
          trustLaterBtn.click();
          return { done: true, submitted: true, successHint: true };
        }

        const passEl = qs(cfg.passwordSel);
        if (phaseAwareEmailStep()) {
          const emailEl = qs(cfg.emailSel);
          const submitEmailBtn = findVisibleByText(cfg.submitSel, /submit|continuer|continue|suivant|next/i) || qs('button[type="submit"]');
          if (!emailEl || !submitEmailBtn) {
            return { done: false, error: 'Étape email AXA introuvable' };
          }
          emailEl.value = email;
          emailEl.dispatchEvent(new Event('input', { bubbles: true }));
          emailEl.dispatchEvent(new Event('change', { bubbles: true }));
          submitEmailBtn.click();
          return { done: true, submitted: true, needPhase2: true };
        }

        if (passEl) {
          const submitPasswordBtn = findVisibleByText(cfg.submitSel, /se connecter|sign in|log in|connexion/i) || qs(cfg.submitSel);
          if (!submitPasswordBtn) {
            return { done: false, error: 'Bouton mot de passe AXA introuvable' };
          }
          passEl.value = password;
          passEl.dispatchEvent(new Event('input', { bubbles: true }));
          passEl.dispatchEvent(new Event('change', { bubbles: true }));
          submitPasswordBtn.click();
          return { done: true, submitted: true };
        }

        return { done: false, error: 'Formulaire AXA introuvable' };
      }

      if (bankId === 'allianz' && isAllianzLoggedInPage()) {
        const signOutEl =
          document.querySelector('#_signout') ||
          document.querySelector('#lnkLogout') ||
          document.querySelector('a.loggedInStatus[title="Sign Out"]') ||
          findVisibleByText('a, button, [role="button"], input[type="button"], input[type="submit"]', /sign out|log out/i);
        if (!signOutEl) {
          return { done: false, error: 'Bouton Sign Out Allianz introuvable.' };
        }
        signOutEl.click();
        return { done: true, submitted: false, signedOut: true, needRetry: true };
      }

      const emailEl = qs(cfg.emailSel);
      const passEl = document.querySelector(cfg.passwordSel);
      const submitEl = document.querySelector(cfg.submitSel);

      if (!emailEl || !passEl || !submitEl) {
        return { done: false, error: 'Formulaire de connexion non trouvé' };
      }

      emailEl.value = email;
      emailEl.dispatchEvent(new Event('input', { bubbles: true }));
      emailEl.dispatchEvent(new Event('change', { bubbles: true }));
      passEl.value = password;
      passEl.dispatchEvent(new Event('input', { bubbles: true }));
      passEl.dispatchEvent(new Event('change', { bubbles: true }));

      submitEl.click();
      return { done: true, submitted: true };
    } catch (e) {
      return { done: false, error: e.message };
    }
  }

  function checkResult(bankId) {
    const cfg = CONFIG[bankId];
    if (!cfg) return { success: false, message: 'Banque non supportée' };

    const url = (window.location?.href || '').toLowerCase();
    const content = (document.body?.innerText || document.body?.innerHTML || '').toLowerCase();

    if (cfg.failureCheck(url, content)) {
      if (bankId === 'bnp_paribas' && /il se peut que le nom d'utilisateur ou le mot de passe soit incorrect, ou que l'accès soit limité\./i.test(document.body?.innerText || '')) {
        return { success: false, message: "Il se peut que le nom d'utilisateur ou le mot de passe soit incorrect, ou que l'accès soit limité." };
      }
      if (bankId === 'societe_generale' && /errorMessageTitle/i.test(document.body?.innerHTML || '')) {
        return { success: false, message: 'Email ou mot de passe incorrect.' };
      }
      if (bankId === 'deloitte' && document.querySelector('[data-automation-id="errorMessage"]')) {
        return { success: false, message: 'Identifiants Deloitte incorrects' };
      }
      if (bankId === 'bpifrance') {
        return { success: false, message: 'Identifiants Bpifrance incorrects' };
      }
      if (bankId === 'axa') {
        const errorBox = document.querySelector('[data-component="login-error-alert"], .alert--error, [role="alert"]');
        const errorText = (errorBox?.textContent || '').trim();
        return {
          success: false,
          message: errorText || "Nom d'utilisateur ou mot de passe incorrect"
        };
      }
      if (bankId === 'allianz') {
        const errText = document.querySelector('#errorMsg_1, #uiErrorMsg, #uiErrorContainer_2')?.innerText?.trim();
        return { success: false, message: errText || 'Identifiants Allianz incorrects.' };
      }
      return { success: false, message: 'Email ou mot de passe incorrect.' };
    }
    if (cfg.successCheck(url, content)) {
      return { success: true, message: 'Connexion réussie' };
    }

    return null;
  }

  window.__taleosConnectionTestFill = function() {
    const params = window.__taleosConnectionTestParams || {};
    const { bankId, email, password, phase } = params;
    if (bankId === 'deloitte' && phase === 1) {
      const connexionBtn = Array.from(document.querySelectorAll('span, button, a')).find(el => /^connexion$/i.test((el.textContent || '').trim()));
      if (connexionBtn && connexionBtn.offsetParent !== null) {
        connexionBtn.click();
        return { done: true, phase: 1, needPhase2: true };
      }
      return { done: false, error: 'Bouton Connexion non trouvé' };
    }
    if (bankId === 'axa') {
      const trustLaterBtn = findVisibleByText('button, a, [role="button"]', /me rappeler plus tard|pas sur cet appareil/i);
      if (trustLaterBtn) {
        trustLaterBtn.click();
        return { done: true, submitted: true, successHint: true };
      }
      if (phase === 1) {
        const emailEl = document.querySelector('input[formcontrolname="email"], input[type="email"], input[name*="email" i], input[id*="email" i], input[name*="username" i], input[id*="username" i]');
        if (emailEl && !document.querySelector('input[type="password"]')) {
          return fillAndSubmit(bankId, email, password);
        }
        return { done: true, phase: 1, needPhase2: true };
      }
      if (phase === 2) {
        return fillAndSubmit(bankId, email, password);
      }
    }
    if (!bankId || !email || !password) {
      const missing = [];
      if (!bankId) missing.push('bankId');
      if (!email) missing.push('email');
      if (!password) missing.push('mot de passe');
      return { done: false, error: `Paramètres manquants: ${missing.join(', ')}` };
    }
    if (bankId === 'allianz' && phase === 1) {
      return fillAndSubmit(bankId, email, password);
    }
    if (bankId === 'allianz' && phase === 2) {
      return fillAndSubmit(bankId, email, password);
    }
    if (!bankId || !email || !password) {
      const missing = [];
      if (!bankId) missing.push('bankId');
      if (!email) missing.push('email');
      if (!password) missing.push('mot de passe');
      return { done: false, error: `Paramètres manquants: ${missing.join(', ')}` };
    }
    return fillAndSubmit(bankId, email, password);
  };

  function phaseAwareEmailStep() {
    const emailEl = document.querySelector('input[formcontrolname="email"], input[type="email"], input[name*="email" i], input[id*="email" i], input[name*="username" i], input[id*="username" i]');
    const passEl = document.querySelector('input[formcontrolname="password"], input[type="password"]');
    return !!emailEl && !passEl;
  }

  window.__taleosConnectionTestCheck = function() {
    const params = window.__taleosConnectionTestParams || {};
    return checkResult(params.bankId || '');
  };
})();
