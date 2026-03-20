/**
 * Taleos - Outlook Web Interceptor (Version 1.0.53)
 * Ce script s'exécute sur outlook.live.com ou outlook.office.com.
 * Il surveille l'arrivée d'emails BPCE/Oracle et extrait le code PIN s'il est récent (< 2 min).
 */

(function() {
  'use strict';

  function log(msg) {
    console.log(`[${new Date().toLocaleTimeString('fr-FR')}] [Taleos Outlook] ${msg}`);
  }

  const PIN_REGEX = /\b(\d{6})\b/;
  const ORACLE_SENDER = 'ekez.fa.sender@workflow.mail.em2.cloud.oracle.com';
  const MAX_PIN_AGE_MS = 2 * 60 * 1000; // 2 minutes

  function isEmailRecent(element) {
    // Outlook utilise souvent un attribut 'title' ou un élément de temps avec une date complète
    const timeEl = element.querySelector('time') || element.querySelector('[data-testid="SystemTime"]');
    if (timeEl) {
      const timeStr = timeEl.getAttribute('datetime') || timeEl.getAttribute('title') || '';
      if (timeStr) {
        const emailDate = new Date(timeStr);
        const now = new Date();
        const age = now - emailDate;
        return age >= 0 && age < MAX_PIN_AGE_MS;
      }
    }
    // Fallback : Si on ne peut pas lire l'heure, on se base sur la visibilité du badge "Nouveau" ou on suppose que c'est récent si c'est le tout premier mail
    return true; 
  }

  function scanEmails() {
    // 1. Chercher dans la liste des messages
    const messages = document.querySelectorAll('[role="option"], [data-testid="CustomNode"]');
    for (const msg of messages) {
      const text = msg.textContent || '';
      if ((text.includes('Confirmer votre identité') || text.includes(ORACLE_SENDER)) && isEmailRecent(msg)) {
        const match = text.match(PIN_REGEX);
        if (match) {
          const pinCode = match[1];
          log('📌 Nouveau code PIN intercepté (récent) : ' + pinCode);
          chrome.runtime.sendMessage({ action: 'bpce_pin_code', pinCode });
          // On marque le message pour ne pas le traiter deux fois
          msg.setAttribute('data-taleos-processed', 'true');
          return;
        }
      }
    }

    // 2. Chercher dans le corps du message ouvert
    const body = document.querySelector('[role="main"]') || document.querySelector('#ItemField_0');
    if (body) {
      const text = body.textContent || '';
      if (text.includes('code d\'accès à usage unique')) {
        const match = text.match(PIN_REGEX);
        if (match) {
          const pinCode = match[1];
          // Pour le corps du mail, on suppose que l'utilisateur vient de l'ouvrir
          log('📌 Code PIN trouvé dans le mail ouvert : ' + pinCode);
          chrome.runtime.sendMessage({ action: 'bpce_pin_code', pinCode });
        }
      }
    }
  }

  // Scanner régulièrement
  setInterval(scanEmails, 3000);
  
  const observer = new MutationObserver((mutations) => {
    for (const mutation of mutations) {
      if (mutation.addedNodes.length > 0) {
        scanEmails();
        break;
      }
    }
  });
  observer.observe(document.body, { childList: true, subtree: true });
  
  log('👁️  Outlook Interceptor actif (V1.0.53) : surveillance des emails récents BPCE...');
})();
