/**
 * Taleos Extension - Popup
 * Authentification Firebase (mêmes identifiants que le site web)
 */

const firebaseConfig = {
  apiKey: "AIzaSyAGeNfIevsaNjfbKTYWMaURhJWdfzWMjmc",
  authDomain: "project-taleos.firebaseapp.com",
  projectId: "project-taleos",
  storageBucket: "project-taleos.firebasestorage.app",
  messagingSenderId: "974062127016",
  appId: "1:974062127016:web:b6cffae44f1bae56f03f9d",
  measurementId: "G-4PZJ4QXMJ0"
};

const loadingView = document.getElementById('loading-view');
const loginView = document.getElementById('login-view');
const loggedView = document.getElementById('logged-view');
const loginForm = document.getElementById('login-form');
let pendingLoginTimeout = null;
const loginError = document.getElementById('login-error');
const loginLoading = document.getElementById('login-loading');
const loginBtn = document.getElementById('login-btn');
const logoutBtn = document.getElementById('logout-btn');

function showLogin() {
  if (pendingLoginTimeout) {
    clearTimeout(pendingLoginTimeout);
    pendingLoginTimeout = null;
  }
  if (loadingView) loadingView.classList.add('hidden');
  if (loginView) loginView.classList.remove('hidden');
  if (loggedView) loggedView.classList.add('hidden');
}

function showLogged(user) {
  if (pendingLoginTimeout) {
    clearTimeout(pendingLoginTimeout);
    pendingLoginTimeout = null;
  }
  if (loadingView) loadingView.classList.add('hidden');
  if (loginView) loginView.classList.add('hidden');
  if (loggedView) loggedView.classList.remove('hidden');
  const emailEl = document.getElementById('user-email');
  const initialEl = document.getElementById('user-initial');
  if (emailEl) emailEl.textContent = user.email || '';
  if (initialEl) initialEl.textContent = (user.email || user.displayName || '?')[0].toUpperCase();
}

function showError(msg) {
  if (loginError) {
    loginError.textContent = msg || '';
    loginError.classList.toggle('hidden', !msg);
  }
  if (loginLoading) loginLoading.classList.add('hidden');
  if (loginBtn) {
    loginBtn.disabled = false;
    loginBtn.textContent = 'Se connecter';
  }
}

function setLoading(loading) {
  if (loginLoading) loginLoading.classList.toggle('hidden', !loading);
  if (loginError) loginError.classList.add('hidden');
  if (loginBtn) {
    loginBtn.disabled = loading;
    loginBtn.textContent = loading ? 'Connexion...' : 'Se connecter';
  }
}

async function setVersion() {
  try {
    const manifest = chrome?.runtime?.getManifest?.() || {};
    const v = manifest.version || '?';
    const badge = document.getElementById('version-badge');
    const badgeLogged = document.getElementById('version-badge-logged');
    const dateEl = document.getElementById('version-date');
    if (badge) badge.textContent = `v${v}`;
    if (badgeLogged) badgeLogged.textContent = `Version ${v}`;
    if (dateEl) {
      const { taleosLastUpdate } = await chrome.storage.local.get('taleosLastUpdate');
      dateEl.textContent = taleosLastUpdate ? `Mise à jour : ${taleosLastUpdate}` : '';
    }
  } catch (_) {}
}

function setupLogout() {
  logoutBtn?.addEventListener('click', () => {
    chrome.storage.local.remove(['taleosUserId', 'taleosIdToken', 'taleosUserEmail']);
    if (typeof firebase !== 'undefined' && firebase.auth) {
      firebase.auth().signOut();
    }
    showLogin();
  });
}

async function runDiagnostic() {
  const statusEl = document.getElementById('diagnostic-status');
  if (!statusEl) return;
  statusEl.textContent = 'Test en cours...';
  statusEl.style.color = '#6b7280';
  try {
    const t0 = Date.now();
    await chrome.runtime.sendMessage({ action: 'ping' });
    const ms = Date.now() - t0;
    statusEl.textContent = `✅ Connexion OK (${ms} ms)`;
    statusEl.style.color = '#059669';
  } catch (e) {
    const msg = (e?.message || String(e)).toLowerCase();
    const invalidated = /context invalidated|receiving end does not exist/i.test(msg);
    statusEl.textContent = invalidated
      ? '❌ Extension déconnectée — Rafraîchissez les pages Taleos'
      : `❌ Erreur : ${(e?.message || String(e)).slice(0, 50)}`;
    statusEl.style.color = '#dc2626';
  }
}

async function refreshTaleosTabs() {
  try {
    const [t1, t2, t3] = await Promise.all([
      chrome.tabs.query({ url: 'https://*.taleos.co/*' }),
      chrome.tabs.query({ url: 'https://*.github.io/*' }),
      chrome.tabs.query({ url: 'http://localhost/*' })
    ]);
    const seen = new Set();
    const tabs = [].concat(t1, t2, t3).filter(t => { if (seen.has(t.id)) return false; seen.add(t.id); return true; });
    for (const tab of tabs) {
      chrome.tabs.reload(tab.id).catch(() => {});
    }
    const statusEl = document.getElementById('diagnostic-status');
    if (statusEl) {
      statusEl.textContent = tabs.length ? `✅ ${tabs.length} page(s) Taleos rafraîchie(s)` : 'Aucune page Taleos ouverte';
      statusEl.style.color = '#059669';
    }
  } catch (e) {
    const statusEl = document.getElementById('diagnostic-status');
    if (statusEl) {
      statusEl.textContent = '❌ ' + (e?.message || 'Erreur');
      statusEl.style.color = '#dc2626';
    }
  }
}

async function init() {
  await setVersion();
  const doReload = () => { if (chrome?.runtime?.reload) chrome.runtime.reload(); };
  document.getElementById('reload-btn')?.addEventListener('click', doReload);
  document.getElementById('reload-btn-login')?.addEventListener('click', doReload);
  document.getElementById('diagnostic-btn')?.addEventListener('click', runDiagnostic);
  document.getElementById('refresh-taleos-btn')?.addEventListener('click', refreshTaleosTabs);
  const macroBtn = document.getElementById('macro-toggle-btn');
  const macroStatus = document.getElementById('macro-status');
  if (macroBtn && macroStatus) {
    let recording = false;
    macroBtn.addEventListener('click', async () => {
      try {
        const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
        if (!tab || !tab.id) return;
        if (!recording) {
          await chrome.tabs.sendMessage(tab.id, { action: 'taleos_start_macro_record' });
          recording = true;
          macroBtn.textContent = 'Arrêter l\'enregistrement';
          macroStatus.textContent = 'Enregistrement en cours… (les actions seront visibles dans la console de la page)';
        } else {
          const res = await chrome.tabs.sendMessage(tab.id, { action: 'taleos_stop_macro_record' });
          recording = false;
          macroBtn.textContent = 'Démarrer l\'enregistrement';
          macroStatus.textContent = res && res.count != null
            ? `Dernier enregistrement : ${res.count} action(s) (voir console Workday)`
            : 'Inactif';
        }
      } catch (e) {
        console.warn('Macro toggle error:', e);
      }
    });
  }
  setupLogout();
  runDiagnostic();

  const { taleosUserId, taleosIdToken, taleosUserEmail } = await chrome.storage.local.get(['taleosUserId', 'taleosIdToken', 'taleosUserEmail']);
  if (taleosUserId && taleosIdToken) {
    showLogged({ email: taleosUserEmail || '(connecté)', uid: taleosUserId });
    return;
  }
  if (typeof firebase === 'undefined') {
    showError('Firebase non chargé. Rechargez l\'extension.');
    return;
  }
  if (!firebase.apps?.length) {
    firebase.initializeApp(firebaseConfig);
  }
  const auth = firebase.auth();

  auth.onAuthStateChanged(async (user) => {
    if (user) {
      showLogged(user);
      try {
        const token = await user.getIdToken();
        if (chrome?.storage?.local) {
          await chrome.storage.local.set({
            taleosUserId: user.uid,
            taleosIdToken: token,
            taleosUserEmail: user.email || ''
          });
        }
      } catch (e) {
        console.warn('Token storage:', e);
      }
    } else {
      if (pendingLoginTimeout) clearTimeout(pendingLoginTimeout);
      pendingLoginTimeout = setTimeout(() => {
        pendingLoginTimeout = null;
        showLogin();
        if (chrome?.storage?.local) {
          chrome.storage.local.remove(['taleosUserId', 'taleosIdToken', 'taleosUserEmail']);
        }
      }, 450);
    }
  });

  loginForm?.addEventListener('submit', async (e) => {
    e.preventDefault();
    showError('');
    const email = document.getElementById('email')?.value?.trim();
    const password = document.getElementById('password')?.value;
    if (!email || !password) {
      showError('Email et mot de passe requis.');
      return;
    }
    setLoading(true);
    try {
      await auth.signInWithEmailAndPassword(email, password);
    } catch (err) {
      const msg = err.code === 'auth/invalid-credential' ? 'Email ou mot de passe incorrect.' :
        err.code === 'auth/user-not-found' ? 'Aucun compte avec cet email.' :
        err.message || 'Erreur de connexion';
      showError(msg);
    } finally {
      setLoading(false);
    }
  });
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', init);
} else {
  init();
}
