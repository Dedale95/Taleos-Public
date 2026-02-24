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

const loginView = document.getElementById('login-view');
const loggedView = document.getElementById('logged-view');
const loginForm = document.getElementById('login-form');
const loginError = document.getElementById('login-error');
const loginLoading = document.getElementById('login-loading');
const loginBtn = document.getElementById('login-btn');
const logoutBtn = document.getElementById('logout-btn');

function showLogin() {
  if (loginView) loginView.classList.remove('hidden');
  if (loggedView) loggedView.classList.add('hidden');
}

function showLogged(user) {
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

function setVersion() {
  try {
    const v = chrome?.runtime?.getManifest?.()?.version || '?';
    const badge = document.getElementById('version-badge');
    const badgeLogged = document.getElementById('version-badge-logged');
    if (badge) badge.textContent = `v${v}`;
    if (badgeLogged) badgeLogged.textContent = `Version ${v}`;
  } catch (_) {}
}

function init() {
  setVersion();
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
            taleosIdToken: token
          });
        }
      } catch (e) {
        console.warn('Token storage:', e);
      }
    } else {
      showLogin();
      if (chrome?.storage?.local) {
        chrome.storage.local.remove(['taleosUserId', 'taleosIdToken']);
      }
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

  logoutBtn?.addEventListener('click', () => auth.signOut());

  const doReload = () => { if (chrome?.runtime?.reload) chrome.runtime.reload(); };
  document.getElementById('reload-btn')?.addEventListener('click', doReload);
  document.getElementById('reload-btn-login')?.addEventListener('click', doReload);

  document.getElementById('test-creds-btn')?.addEventListener('click', async () => {
    const status = document.getElementById('creds-status');
    if (!status) return;
    status.textContent = 'Vérification...';
    status.className = 'loading-text';
    try {
      const r = await chrome.runtime.sendMessage({ action: 'test_credentials', bankId: 'credit_agricole' });
      if (r?.ok) {
        status.textContent = `✅ Identifiants CA trouvés pour ${r.email}`;
        status.className = 'error-text';
        status.style.color = '#059669';
      } else {
        status.textContent = `❌ ${r?.error || 'Erreur inconnue'}`;
        status.className = 'error-text';
      }
    } catch (e) {
      status.textContent = `❌ ${e.message || 'Erreur'}`;
      status.className = 'error-text';
    }
  });
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', init);
} else {
  init();
}
