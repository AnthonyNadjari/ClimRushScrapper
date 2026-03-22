/**
 * ClimRush Web — Main app logic.
 * Handles view navigation and initialization.
 */

const VIEWS = ['setup', 'config', 'run', 'dashboard'];

function navigateTo(view) {
  VIEWS.forEach(v => {
    const el = document.getElementById(`view-${v}`);
    const tab = document.getElementById(`tab-${v}`);
    if (el) el.classList.toggle('active', v === view);
    if (tab) tab.classList.toggle('active', v === view);
  });

  // Load view data
  if (view === 'config') loadConfig();
  if (view === 'run') checkActiveRuns();
  if (view === 'dashboard') loadDashboard();
}

function showToast(message, type = 'info') {
  const container = document.getElementById('toast-container');
  const toast = document.createElement('div');
  toast.className = `toast toast-${type}`;
  toast.textContent = message;
  container.appendChild(toast);
  setTimeout(() => toast.classList.add('show'), 10);
  setTimeout(() => {
    toast.classList.remove('show');
    setTimeout(() => toast.remove(), 300);
  }, 4000);
}

async function setupSubmit(e) {
  e.preventDefault();
  const pat = document.getElementById('input-pat').value.trim();
  const repo = document.getElementById('input-repo').value.trim();

  if (!pat || !repo) {
    showToast('Remplissez tous les champs', 'error');
    return;
  }

  const parts = repo.split('/');
  if (parts.length !== 2) {
    showToast('Format repo: owner/repo-name', 'error');
    return;
  }

  window.githubAPI.save(pat, parts[0], parts[1]);

  try {
    const login = await window.githubAPI.testConnection();
    showToast(`Connecte en tant que ${login}`, 'success');
    document.getElementById('nav-tabs').style.display = 'flex';
    navigateTo('dashboard');
  } catch (e) {
    window.githubAPI.clear();
    showToast('Token invalide: ' + e.message, 'error');
  }
}

function logout() {
  window.githubAPI.clear();
  document.getElementById('nav-tabs').style.display = 'none';
  navigateTo('setup');
}

// ── Init ──
document.addEventListener('DOMContentLoaded', () => {
  if (window.githubAPI.isConfigured()) {
    document.getElementById('nav-tabs').style.display = 'flex';
    document.getElementById('input-repo').value = `${window.githubAPI.owner}/${window.githubAPI.repo}`;
    navigateTo('dashboard');
  } else {
    navigateTo('setup');
  }
});
