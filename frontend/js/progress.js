/**
 * Workflow progress tracking with polling.
 */

let pollInterval = null;
let currentRunId = null;

async function triggerScrape() {
  try {
    document.getElementById('btn-launch').disabled = true;
    document.getElementById('btn-launch').textContent = 'Lancement...';
    showToast('Demarrage du scraping...', 'info');

    await window.githubAPI.triggerScrape();

    showToast('Workflow lance ! Les resultats arrivent dans ~15 min.', 'success');

    // Wait a bit then start polling
    setTimeout(() => startPolling(), 5000);
  } catch (e) {
    showToast('Erreur: ' + e.message, 'error');
    document.getElementById('btn-launch').disabled = false;
    document.getElementById('btn-launch').textContent = 'Lancer le Scraping';
  }
}

function startPolling() {
  if (pollInterval) clearInterval(pollInterval);
  pollInterval = setInterval(pollWorkflow, 10000);
  pollWorkflow(); // immediate first poll
}

function stopPolling() {
  if (pollInterval) {
    clearInterval(pollInterval);
    pollInterval = null;
  }
}

async function pollWorkflow() {
  try {
    const runs = await window.githubAPI.getWorkflowRuns(3);
    renderRuns(runs);

    // If latest run is completed, stop polling
    if (runs.length > 0) {
      const latest = runs[0];
      currentRunId = latest.id;
      if (latest.status === 'completed') {
        stopPolling();
        document.getElementById('btn-launch').disabled = false;
        document.getElementById('btn-launch').textContent = 'Lancer le Scraping';
        if (latest.conclusion === 'success') {
          showToast('Scraping termine avec succes !', 'success');
        } else {
          showToast(`Scraping termine: ${latest.conclusion}`, 'error');
        }
      }
    }
  } catch (e) {
    console.error('Poll error:', e);
  }
}

async function renderRuns(runs) {
  const container = document.getElementById('workflow-runs');
  if (!container) return;

  if (runs.length === 0) {
    container.innerHTML = '<p class="empty-state">Aucun run trouve. Lancez le scraping !</p>';
    return;
  }

  let html = '';
  for (const run of runs) {
    const status = run.status;
    const conclusion = run.conclusion;
    const date = new Date(run.created_at).toLocaleString('fr-FR');
    const elapsed = run.status === 'completed'
      ? Math.round((new Date(run.updated_at) - new Date(run.created_at)) / 1000)
      : Math.round((Date.now() - new Date(run.created_at)) / 1000);
    const elapsedStr = elapsed > 60 ? `${Math.floor(elapsed/60)}min ${elapsed%60}s` : `${elapsed}s`;

    let statusClass = 'status-pending';
    let statusIcon = '⏳';
    if (status === 'completed' && conclusion === 'success') { statusClass = 'status-success'; statusIcon = '✅'; }
    else if (status === 'completed' && conclusion !== 'success') { statusClass = 'status-error'; statusIcon = '❌'; }
    else if (status === 'in_progress') { statusClass = 'status-running'; statusIcon = '🔄'; }
    else if (status === 'queued') { statusClass = 'status-pending'; statusIcon = '⏳'; }

    html += `
      <div class="run-card ${statusClass}">
        <div class="run-header">
          <span class="run-status">${statusIcon} ${status}${conclusion ? ' — ' + conclusion : ''}</span>
          <span class="run-time">${date} (${elapsedStr})</span>
        </div>
        <div class="run-jobs" id="jobs-${run.id}">Chargement des jobs...</div>
      </div>
    `;

    // Load jobs asynchronously
    loadJobs(run.id);
  }
  container.innerHTML = html;
}

async function loadJobs(runId) {
  try {
    const jobs = await window.githubAPI.getRunJobs(runId);
    const container = document.getElementById(`jobs-${runId}`);
    if (!container) return;

    let html = '<div class="jobs-grid">';
    for (const job of jobs) {
      let icon = '⏳';
      let cls = 'job-pending';
      if (job.status === 'completed' && job.conclusion === 'success') { icon = '✅'; cls = 'job-success'; }
      else if (job.status === 'completed') { icon = '❌'; cls = 'job-error'; }
      else if (job.status === 'in_progress') { icon = '🔄'; cls = 'job-running'; }

      html += `<div class="job-chip ${cls}">${icon} ${job.name}</div>`;
    }
    html += '</div>';
    container.innerHTML = html;
  } catch {}
}

async function cancelCurrentRun() {
  if (currentRunId) {
    try {
      await window.githubAPI.cancelRun(currentRunId);
      showToast('Run annule', 'info');
      stopPolling();
    } catch (e) {
      showToast('Erreur annulation: ' + e.message, 'error');
    }
  }
}

// Auto-poll on page load if there's a running workflow
async function checkActiveRuns() {
  if (!window.githubAPI.isConfigured()) return;
  try {
    const runs = await window.githubAPI.getWorkflowRuns(1);
    if (runs.length > 0 && (runs[0].status === 'in_progress' || runs[0].status === 'queued')) {
      startPolling();
    } else {
      renderRuns(runs);
    }
  } catch {}
}
