/**
 * Dashboard: display latest results.
 */

async function loadDashboard() {
  const container = document.getElementById('dashboard-content');
  if (!container) return;

  try {
    container.innerHTML = '<div class="loading-spinner">Chargement des resultats...</div>';

    let data = null;

    // Try GitHub API first
    if (window.githubAPI.isConfigured()) {
      data = await window.githubAPI.getLatestResults();
    }

    // Fallback to local file
    if (!data) {
      try {
        const res = await fetch('results/latest.json');
        data = await res.json();
      } catch {}
    }

    if (!data) {
      container.innerHTML = `
        <div class="empty-state">
          <h3>Pas encore de resultats</h3>
          <p>Lancez le scraping depuis l'onglet "Run" pour generer des leads.</p>
        </div>
      `;
      return;
    }

    renderDashboard(data);
  } catch (e) {
    container.innerHTML = `<div class="error-state">Erreur: ${e.message}</div>`;
  }
}

function renderDashboard(data) {
  const container = document.getElementById('dashboard-content');
  const date = data.run_date ? new Date(data.run_date).toLocaleString('fr-FR') : 'N/A';
  const telPct = data.total_leads ? Math.round(data.total_phones / data.total_leads * 100) : 0;
  const sitePct = data.total_leads ? Math.round(data.total_websites / data.total_leads * 100) : 0;

  let segRows = '';
  (data.segments || []).forEach(seg => {
    const pct = seg.count ? Math.round(seg.phones / seg.count * 100) : 0;
    segRows += `
      <tr>
        <td class="seg-name">${seg.name}</td>
        <td class="num">${seg.count}</td>
        <td class="num">${seg.phones}</td>
        <td class="num">${seg.websites}</td>
        <td class="num">${pct}%</td>
      </tr>
    `;
  });

  let topRows = '';
  (data.top_leads || []).slice(0, 20).forEach(lead => {
    topRows += `
      <tr>
        <td>${lead.nom_entreprise || ''}</td>
        <td class="mono">${lead.telephone || '-'}</td>
        <td class="url">${lead.site_web ? `<a href="${lead.site_web}" target="_blank">${truncate(lead.site_web, 35)}</a>` : '-'}</td>
        <td>${lead.segment || ''}</td>
        <td class="mono">${lead.source || ''}</td>
      </tr>
    `;
  });

  container.innerHTML = `
    <div class="dash-header">
      <span class="dash-date">Derniere mise a jour : ${date}</span>
      <div class="dash-actions">
        <a href="${window.githubAPI.getExcelUrl()}" class="btn btn-primary" download>
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4M7 10l5 5 5-5M12 15V3"/></svg>
          Telecharger Excel
        </a>
        <a href="${window.githubAPI.getCsvUrl()}" class="btn btn-secondary" download>CSV</a>
      </div>
    </div>

    <div class="kpi-grid">
      <div class="kpi-card">
        <div class="kpi-value">${data.total_leads}</div>
        <div class="kpi-label">Leads uniques</div>
      </div>
      <div class="kpi-card kpi-phone">
        <div class="kpi-value">${data.total_phones}</div>
        <div class="kpi-label">Telephones (${telPct}%)</div>
      </div>
      <div class="kpi-card kpi-web">
        <div class="kpi-value">${data.total_websites}</div>
        <div class="kpi-label">Sites web (${sitePct}%)</div>
      </div>
      <div class="kpi-card kpi-seg">
        <div class="kpi-value">${(data.segments || []).length}</div>
        <div class="kpi-label">Segments</div>
      </div>
    </div>

    <h3>Par segment</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th>Segment</th>
          <th>Leads</th>
          <th>Tel</th>
          <th>Sites</th>
          <th>% Tel</th>
        </tr>
      </thead>
      <tbody>${segRows}</tbody>
    </table>

    <h3>Top 20 leads</h3>
    <table class="data-table leads-table">
      <thead>
        <tr>
          <th>Entreprise</th>
          <th>Telephone</th>
          <th>Site web</th>
          <th>Segment</th>
          <th>Source</th>
        </tr>
      </thead>
      <tbody>${topRows}</tbody>
    </table>
  `;
}

function truncate(str, max) {
  if (!str) return '';
  return str.length > max ? str.slice(0, max) + '...' : str;
}
