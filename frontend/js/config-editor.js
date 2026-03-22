/**
 * Config editor: CRUD segments, zones, concurrency.
 */

let currentConfig = null;
let zonesData = null;

async function loadConfig() {
  try {
    showConfigLoading(true);
    currentConfig = await window.githubAPI.getConfig();
    if (!currentConfig) {
      // Load from local config as fallback
      const res = await fetch('config/segments.json');
      currentConfig = await res.json();
    }
    // Load zones
    try {
      const zRes = await fetch('config/zones.json');
      zonesData = await zRes.json();
    } catch {
      zonesData = { departments: [{ code: '75', label: 'Paris' }] };
    }
    renderConfig();
  } catch (e) {
    showToast('Erreur chargement config: ' + e.message, 'error');
  } finally {
    showConfigLoading(false);
  }
}

function renderConfig() {
  if (!currentConfig) return;

  // Global settings
  document.getElementById('concurrency').value = currentConfig.concurrency || 4;

  // Zones checkboxes
  const zonesContainer = document.getElementById('zones-list');
  zonesContainer.innerHTML = '';
  const selectedZones = currentConfig.zones || ['75'];
  (zonesData?.departments || []).forEach(dept => {
    const checked = selectedZones.includes(dept.code) ? 'checked' : '';
    zonesContainer.innerHTML += `
      <label class="zone-chip ${checked ? 'active' : ''}" data-code="${dept.code}">
        <input type="checkbox" value="${dept.code}" ${checked} onchange="toggleZone(this)">
        <span>${dept.label} (${dept.code})</span>
      </label>
    `;
  });

  // Segments
  const container = document.getElementById('segments-list');
  container.innerHTML = '';
  (currentConfig.segments || []).forEach((seg, idx) => {
    container.innerHTML += renderSegmentCard(seg, idx);
  });
}

function renderSegmentCard(seg, idx) {
  const enabledClass = seg.enabled !== false ? 'enabled' : 'disabled';
  const enabledCheck = seg.enabled !== false ? 'checked' : '';
  return `
    <div class="segment-card ${enabledClass}" id="seg-${idx}">
      <div class="segment-header">
        <label class="toggle">
          <input type="checkbox" ${enabledCheck} onchange="toggleSegment(${idx}, this.checked)">
          <span class="toggle-slider"></span>
        </label>
        <input type="text" class="segment-name" value="${seg.name}" onchange="updateSegName(${idx}, this.value)">
        <button class="btn-icon btn-danger" onclick="removeSegment(${idx})" title="Supprimer">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 6h18M8 6V4a2 2 0 012-2h4a2 2 0 012 2v2m3 0v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6h14"/></svg>
        </button>
      </div>
      <div class="segment-body">
        <div class="field-group">
          <label>Sources</label>
          <div class="source-toggles">
            <label class="source-chip ${(seg.sources || []).includes('gmaps') ? 'active' : ''}">
              <input type="checkbox" ${(seg.sources || []).includes('gmaps') ? 'checked' : ''} onchange="toggleSource(${idx}, 'gmaps', this.checked)">
              Google Maps
            </label>
            <label class="source-chip ${(seg.sources || []).includes('pagesjaunes') ? 'active' : ''}">
              <input type="checkbox" ${(seg.sources || []).includes('pagesjaunes') ? 'checked' : ''} onchange="toggleSource(${idx}, 'pagesjaunes', this.checked)">
              Pages Jaunes
            </label>
          </div>
        </div>
        <div class="field-group">
          <label>Requetes Google Maps <span class="count">(${(seg.gmaps_queries || []).length})</span></label>
          <textarea rows="4" onchange="updateQueries(${idx}, 'gmaps_queries', this.value)">${(seg.gmaps_queries || []).join('\n')}</textarea>
        </div>
        <div class="field-group">
          <label>Requetes Pages Jaunes <span class="count">(${(seg.pj_queries || []).length})</span></label>
          <textarea rows="3" onchange="updateQueries(${idx}, 'pj_queries', this.value)">${(seg.pj_queries || []).join('\n')}</textarea>
        </div>
        <div class="field-group">
          <label>Mots-cles exclus</label>
          <input type="text" value="${(seg.exclude || []).join(', ')}" onchange="updateExclude(${idx}, this.value)">
        </div>
      </div>
    </div>
  `;
}

function toggleZone(checkbox) {
  const chip = checkbox.closest('.zone-chip');
  chip.classList.toggle('active', checkbox.checked);
  currentConfig.zones = Array.from(document.querySelectorAll('#zones-list input:checked')).map(i => i.value);
}

function toggleSegment(idx, enabled) {
  currentConfig.segments[idx].enabled = enabled;
  const card = document.getElementById(`seg-${idx}`);
  card.classList.toggle('enabled', enabled);
  card.classList.toggle('disabled', !enabled);
}

function toggleSource(idx, source, checked) {
  const sources = currentConfig.segments[idx].sources || [];
  if (checked && !sources.includes(source)) sources.push(source);
  if (!checked) {
    const i = sources.indexOf(source);
    if (i >= 0) sources.splice(i, 1);
  }
  currentConfig.segments[idx].sources = sources;
}

function updateSegName(idx, name) {
  currentConfig.segments[idx].name = name;
}

function updateQueries(idx, field, text) {
  currentConfig.segments[idx][field] = text.split('\n').map(l => l.trim()).filter(l => l);
}

function updateExclude(idx, text) {
  currentConfig.segments[idx].exclude = text.split(',').map(s => s.trim()).filter(s => s);
}

function addSegment() {
  currentConfig.segments.push({
    name: 'Nouveau segment',
    enabled: true,
    sources: ['gmaps', 'pagesjaunes'],
    gmaps_queries: [],
    pj_queries: [],
    exclude: [],
  });
  renderConfig();
  // Scroll to new segment
  const cards = document.querySelectorAll('.segment-card');
  cards[cards.length - 1]?.scrollIntoView({ behavior: 'smooth' });
}

function removeSegment(idx) {
  if (confirm(`Supprimer "${currentConfig.segments[idx].name}" ?`)) {
    currentConfig.segments.splice(idx, 1);
    renderConfig();
  }
}

async function saveConfig() {
  try {
    currentConfig.concurrency = parseInt(document.getElementById('concurrency').value) || 4;
    showToast('Sauvegarde en cours...', 'info');
    await window.githubAPI.saveConfig(currentConfig);
    showToast('Config sauvegardee ! Le scraper va se lancer automatiquement.', 'success');
  } catch (e) {
    showToast('Erreur sauvegarde: ' + e.message, 'error');
  }
}

function showConfigLoading(show) {
  const el = document.getElementById('config-loading');
  if (el) el.style.display = show ? 'flex' : 'none';
}
