/**
 * GitHub API wrapper for ClimRush.
 * Handles: trigger workflows, poll status, fetch/commit files.
 */
class GitHubAPI {
  constructor() {
    this.pat = localStorage.getItem('climrush_pat') || '';
    this.owner = localStorage.getItem('climrush_owner') || '';
    this.repo = localStorage.getItem('climrush_repo') || '';
    this.baseUrl = 'https://api.github.com';
  }

  isConfigured() {
    return !!(this.pat && this.owner && this.repo);
  }

  save(pat, owner, repo) {
    this.pat = pat;
    this.owner = owner;
    this.repo = repo;
    localStorage.setItem('climrush_pat', pat);
    localStorage.setItem('climrush_owner', owner);
    localStorage.setItem('climrush_repo', repo);
  }

  clear() {
    this.pat = '';
    localStorage.removeItem('climrush_pat');
    localStorage.removeItem('climrush_owner');
    localStorage.removeItem('climrush_repo');
  }

  async _fetch(endpoint, options = {}) {
    const url = endpoint.startsWith('http') ? endpoint : `${this.baseUrl}${endpoint}`;
    const res = await fetch(url, {
      ...options,
      headers: {
        'Authorization': `token ${this.pat}`,
        'Accept': 'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28',
        ...options.headers,
      },
    });
    if (!res.ok) {
      const text = await res.text();
      throw new Error(`GitHub API ${res.status}: ${text}`);
    }
    if (res.status === 204) return null;
    return res.json();
  }

  async testConnection() {
    const user = await this._fetch('/user');
    return user.login;
  }

  // ── Workflow triggers ──

  async triggerScrape(configBase64 = '') {
    await this._fetch(`/repos/${this.owner}/${this.repo}/actions/workflows/scrape.yml/dispatches`, {
      method: 'POST',
      body: JSON.stringify({
        ref: 'main',
        inputs: { config: configBase64 || '' },
      }),
    });
    return true;
  }

  async getWorkflowRuns(limit = 5) {
    const data = await this._fetch(
      `/repos/${this.owner}/${this.repo}/actions/workflows/scrape.yml/runs?per_page=${limit}`
    );
    return data.workflow_runs || [];
  }

  async getRunJobs(runId) {
    const data = await this._fetch(`/repos/${this.owner}/${this.repo}/actions/runs/${runId}/jobs`);
    return data.jobs || [];
  }

  async cancelRun(runId) {
    await this._fetch(`/repos/${this.owner}/${this.repo}/actions/runs/${runId}/cancel`, {
      method: 'POST',
    });
  }

  // ── File operations ──

  async getFileContent(path) {
    try {
      const data = await this._fetch(`/repos/${this.owner}/${this.repo}/contents/${path}?ref=main`);
      const content = atob(data.content.replace(/\n/g, ''));
      return { content, sha: data.sha };
    } catch {
      return null;
    }
  }

  async commitFile(path, content, message) {
    const existing = await this.getFileContent(path);
    const body = {
      message,
      content: btoa(unescape(encodeURIComponent(content))),
      branch: 'main',
    };
    if (existing) body.sha = existing.sha;

    await this._fetch(`/repos/${this.owner}/${this.repo}/contents/${path}`, {
      method: 'PUT',
      body: JSON.stringify(body),
    });
  }

  async getConfig() {
    const result = await this.getFileContent('config/segments.json');
    if (result) return JSON.parse(result.content);
    return null;
  }

  async saveConfig(config) {
    const content = JSON.stringify(config, null, 2);
    await this.commitFile('config/segments.json', content, 'Update scraper config from UI');
  }

  async getLatestResults() {
    try {
      const result = await this.getFileContent('results/latest.json');
      if (result) return JSON.parse(result.content);
    } catch {}
    return null;
  }

  getExcelUrl() {
    return `https://raw.githubusercontent.com/${this.owner}/${this.repo}/main/results/CLIMRUSH_Leads.xlsx`;
  }

  getCsvUrl() {
    return `https://raw.githubusercontent.com/${this.owner}/${this.repo}/main/results/CLIMRUSH_Leads.csv`;
  }
}

window.githubAPI = new GitHubAPI();
