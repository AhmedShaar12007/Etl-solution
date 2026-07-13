/* Pipeline Control Center — App Logic */

const API = 'http://localhost:5050';
let currentProject = null;
let currentTrigger = 'sql';
let currentModel   = null;
let vars           = {};

// ══════════════════════════════════════════════════════════
// INIT
// ══════════════════════════════════════════════════════════
window.addEventListener('DOMContentLoaded', () => {
  loadProjects();
  checkAirflow();
  renderTriggerConfig('sql');
  setInterval(checkAirflow, 15000);
});

// ══════════════════════════════════════════════════════════
// NAVIGATION
// ══════════════════════════════════════════════════════════
function switchTab(name) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
  document.getElementById('tab-'   + name).classList.add('active');
  document.getElementById('panel-' + name).classList.add('active');

  if (name === 'dbt'    && currentProject) loadModels();
  if (name === 'logs')  {}
}

// ══════════════════════════════════════════════════════════
// PROJECTS
// ══════════════════════════════════════════════════════════
async function loadProjects() {
  const list = await api('/api/projects');
  const el   = document.getElementById('project-list');
  el.innerHTML = '';

  if (!list.length) {
    el.innerHTML = '<p style="font-size:12px;color:rgba(255,255,255,.35);padding:6px">No projects yet</p>';
    return;
  }

  const colors = ['#1A9E75','#378ADD','#E69B00','#9B59B6','#E74C3C'];
  list.forEach((p, i) => {
    const div = document.createElement('div');
    div.className = 'proj-item' + (currentProject === p.name ? ' active' : '');
    div.innerHTML = `<span class="proj-dot" style="background:${colors[i%colors.length]}"></span>${p.name}`;
    div.onclick = () => selectProject(p.name);
    el.appendChild(div);
  });
}

async function selectProject(name) {
  currentProject = name;
  document.querySelectorAll('.proj-item').forEach(el => {
    el.classList.toggle('active', el.textContent.trim() === name);
  });

  document.getElementById('topbar-project-name').textContent = name;
  ['btn-trigger','btn-run-dlt','btn-run-dbt','btn-deploy'].forEach(id => {
    document.getElementById(id).disabled = false;
  });

  await loadProjectConfig(name);
  await loadDagRuns(name + '_pipeline');
  if (document.getElementById('panel-dbt').classList.contains('active')) loadModels();
}

async function loadProjectConfig(name) {
  const cfg = await api(`/api/projects/${name}`);
  if (cfg.error) return;

  // Populate Airflow tab
  setValue('af-src-host',    cfg.source_host || '');
  setValue('af-src-port',    cfg.source_port || 1521);
  setValue('af-src-service', cfg.source_service || '');
  setValue('af-src-schema',  cfg.source_schema || '');
  setValue('af-wh-host',     cfg.warehouse_host || '');
  setValue('af-wh-port',     cfg.warehouse_port || 5432);
  setValue('af-wh-db',       cfg.warehouse_db || '');

  // Trigger
  const t = cfg.trigger || {};
  selectTriggerValue(t.trigger_type || 'sql');
  renderTriggerConfig(t.trigger_type || 'sql', t);

  // DLT tables
  renderTables(cfg.tables || []);

  // DBT vars
  vars = {};
  const varsCfg = await api(`/api/projects/${name}/dbt/vars`);
  vars = varsCfg.vars || {};
  renderVars();
}

// ══════════════════════════════════════════════════════════
// AIRFLOW
// ══════════════════════════════════════════════════════════
async function checkAirflow() {
  const dot  = document.getElementById('airflow-status-dot');
  const text = document.getElementById('airflow-status-text');
  const dags = await api('/api/airflow/dags');

  if (Array.isArray(dags)) {
    dot.className = 'status-dot ok';
    text.textContent = `Airflow — ${dags.length} DAG${dags.length !== 1 ? 's' : ''}`;
  } else {
    dot.className = 'status-dot err';
    text.textContent = 'Airflow unreachable';
  }
}

async function loadDagRuns(dagId) {
  const runs = await api(`/api/airflow/dags/${dagId}/runs`);
  const el   = document.getElementById('dag-runs-list');

  if (!Array.isArray(runs) || !runs.length) {
    el.innerHTML = '<p class="empty-state">No runs yet for this project.</p>';
    document.getElementById('topbar-dag-status').textContent = 'No runs yet';
    return;
  }

  el.innerHTML = runs.map(r => {
    const stateClass = `state-${r.state || 'queued'}`;
    const dt = r.execution_date ? new Date(r.execution_date).toLocaleString() : '—';
    return `<div class="run-row">
      <span class="run-id">${r.dag_run_id || r.run_id || '—'}</span>
      <span>${dt}</span>
      <span class="run-state ${stateClass}">${r.state || 'unknown'}</span>
    </div>`;
  }).join('');

  const last = runs[0];
  document.getElementById('topbar-dag-status').textContent =
    `Last run: ${last.state || 'unknown'}`;
}

async function triggerDag() {
  if (!currentProject) return;
  const dagId = currentProject + '_pipeline';
  const result = await api(`/api/airflow/trigger/${dagId}`, 'POST');
  appendLog(`[TRIGGER] ${dagId}: ${result.status || result.error}`);
  setTimeout(() => loadDagRuns(dagId), 2000);
}

// ── TRIGGER CONFIG UI ──────────────────────────────────────
function selectTrigger(type, el) {
  currentTrigger = type;
  document.querySelectorAll('.trigger-opt').forEach(o => o.classList.remove('selected'));
  el.classList.add('selected');
  renderTriggerConfig(type);
}

function selectTriggerValue(type) {
  currentTrigger = type;
  document.querySelectorAll('.trigger-opt').forEach(o => {
    const val = o.querySelector('input[type=radio]')?.value;
    o.classList.toggle('selected', val === type);
  });
}

function renderTriggerConfig(type, existing = {}) {
  const el = document.getElementById('trigger-config');
  const configs = {
    sql: `
      <div class="trigger-config-box">
        <div class="card-title">SQL sensor configuration</div>
        <div class="field"><label>Control table name</label>
          <input id="t-ctrl-table" value="${existing.trigger_sql ? '' : 'load_status'}" oninput="rebuildSensorSQL()"/>
        </div>
        <div class="inline-fields">
          <div class="field"><label>Status column</label>
            <input id="t-ctrl-col" value="status" oninput="rebuildSensorSQL()"/>
          </div>
          <div class="field"><label>Completion value</label>
            <input id="t-ctrl-val" value="COMPLETE" oninput="rebuildSensorSQL()"/>
          </div>
        </div>
        <div class="field"><label>Poll interval (seconds)</label>
          <input id="t-poll" type="number" value="10"/>
        </div>
        <div class="field"><label>Sensor SQL — editable</label>
          <textarea id="t-sql" style="min-height:70px">${existing.trigger_sql || "SELECT COUNT(*) FROM load_status\nWHERE status = 'COMPLETE' AND processed = 0"}</textarea>
        </div>
        <div class="field"><label>Cleanup SQL (runs after pipeline)</label>
          <textarea id="t-cleanup" style="min-height:50px">${existing.cleanup_sql || "UPDATE load_status SET processed=1 WHERE status='COMPLETE' AND processed=0"}</textarea>
        </div>
      </div>`,
    file: `
      <div class="trigger-config-box">
        <div class="card-title">File sensor configuration</div>
        <div class="field"><label>Snapshots folder path</label>
          <input id="t-flag-path" value="${existing.flag_path || '/mnt/snapshots'}"/>
        </div>
        <div class="inline-fields">
          <div class="field"><label>Poll interval (sec)</label>
            <input type="number" value="10"/>
          </div>
          <div class="field"><label>Timeout (hours)</label>
            <input type="number" value="12"/>
          </div>
        </div>
      </div>`,
    schedule: `
      <div class="trigger-config-box">
        <div class="card-title">Schedule configuration</div>
        <div class="field"><label>Cron expression</label>
          <input id="t-cron" value="${existing.cron_expression || '0 2 * * *'}"/>
        </div>
        <div class="field"><label>Timezone</label>
          <input value="Africa/Cairo"/>
        </div>
        <p class="hint" style="margin-top:4px">Runs at 02:00 Cairo time every day.</p>
      </div>`,
    custom: `
      <div class="trigger-config-box">
        <div class="card-title">Custom trigger <span class="badge-code">Python</span></div>
        <p class="hint" style="margin-bottom:10px">
          Use variable name <code>wait</code> — the factory wires it into the DAG automatically.
        </p>
        <div class="code-editor">
          <div class="code-header">
            <span class="code-filename">custom_trigger.py</span>
            <div style="display:flex;gap:6px">
              <button class="btn btn-ghost btn-sm" onclick="insertTriggerSnippet('http')">HTTP</button>
              <button class="btn btn-ghost btn-sm" onclick="insertTriggerSnippet('kafka')">Kafka</button>
              <button class="btn btn-ghost btn-sm" onclick="insertTriggerSnippet('s3')">S3</button>
              <button class="btn btn-ghost btn-sm" onclick="insertTriggerSnippet('rowcount')">Row count</button>
            </div>
          </div>
          <textarea class="code-textarea" id="t-custom-code">${existing.custom_code || '# Write your Airflow sensor here.\n# Variable name must be: wait\n'}</textarea>
        </div>
      </div>`
  };
  el.innerHTML = configs[type] || '';
}

function rebuildSensorSQL() {
  const t = document.getElementById('t-ctrl-table')?.value || 'load_status';
  const c = document.getElementById('t-ctrl-col')?.value   || 'status';
  const v = document.getElementById('t-ctrl-val')?.value   || 'COMPLETE';
  const el = document.getElementById('t-sql');
  if (el) el.value = `SELECT COUNT(*) FROM ${t}\nWHERE ${c} = '${v}' AND processed = 0`;
}

const triggerSnippets = {
  http: `from airflow.providers.http.sensors.http import HttpSensor\n\nwait = HttpSensor(\n    task_id="wait_for_api",\n    http_conn_id="client_api",\n    endpoint="/pipeline/status",\n    response_check=lambda r: r.json()["status"] == "ready",\n    poke_interval=30, timeout=43200, mode="poke",\n)`,
  kafka: `from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor\n\nwait = AwaitMessageSensor(\n    task_id="wait_for_kafka",\n    topics=["data.pipeline.ready"],\n    kafka_config_id="kafka_default",\n    apply_function=lambda msg: msg.value().get("status") == "COMPLETE",\n    poke_interval=10, timeout=43200,\n)`,
  s3: `from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor\n\nwait = S3KeySensor(\n    task_id="wait_for_s3",\n    bucket_name="my-bucket",\n    bucket_key="signals/READY_FLAG_{{ ds_nodash }}",\n    aws_conn_id="aws_default",\n    poke_interval=30, timeout=43200,\n)`,
  rowcount: `from airflow.providers.common.sql.sensors.sql import SqlSensor\n\nwait = SqlSensor(\n    task_id="wait_for_row_count",\n    conn_id="replica_oracle_conn",\n    sql="SELECT CASE WHEN COUNT(*) > 100000 THEN 1 ELSE 0 END FROM orders WHERE created_at >= TRUNC(SYSDATE)",\n    poke_interval=60, timeout=43200, mode="poke",\n)`
};

function insertTriggerSnippet(type) {
  const el = document.getElementById('t-custom-code');
  if (el) el.value = triggerSnippets[type];
}

// ══════════════════════════════════════════════════════════
// DLT — TABLES
// ══════════════════════════════════════════════════════════
function renderTables(tables) {
  const el = document.getElementById('table-list');
  el.innerHTML = '';

  if (!tables.length) {
    el.innerHTML = '<p class="empty-state">No tables configured. Click + Add table.</p>';
    return;
  }

  tables.forEach((tbl, i) => {
    const delta = tbl.delta || 'auto';
    const badgeClass = { auto:'badge-auto', timestamp:'badge-ts', hash:'badge-hash', custom:'badge-custom' }[delta] || 'badge-auto';

    const row = document.createElement('div');
    row.className = 'tbl-row';
    row.id = `tbl-${i}`;
    row.innerHTML = `
      <div class="tbl-row-head" onclick="toggleTableRow(${i})">
        <span class="tbl-name">${tbl.name}</span>
        <span class="badge ${badgeClass}" id="tbl-badge-${i}">${delta}</span>
        <span style="color:var(--muted);font-size:12px" id="tbl-chev-${i}">▼</span>
      </div>
      <div class="tbl-row-body" id="tbl-body-${i}">
        ${renderDeltaOpts(i, tbl)}
        <div id="tbl-config-${i}">
          ${renderDeltaConfig(i, delta, tbl)}
        </div>
      </div>`;
    el.appendChild(row);
  });
}

function renderDeltaOpts(i, tbl) {
  const delta = tbl.delta || 'auto';
  const opts = ['auto','timestamp','hash','custom'];
  const descs = {
    auto:      'Auto-detect at runtime',
    timestamp: 'Cursor on timestamp column',
    hash:      'Full row hash comparison',
    custom:    'Write your own DLT logic'
  };
  return `<div class="delta-opts">
    ${opts.map(o => `
      <label class="delta-opt ${delta===o?'selected':''}" onclick="selectDeltaOpt(${i},'${o}',this)">
        <input type="radio" name="delta-${i}" value="${o}" ${delta===o?'checked':''}>
        <div><div class="delta-opt-title">${o.charAt(0).toUpperCase()+o.slice(1)}</div>
        <div class="delta-opt-desc">${descs[o]}</div></div>
      </label>`).join('')}
  </div>`;
}

function renderDeltaConfig(i, delta, tbl) {
  if (delta === 'auto') {
    return `<div class="inline-fields">
      <div class="field"><label>Primary key</label>
        <input id="tbl-pk-${i}" value="${tbl.pk||tbl.primary_key||'id'}"/>
      </div>
      <div class="field"><label>Cursor candidates (comma-separated)</label>
        <input id="tbl-cands-${i}" value="${(tbl.cursor_candidates||['updated_at','created_at']).join(', ')}"/>
      </div>
    </div>`;
  }
  if (delta === 'timestamp') {
    return `<div class="inline-fields">
      <div class="field"><label>Cursor column</label>
        <input id="tbl-cursor-${i}" value="${tbl.cursor||'updated_at'}"/>
      </div>
      <div class="field"><label>Primary key</label>
        <input id="tbl-pk-${i}" value="${tbl.pk||tbl.primary_key||'id'}"/>
      </div>
    </div>`;
  }
  if (delta === 'hash') {
    return `<div class="inline-fields">
      <div class="field"><label>Primary key</label>
        <input id="tbl-pk-${i}" value="${tbl.pk||tbl.primary_key||'id'}"/>
      </div>
      <div class="field"><label>Hash columns (blank = all)</label>
        <input id="tbl-hash-cols-${i}" value="${tbl.hash_columns||''}"/>
      </div>
    </div>`;
  }
  if (delta === 'custom') {
    const deltaSnippets = {
      composite: `# Composite primary key\nsrc.${tbl.name}.apply_hints(\n    incremental=Incremental(cursor_path="updated_at"),\n    primary_key=("order_id", "line_item_id"),\n)`,
      sequence:  `# Sequence number cursor\nsrc.${tbl.name}.apply_hints(\n    incremental=Incremental(cursor_path="sequence_id", initial_value=0),\n    primary_key="sequence_id",\n)`,
      cdclog:    `# CDC log table\n@dlt.resource(name="${tbl.name}", primary_key="id", write_disposition="merge")\ndef ${tbl.name}_from_cdc():\n    yield from conn.execute(\n        "SELECT * FROM ${tbl.name}_cdc_log WHERE change_ts > :c",\n        c=incremental.last_value\n    )`,
    };
    return `
      <div class="code-editor" style="margin-top:4px">
        <div class="code-header">
          <span class="code-filename">custom_delta_${i}.py</span>
          <div style="display:flex;gap:6px">
            <button class="btn btn-ghost btn-sm" onclick="insertDeltaSnippet(${i},'composite')">Composite key</button>
            <button class="btn btn-ghost btn-sm" onclick="insertDeltaSnippet(${i},'sequence')">Sequence num</button>
            <button class="btn btn-ghost btn-sm" onclick="insertDeltaSnippet(${i},'cdclog')">CDC log</button>
          </div>
        </div>
        <textarea class="code-textarea" id="tbl-custom-${i}" style="min-height:140px">${tbl.custom_code||'# Write your DLT extraction logic here.\n# Variable: src\n'}</textarea>
      </div>`;
  }
  return '';
}

const _deltaSnippets = {
  composite: (name) => `# Composite primary key\nsrc.${name}.apply_hints(\n    incremental=Incremental(cursor_path="updated_at"),\n    primary_key=("order_id", "line_item_id"),\n)`,
  sequence:  (name) => `# Sequence number cursor\nsrc.${name}.apply_hints(\n    incremental=Incremental(cursor_path="sequence_id", initial_value=0),\n    primary_key="sequence_id",\n)`,
  cdclog:    (name) => `# CDC log table\n@dlt.resource(name="${name}", primary_key="id", write_disposition="merge")\ndef ${name}_from_cdc():\n    yield from conn.execute("SELECT * FROM ${name}_cdc WHERE change_ts > :c", c=incremental.last_value)`,
};
function insertDeltaSnippet(i, type) {
  const tblName = document.querySelector(`#tbl-${i} .tbl-name`)?.textContent || 'table';
  const el = document.getElementById(`tbl-custom-${i}`);
  if (el) el.value = _deltaSnippets[type]?.(tblName) || '';
}

function selectDeltaOpt(i, delta, labelEl) {
  const parent = labelEl.closest('.delta-opts');
  parent.querySelectorAll('.delta-opt').forEach(o => o.classList.remove('selected'));
  labelEl.classList.add('selected');

  const badgeEl = document.getElementById(`tbl-badge-${i}`);
  const classes = { auto:'badge-auto', timestamp:'badge-ts', hash:'badge-hash', custom:'badge-custom' };
  badgeEl.className = `badge ${classes[delta]||'badge-auto'}`;
  badgeEl.textContent = delta;

  const tblName = document.querySelector(`#tbl-${i} .tbl-name`)?.textContent || '';
  document.getElementById(`tbl-config-${i}`).innerHTML =
    renderDeltaConfig(i, delta, { name: tblName });
}

function toggleTableRow(i) {
  const body = document.getElementById(`tbl-body-${i}`);
  const chev = document.getElementById(`tbl-chev-${i}`);
  const open = body.classList.toggle('open');
  chev.textContent = open ? '▲' : '▼';
}

function addTable() {
  const name = prompt('Table name:');
  if (!name) return;
  const el = document.getElementById('table-list');
  const i  = el.children.length;
  const tbl = { name, delta: 'auto', cursor_candidates: ['updated_at','created_at'], pk: 'id' };
  const tempDiv = document.createElement('div');
  tempDiv.innerHTML = `<div class="tbl-row" id="tbl-${i}">
    <div class="tbl-row-head" onclick="toggleTableRow(${i})">
      <span class="tbl-name">${name}</span>
      <span class="badge badge-auto" id="tbl-badge-${i}">auto</span>
      <span style="color:var(--muted);font-size:12px" id="tbl-chev-${i}">▼</span>
    </div>
    <div class="tbl-row-body" id="tbl-body-${i}">
      ${renderDeltaOpts(i, tbl)}
      <div id="tbl-config-${i}">${renderDeltaConfig(i,'auto',tbl)}</div>
    </div>
  </div>`;
  el.appendChild(tempDiv.firstElementChild);
  toggleTableRow(i);
}

// ══════════════════════════════════════════════════════════
// DBT — VARS
// ══════════════════════════════════════════════════════════
function renderVars() {
  const el = document.getElementById('vars-list');
  el.innerHTML = Object.entries(vars).map(([k,v]) => `
    <div class="vars-row">
      <input value="${k}" onchange="renameVar('${k}',this.value)" placeholder="variable name"/>
      <input value="${v}" onchange="vars['${k}']=this.value" placeholder="value"/>
      <button class="vars-del" onclick="deleteVar('${k}')">✕</button>
    </div>`).join('');
}

function addVar() {
  const key = `new_var_${Object.keys(vars).length+1}`;
  vars[key] = '';
  renderVars();
}

function deleteVar(key) { delete vars[key]; renderVars(); }

function renameVar(oldKey, newKey) {
  if (oldKey === newKey) return;
  vars[newKey] = vars[oldKey];
  delete vars[oldKey];
}

async function saveVars() {
  if (!currentProject) return alert('Select a project first');
  const result = await api(`/api/projects/${currentProject}/dbt/vars`, 'POST', { vars });
  appendLog(`[DBT VARS] Saved: ${JSON.stringify(result)}`);
}

// ══════════════════════════════════════════════════════════
// DBT — MODELS
// ══════════════════════════════════════════════════════════
async function loadModels() {
  if (!currentProject) return;
  const models = await api(`/api/projects/${currentProject}/dbt/models`);
  const tabsEl = document.getElementById('model-tabs');
  tabsEl.innerHTML = '';

  if (!Array.isArray(models) || !models.length) {
    tabsEl.innerHTML = '<p class="empty-state" style="width:100%">No models yet.</p>';
    return;
  }

  models.forEach(m => {
    const btn = document.createElement('button');
    btn.className = 'model-tab';
    btn.textContent = m.name;
    btn.title = `${m.schema}/${m.file}`;
    btn.onclick = () => openModel(m.schema, m.name, btn);
    tabsEl.appendChild(btn);
  });
}

async function openModel(schema, name, btn) {
  currentModel = { schema, name };
  document.querySelectorAll('.model-tab').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');

  const data = await api(`/api/projects/${currentProject}/dbt/models/${schema}/${name}`);
  document.getElementById('model-editor-wrap').innerHTML = `
    <div class="code-editor">
      <div class="code-header">
        <span class="code-filename">models/${schema}/${name}.sql</span>
        <div style="display:flex;gap:6px">
          <button class="btn btn-ghost btn-sm" onclick="saveCurrentModel()">Save</button>
          <button class="btn btn-ghost btn-sm" onclick="runDbtManual()">Run DBT</button>
        </div>
      </div>
      <textarea class="code-textarea" id="current-model-sql">${data.sql || ''}</textarea>
    </div>`;
}

async function saveCurrentModel() {
  if (!currentModel || !currentProject) return;
  const sql = document.getElementById('current-model-sql')?.value || '';
  const result = await api(
    `/api/projects/${currentProject}/dbt/models/${currentModel.schema}/${currentModel.name}`,
    'POST', { sql }
  );
  appendLog(`[DBT MODEL] Saved ${currentModel.name}: ${result.status}`);
}

async function addModel() {
  const name = prompt('Model name (without .sql):');
  if (!name) return;
  const schema = prompt('Schema: staging or marts?', 'marts');
  if (!schema) return;

  const defaultSQL = schema === 'marts'
    ? `{{ config(materialized='incremental', unique_key='id', incremental_strategy='delete+insert') }}\n\nSELECT\n    *\nFROM {{ ref('stg_orders') }}\n\n{% if is_incremental() %}\nWHERE updated_at > (SELECT COALESCE(MAX(updated_at),'1970-01-01'::timestamptz) FROM {{ this }})\n{% endif %}`
    : `{{ config(materialized='view') }}\n\nSELECT\n    *\nFROM {{ source('staging', '${name}') }}`;

  await api(`/api/projects/${currentProject}/dbt/models/${schema}/${name}`, 'POST', { sql: defaultSQL });
  appendLog(`[DBT] Created model: ${schema}/${name}.sql`);
  loadModels();
}

// ══════════════════════════════════════════════════════════
// SAVE + DEPLOY
// ══════════════════════════════════════════════════════════
async function deployProject() {
  if (!currentProject) return;

  appendLog(`[DEPLOY] Saving project config for ${currentProject}...`);

  // Build config from UI
  const config = buildConfigFromUI();
  await api(`/api/projects/${currentProject}`, 'POST', config);
  appendLog(`[DEPLOY] Config saved.`);

  // Deploy (add connections + restart Airflow)
  appendLog(`[DEPLOY] Restarting Airflow and registering connections...`);
  const result = await api(`/api/deploy/${currentProject}`, 'POST');
  appendLog(`[DEPLOY] Done: ${JSON.stringify(result.results || result)}`);

  switchTab('logs');
}

function buildConfigFromUI() {
  const trig = buildTriggerFromUI();
  const tables = buildTablesFromUI();
  return {
    project_name:    currentProject,
    source_host:     val('af-src-host'),
    source_port:     parseInt(val('af-src-port')) || 1521,
    source_service:  val('af-src-service'),
    source_schema:   val('af-src-schema'),
    warehouse_host:  val('af-wh-host'),
    warehouse_port:  parseInt(val('af-wh-port')) || 5432,
    warehouse_db:    val('af-wh-db'),
    replica_conn_id:   currentProject + '_oracle_replica',
    warehouse_conn_id: currentProject + '_warehouse_pg',
    trigger: trig,
    tables,
  };
}

function buildTriggerFromUI() {
  const type = currentTrigger;
  if (type === 'sql') return {
    trigger_type: 'sql',
    trigger_sql: val('t-sql') || "SELECT COUNT(*) FROM load_status WHERE status='COMPLETE' AND processed=0",
    cleanup_sql: val('t-cleanup') || "UPDATE load_status SET processed=1 WHERE status='COMPLETE' AND processed=0",
  };
  if (type === 'file') return {
    trigger_type: 'file',
    flag_path: val('t-flag-path') || '/mnt/snapshots',
  };
  if (type === 'schedule') return {
    trigger_type: 'schedule',
    cron_expression: val('t-cron') || '0 2 * * *',
  };
  if (type === 'custom') return {
    trigger_type: 'custom',
    custom_code: val('t-custom-code') || '',
  };
  return { trigger_type: type };
}

function buildTablesFromUI() {
  const rows = document.querySelectorAll('.tbl-row');
  const tables = [];
  rows.forEach((row, i) => {
    const nameEl = row.querySelector('.tbl-name');
    const name   = nameEl ? nameEl.textContent.trim() : '';
    if (!name || name === '+ add table') return; // skip empty rows
    const badge = document.getElementById('tbl-badge-' + i)?.textContent?.trim() || 'auto';
    const table = {
      name,
      delta: badge,
      pk: val('tbl-pk-' + i) || 'id',
    };
    if (badge === 'timestamp') table.cursor = val('tbl-cursor-' + i) || 'updated_at';
    if (badge === 'auto')      table.cursor_candidates = (val('tbl-cands-' + i) || 'updated_at,created_at').split(',').map(s=>s.trim());
    if (badge === 'hash')      table.hash_columns = val('tbl-hash-cols-' + i) || '';
    if (badge === 'custom')    table.custom_code = val('tbl-custom-' + i) || '';
    tables.push(table);
  });
  return tables;
}

// ══════════════════════════════════════════════════════════
// MANUAL RUN
// ══════════════════════════════════════════════════════════
async function runDltManual() {
  if (!currentProject) return;
  appendLog(`[DLT] Starting manual run for ${currentProject}...`);
  switchTab('logs');
  const result = await api(`/api/run_dlt/${currentProject}`, 'POST');
  appendLog(result.stdout || result.error || 'Done');
  if (result.returncode !== 0) appendLog(`[ERROR] ${result.stderr}`);
}

async function runDbtManual() {
  if (!currentProject) return;
  // Save current model first if open
  if (currentModel) await saveCurrentModel();
  appendLog(`[DBT] Starting manual run for ${currentProject}...`);
  switchTab('logs');
  const result = await api(`/api/run_dbt/${currentProject}`, 'POST');
  appendLog(result.stdout || result.error || 'Done');
  if (result.returncode !== 0) appendLog(`[ERROR] ${result.stderr}`);
}

// ══════════════════════════════════════════════════════════
// NEW PROJECT MODAL
// ══════════════════════════════════════════════════════════
function openNewProjectModal() {
  document.getElementById('new-project-modal').style.display = 'flex';
}
function closeModal() {
  document.getElementById('new-project-modal').style.display = 'none';
}

async function createProject() {
  const name = val('np-name').trim().toLowerCase().replace(/\s+/g,'_');
  if (!name) return alert('Enter a project name');

  const config = {
    project_name:    name,
    source_host:     val('np-src-host'),
    source_port:     parseInt(val('np-src-port')) || 1521,
    source_service:  val('np-src-service'),
    source_schema:   val('np-src-schema'),
    warehouse_host:  val('np-wh-host'),
    warehouse_port:  parseInt(val('np-wh-port')) || 5432,
    warehouse_db:    val('np-wh-db'),
    replica_conn_id:   name + '_oracle_replica',
    warehouse_conn_id: name + '_warehouse_pg',
    trigger: { trigger_type: 'sql',
      trigger_sql: "SELECT COUNT(*) FROM load_status WHERE status='COMPLETE' AND processed=0",
      cleanup_sql: "UPDATE load_status SET processed=1 WHERE status='COMPLETE' AND processed=0"
    },
    tables: [],
  };

  await api(`/api/projects/${name}`, 'POST', config);
  appendLog(`[PROJECT] Created: ${name}`);
  closeModal();
  await loadProjects();
  selectProject(name);
}

// ══════════════════════════════════════════════════════════
// LOGS
// ══════════════════════════════════════════════════════════
function appendLog(msg) {
  const el = document.getElementById('log-output');
  const ts = new Date().toLocaleTimeString();
  el.textContent += `[${ts}] ${msg}\n`;
  el.scrollTop = el.scrollHeight;
}

function clearLogs() {
  document.getElementById('log-output').textContent = '';
}

// ══════════════════════════════════════════════════════════
// UTILS
// ══════════════════════════════════════════════════════════
function val(id)         { return document.getElementById(id)?.value || ''; }
function setValue(id, v) { const el=document.getElementById(id); if(el) el.value=v; }

async function api(path, method='GET', body=null) {
  try {
    const opts = { method, headers: {'Content-Type':'application/json'} };
    if (body) opts.body = JSON.stringify(body);
    const r = await fetch(API + path, opts);
    return await r.json();
  } catch(e) {
    return { error: e.message };
  }
}
