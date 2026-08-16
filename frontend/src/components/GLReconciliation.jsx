import { useEffect, useRef, useState } from "react";
import {
  getGLAccounts,
  createGLAccount,
  getGLMappings,
  createGLMapping,
  getUnmappedCategories,
  uploadGLFile,
} from "../api.js";

function formatMoney(value) {
  if (value == null) return "—";
  return value.toLocaleString(undefined, { style: "currency", currency: "USD" });
}

const SOURCE_SYSTEMS = [
  { value: "xero", label: "Xero (source of truth)" },
  { value: "dyner", label: "Dyner" },
  { value: "lightspeed", label: "Lightspeed" },
  { value: "other", label: "Other" },
];

function UploadWidget({ onUploaded }) {
  const [sourceSystem, setSourceSystem] = useState("xero");
  const [uploading, setUploading] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);
  const fileInputRef = useRef(null);

  async function handleFileChange(e) {
    const file = e.target.files?.[0];
    if (!file) return;

    setUploading(true);
    setError(null);
    setResult(null);
    try {
      const res = await uploadGLFile(file, sourceSystem);
      setResult(res);
      onUploaded();
    } catch (err) {
      setError(err.message);
    } finally {
      setUploading(false);
      if (fileInputRef.current) fileInputRef.current.value = "";
    }
  }

  return (
    <div className="gl-upload">
      <div className="gl-upload-controls">
        <span className="gl-upload-label mono">THIS FILE IS FROM:</span>
        <select
          className="gl-input gl-input-small"
          value={sourceSystem}
          onChange={(e) => setSourceSystem(e.target.value)}
        >
          {SOURCE_SYSTEMS.map((s) => (
            <option key={s.value} value={s.value}>
              {s.label}
            </option>
          ))}
        </select>
        <label className="gl-btn gl-btn-primary gl-upload-btn">
          {uploading ? "Uploading…" : "Choose CSV file"}
          <input
            ref={fileInputRef}
            type="file"
            accept=".csv"
            onChange={handleFileChange}
            disabled={uploading}
            style={{ display: "none" }}
          />
        </label>
      </div>

      {error && <div className="gl-error mono">{error}</div>}

      {result && (
        <div className="gl-upload-result mono">
          <strong>{result.filename}</strong> ({result.source_system}) — {result.rows_ingested} row(s) ingested
          {result.auto_resolved > 0 && <> · {result.auto_resolved} auto-resolved from Xero</>}
          {result.needs_mapping > 0 && <> · {result.needs_mapping} need mapping below</>}
          {result.rows_skipped > 0 && <> · {result.rows_skipped} skipped (no category)</>}
        </div>
      )}
    </div>
  );
}

function NewAccountForm({ onCreated, onCancel }) {
  const [code, setCode] = useState("");
  const [name, setName] = useState("");
  const [type, setType] = useState("cogs");
  const [error, setError] = useState(null);
  const [saving, setSaving] = useState(false);

  async function handleCreate() {
    setError(null);
    setSaving(true);
    try {
      const account = await createGLAccount(code, name, type);
      onCreated(account);
    } catch (err) {
      setError(err.message);
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="gl-new-account-form">
      <input
        className="gl-input gl-input-small"
        placeholder="Code (5010)"
        value={code}
        onChange={(e) => setCode(e.target.value)}
      />
      <input
        className="gl-input"
        placeholder="Account name (COGS - Beverage)"
        value={name}
        onChange={(e) => setName(e.target.value)}
      />
      <select className="gl-input gl-input-small" value={type} onChange={(e) => setType(e.target.value)}>
        <option value="cogs">COGS</option>
        <option value="revenue">Revenue</option>
        <option value="expense">Expense</option>
        <option value="asset">Asset</option>
        <option value="liability">Liability</option>
      </select>
      <button className="gl-btn gl-btn-primary" onClick={handleCreate} disabled={saving || !code || !name}>
        Create
      </button>
      <button className="gl-btn" onClick={onCancel}>
        Cancel
      </button>
      {error && <span className="gl-error mono">{error}</span>}
    </div>
  );
}

function UnmappedRow({ item, accounts, onMapped, onAccountCreated }) {
  const [selectedId, setSelectedId] = useState("");
  const [showNewAccount, setShowNewAccount] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState(null);

  async function handleMap() {
    if (!selectedId) return;
    setSaving(true);
    setError(null);
    try {
      await createGLMapping(item.source_system, item.source_category, Number(selectedId));
      onMapped();
    } catch (err) {
      setError(err.message);
    } finally {
      setSaving(false);
    }
  }

  function handleAccountCreated(account) {
    onAccountCreated(account);
    setSelectedId(String(account.id));
    setShowNewAccount(false);
  }

  return (
    <div className="gl-row">
      <div className="gl-row-main">
        <span className="gl-system-tag mono">{item.source_system}</span>
        <span className="gl-category">{item.source_category}</span>
        <span className="gl-count mono">{item.transaction_count}x</span>
        <span className="gl-amount mono">{formatMoney(item.total_amount)}</span>
      </div>

      {!showNewAccount ? (
        <div className="gl-row-action">
          <select className="gl-input gl-input-small" value={selectedId} onChange={(e) => setSelectedId(e.target.value)}>
            <option value="">Map to account…</option>
            {accounts.map((a) => (
              <option key={a.id} value={a.id}>
                {a.code} — {a.name}
              </option>
            ))}
          </select>
          <button className="gl-btn gl-btn-primary" onClick={handleMap} disabled={!selectedId || saving}>
            Map
          </button>
          <button className="gl-btn-link" onClick={() => setShowNewAccount(true)}>
            + new account
          </button>
        </div>
      ) : (
        <NewAccountForm onCreated={handleAccountCreated} onCancel={() => setShowNewAccount(false)} />
      )}

      {error && <div className="gl-error mono">{error}</div>}
    </div>
  );
}

export default function GLReconciliation() {
  const [unmapped, setUnmapped] = useState(null);
  const [accounts, setAccounts] = useState([]);
  const [mappings, setMappings] = useState([]);
  const [error, setError] = useState(null);
  const [showAllMappings, setShowAllMappings] = useState(false);

  async function loadAll() {
    try {
      const [u, a, m] = await Promise.all([getUnmappedCategories(), getGLAccounts(), getGLMappings()]);
      setUnmapped(u);
      setAccounts(a);
      setMappings(m);
      setError(null);
    } catch (err) {
      setError(err.message);
    }
  }

  useEffect(() => {
    loadAll();
  }, []);

  function handleAccountCreated(account) {
    setAccounts((prev) => [...prev, account].sort((a, b) => a.code.localeCompare(b.code)));
  }

  return (
    <div className="panel gl-panel">
      <div className="panel-header">
        <span className="panel-title">GL RECONCILIATION</span>
        <span className="panel-sub mono">
          {unmapped ? `${unmapped.length} categories need mapping` : "loading…"}
        </span>
      </div>

      {error && <div className="feed-error mono">{error}</div>}

      <UploadWidget onUploaded={loadAll} />

      <div className="gl-body">
        <div className="gl-section-label mono">NEEDS MAPPING</div>
        {unmapped && unmapped.length === 0 && (
          <div className="gl-empty mono">Everything's reconciled. Drop a new export to see more here.</div>
        )}
        {(unmapped || []).map((item) => (
          <UnmappedRow
            key={`${item.source_system}-${item.source_category}`}
            item={item}
            accounts={accounts}
            onMapped={loadAll}
            onAccountCreated={handleAccountCreated}
          />
        ))}

        {mappings.length > 0 && (
          <>
            <div className="gl-section-label mono gl-section-label-mapped">
              <button className="gl-toggle" onClick={() => setShowAllMappings((v) => !v)}>
                {showAllMappings ? "▾" : "▸"} RECONCILED ({mappings.length})
              </button>
            </div>
            {showAllMappings &&
              mappings.map((m) => (
                <div key={m.id} className="gl-mapped-row mono">
                  <span className="gl-system-tag">{m.source_system}</span>
                  <span>{m.source_category}</span>
                  <span className="gl-arrow">→</span>
                  <span className="gl-mapped-target">
                    {m.gl_account_code} {m.gl_account_name}
                  </span>
                </div>
              ))}
          </>
        )}
      </div>
    </div>
  );
}
