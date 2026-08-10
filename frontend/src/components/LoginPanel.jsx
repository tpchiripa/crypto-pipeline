import { useState } from "react";
import { login, signup, setToken } from "../api.js";

const INDUSTRIES = [
  { value: "retail", label: "Retail business" },
  { value: "crypto", label: "Crypto trading" },
  { value: "general", label: "General / see everything" },
];

export default function LoginPanel({ onAuthenticated }) {
  const [mode, setMode] = useState("login"); // "login" | "signup"
  const [orgName, setOrgName] = useState("");
  const [industry, setIndustry] = useState("retail");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);

  async function handleSubmit(e) {
    e.preventDefault();
    setError(null);
    setLoading(true);
    try {
      const result =
        mode === "signup" ? await signup(orgName, email, password, industry) : await login(email, password);
      setToken(result.access_token);
      onAuthenticated({ orgName: result.org_name, industry: result.industry });
    } catch (err) {
      setError(err.message || "Something went wrong");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="panel login-panel">
      <div className="panel-header">
        <span className="panel-title">SIGN IN</span>
        <span className="panel-sub mono">tenant login required</span>
      </div>

      <form className="login-form" onSubmit={handleSubmit}>
        <div className="login-tabs">
          <button
            type="button"
            className={`login-tab mono ${mode === "login" ? "active" : ""}`}
            onClick={() => setMode("login")}
          >
            LOG IN
          </button>
          <button
            type="button"
            className={`login-tab mono ${mode === "signup" ? "active" : ""}`}
            onClick={() => setMode("signup")}
          >
            SIGN UP
          </button>
        </div>

        {mode === "signup" && (
          <>
            <input
              className="login-input"
              type="text"
              placeholder="Business name (e.g. Acme Retail)"
              value={orgName}
              onChange={(e) => setOrgName(e.target.value)}
              required
            />
            <select
              className="login-input login-select"
              value={industry}
              onChange={(e) => setIndustry(e.target.value)}
            >
              {INDUSTRIES.map((opt) => (
                <option key={opt.value} value={opt.value}>
                  {opt.label}
                </option>
              ))}
            </select>
          </>
        )}
        <input
          className="login-input"
          type="email"
          placeholder="Email"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          required
        />
        <input
          className="login-input"
          type="password"
          placeholder="Password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          required
          minLength={8}
        />

        {error && <div className="login-error mono">{error}</div>}

        <button type="submit" className="login-submit mono" disabled={loading}>
          {loading ? "…" : mode === "signup" ? "Create account" : "Log in"}
        </button>
      </form>
    </div>
  );
}
