import { useEffect, useState } from "react";
import TickerTape from "./components/TickerTape.jsx";
import PriceChart from "./components/PriceChart.jsx";
import TradeFeed from "./components/TradeFeed.jsx";
import NewsFeed from "./components/NewsFeed.jsx";
import RetailFeed from "./components/RetailFeed.jsx";
import HospitalityFeed from "./components/HospitalityFeed.jsx";
import GLReconciliation from "./components/GLReconciliation.jsx";
import LoginPanel from "./components/LoginPanel.jsx";
import { getToken, clearToken, getMe } from "./api.js";

const TRACKED_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"];

export default function App() {
  const [symbol, setSymbol] = useState(TRACKED_SYMBOLS[0]);
  const [orgName, setOrgName] = useState(null); // null = not authenticated
  const [industry, setIndustry] = useState(null); // 'retail' | 'hospitality' | 'crypto' | 'general'
  const [checkingAuth, setCheckingAuth] = useState(true);

  const isAuthed = !!orgName;
  // Before login we don't know the org's industry yet, so default to
  // showing crypto - it's public data anyway, nothing to gate. Once
  // logged in, only show it if this org actually cares about it.
  const wantsCrypto = !isAuthed || industry === "crypto" || industry === "general";
  const wantsRetail = isAuthed && (industry === "retail" || industry === "general");
  const wantsHospitality = isAuthed && (industry === "hospitality" || industry === "general");

  useEffect(() => {
    const token = getToken();
    if (!token) {
      setCheckingAuth(false);
      return;
    }
    getMe()
      .then((me) => {
        setOrgName(me.org_name);
        setIndustry(me.industry);
      })
      .catch(() => clearToken())
      .finally(() => setCheckingAuth(false));
  }, []);

  function handleAuthenticated({ orgName, industry }) {
    setOrgName(orgName);
    setIndustry(industry);
  }

  function handleLogout() {
    clearToken();
    setOrgName(null);
    setIndustry(null);
  }

  return (
    <div className="app">
      <header className="app-header">
        <div className="app-title">
          <span className="app-title-mark">◆</span>
          <span>PIPELINE</span>
          <span className="app-title-sub mono">/ market feed</span>
        </div>
        <div className="app-status">
          {orgName && (
            <span className="org-badge mono">
              {orgName}
              <button className="logout-btn" onClick={handleLogout} title="Log out">
                ×
              </button>
            </span>
          )}
          <span className="status-dot" />
          <span className="mono">LIVE</span>
        </div>
      </header>

      {wantsCrypto && (
        <>
          <TickerTape />
          <nav className="symbol-tabs">
            {TRACKED_SYMBOLS.map((s) => (
              <button
                key={s}
                className={`symbol-tab mono ${s === symbol ? "active" : ""}`}
                onClick={() => setSymbol(s)}
              >
                {s}
              </button>
            ))}
          </nav>
          <main className="app-grid">
            <PriceChart symbol={symbol} />
            <TradeFeed symbol={symbol} />
          </main>
        </>
      )}

      <section className="app-section">
        <div className="app-section-label mono">
          {isAuthed ? "YOUR DATA" : "SIGN IN FOR RETAIL DATA"}
        </div>
        <div className="secondary-grid">
          <NewsFeed />
          {!checkingAuth && wantsRetail && <RetailFeed />}
          {!checkingAuth && wantsHospitality && <HospitalityFeed />}
          {!checkingAuth && !isAuthed && <LoginPanel onAuthenticated={handleAuthenticated} />}
        </div>
      </section>

      {!checkingAuth && isAuthed && (
        <section className="app-section">
          <div className="app-section-label mono">FINANCE / multi-system reconciliation</div>
          <GLReconciliation />
        </section>
      )}

      <footer className="app-footer mono">
        binance ws + hnrss poll + csv drop → kafka → postgres → fastapi → react
      </footer>
    </div>
  );
}
