import { usePolling } from "../hooks/usePolling.js";
import { getNews } from "../api.js";

function timeAgo(isoString) {
  const seconds = Math.max(0, (Date.now() - new Date(isoString).getTime()) / 1000);
  if (seconds < 60) return `${seconds.toFixed(0)}s ago`;
  if (seconds < 3600) return `${(seconds / 60).toFixed(0)}m ago`;
  return `${(seconds / 3600).toFixed(0)}h ago`;
}

export default function NewsFeed() {
  const { data, error } = usePolling(() => getNews(15), 10000);

  return (
    <div className="panel news-feed">
      <div className="panel-header">
        <span className="panel-title">NEWS FEED</span>
        <span className="panel-sub mono">hnrss · crypto</span>
      </div>

      {error && <div className="feed-error mono">feed unavailable — retrying…</div>}

      <div className="news-rows">
        {(data || []).map((article) => (
          <a
            key={article.article_id}
            href={article.link}
            target="_blank"
            rel="noopener noreferrer"
            className="news-row"
          >
            <div className="news-title">{article.title}</div>
            <div className="news-meta mono">
              {article.author && <span>{article.author}</span>}
              <span>{timeAgo(article.received_at)}</span>
            </div>
          </a>
        ))}
        {!data && !error && <div className="feed-loading mono">loading articles…</div>}
        {data && data.length === 0 && (
          <div className="feed-loading mono">no articles yet — waiting on next poll…</div>
        )}
      </div>
    </div>
  );
}
