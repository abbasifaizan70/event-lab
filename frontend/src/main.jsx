import React, { useEffect, useState } from 'react';
import { createRoot } from 'react-dom/client';
import './style.css';

function App() {
  const [text, setText] = useState('Hello from React. Django will count the words in this report.');
  const [reports, setReports] = useState([]);
  const [connection, setConnection] = useState('Connecting');
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  async function refresh() {
    const response = await fetch('/api/reports');
    if (!response.ok) throw new Error('Could not load reports. Check that Spring Boot is running.');
    setReports(await response.json());
  }
  useEffect(() => {
    const reload = () => refresh().catch(error => setError(error.message));
    reload();
    const events = new EventSource('/api/reports/events');
    events.addEventListener('ready', () => { setConnection('Live'); reload(); });
    events.addEventListener('report-updated', reload);
    events.onerror = () => setConnection('Reconnecting');
    // A reconciliation read covers missed notifications while disconnected.
    const timer = setInterval(reload, 15000);
    return () => { events.close(); clearInterval(timer); };
  }, []);
  async function submit(event) {
    event.preventDefault(); setBusy(true); setError('');
    try {
      const response = await fetch('/api/reports', {
        method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({text}),
      });
      if (!response.ok) throw new Error('Request could not be confirmed. Check the list and backend logs before trying again.');
      await refresh();
    } catch (error) { setError(error.message); await refresh().catch(() => {}); }
    finally { setBusy(false); }
  }
  return <main>
    <header><span className="brand">◈ EVENT LAB</span><span className="connection">● {connection}</span></header>
    <p className="eyebrow">A SMALL DISTRIBUTED SYSTEM</p>
    <h1>Follow a request.<br/><span>Understand every step.</span></h1>
    <p className="intro">Submit some text. A separate Django worker makes a word-count report, then Spring Boot sends the update back here.</p>
    <div className="flow" aria-label="Architecture">React <b>→</b> Spring Boot <b>→</b> Kafka <b>→</b> Django <b>→</b> Kafka <b>→</b> Spring <b>→</b> React</div>
    <div className="columns"><section className="card">
      <p className="eyebrow">01 / SEND A REQUEST</p><h2>Your report</h2>
      <form onSubmit={submit}><label htmlFor="text">Text to analyse</label>
        <textarea id="text" value={text} onChange={e => setText(e.target.value)} maxLength={5000} required rows={6}/>
        <div className="form-footer"><small>{text.length} / 5,000 characters</small><button disabled={busy || !text.trim()}>{busy ? 'Submitting…' : 'Generate report →'}</button></div>
      </form>{error && <p role="alert" className="error">{error}</p>}
      <p className="note">The API returns <code>202 Accepted</code>. That means the work was accepted; the report can still be queued.</p>
    </section><aside className="card explainer"><p className="eyebrow">02 / WATCH IT TRAVEL</p><h2>What happens next?</h2>
      <ol><li>Hibernate saves a <strong>QUEUED</strong> row in PostgreSQL.</li><li>Spring publishes <code>ReportRequested</code>.</li><li>Django counts the words and publishes <code>ReportCompleted</code>.</li><li>Spring saves the result and sends an SSE notification.</li><li>React fetches the saved result and renders it.</li></ol>
    </aside></div>
    <section className="results"><div className="result-heading"><h2>Reports <small>{reports.length}</small></h2><button className="secondary" onClick={() => refresh().catch(e => setError(e.message))}>Refresh</button></div>
      {reports.length === 0 && <p className="empty">Your first report will appear here. Try submitting the sample text above.</p>}
      <div aria-live="polite">{reports.map(report => <article className="report" key={report.id}>
        <div><span className={'badge ' + report.status.toLowerCase()}>{report.status}</span><p className="report-text">{report.text}</p><code className="id">{report.id}</code></div>
        <div className="counts">{report.status === 'COMPLETED' ? <><strong>{report.wordCount}<small>words</small></strong><strong>{report.characterCount}<small>characters</small></strong></> : <span>Waiting for the worker…</span>}</div>
      </article>)}</div>
    </section><footer>PostgreSQL stores the truth. Kafka carries the events. SSE carries browser notifications.</footer>
  </main>;
}
createRoot(document.getElementById('root')).render(<App/>);
