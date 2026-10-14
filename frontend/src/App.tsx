import { FormEvent, useCallback, useEffect, useMemo, useState } from 'react'

const API_URL = import.meta.env.VITE_API_URL ?? ''

type Status = { status: string; summary?: { accepted_records: number; persisted_records: number; persisted_chunks: number } }
type Chunk = { chunk_id: string; status: string; record_count: number; byte_size?: number }

const nodes = ['Generator', 'Queue', 'Processor', 'Buffer', 'Chunk Builder', 'Parquet Writer', 'Local Storage']

const request = async (path: string, init?: RequestInit) => {
  const response = await fetch(`${API_URL}${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...init,
  })
  if (!response.ok) throw new Error((await response.json()).detail ?? `Request failed (${response.status})`)
  return response.json()
}

export function App() {
  const [recordCount, setRecordCount] = useState(100)
  const [feedRate, setFeedRate] = useState(1000)
  const [status, setStatus] = useState<Status>({ status: 'idle' })
  const [chunks, setChunks] = useState<Chunk[]>([])
  const [error, setError] = useState('')
  const [connected, setConnected] = useState(false)

  const refresh = useCallback(async () => {
    try {
      const [nextStatus, nextChunks] = await Promise.all([
        request('/api/runs/status') as Promise<Status>,
        request('/api/chunks') as Promise<Chunk[]>,
      ])
      setStatus(nextStatus)
      setChunks(nextChunks)
      setConnected(true)
      setError('')
    } catch (cause) {
      setConnected(false)
      setError(cause instanceof Error ? cause.message : 'Unable to reach the backend')
    }
  }, [])

  useEffect(() => {
    void refresh()
    const timer = window.setInterval(() => void refresh(), 1000)
    return () => window.clearInterval(timer)
  }, [refresh])

  const start = async (event: FormEvent) => {
    event.preventDefault()
    try {
      await request('/api/runs/start', {
        method: 'POST',
        body: JSON.stringify({
          run_id: `run-${Date.now()}`,
          record_count: recordCount,
          settings: { feed: { records_per_second: feedRate } },
        }),
      })
      await refresh()
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'Unable to start run')
    }
  }

  const stop = async () => {
    try {
      await request('/api/runs/stop', { method: 'POST' })
      await refresh()
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'Unable to stop run')
    }
  }

  const summary = status.summary
  const displayChunks = useMemo(() => chunks.slice(0, 8), [chunks])

  return (
    <main className="shell">
      <header className="hero">
        <div>
          <p className="eyebrow">FLOWSTORE / LOCAL LAB</p>
          <h1>Time-Series Virtual Lab</h1>
          <p className="lede">Observe records move from a deterministic source to verified Parquet storage.</p>
        </div>
        <span className={`connection ${connected ? 'online' : ''}`}><i />{connected ? 'Connected' : 'Offline'}</span>
      </header>
      {error && <div className="alert" role="alert">{error}</div>}
      <section className="panel controls">
        <div className="section-title"><h2>Run configuration</h2><span>{status.status}</span></div>
        <form onSubmit={start}>
          <label>Records<input type="number" min="0" max="100000" value={recordCount} onChange={(event) => setRecordCount(Number(event.target.value))} /></label>
          <label>Feed rate / sec<input type="number" min="1" max="100000" value={feedRate} onChange={(event) => setFeedRate(Number(event.target.value))} /></label>
          <button type="submit" disabled={status.status === 'running'}>Start simulation</button>
          <button type="button" className="secondary" onClick={stop} disabled={status.status !== 'running'}>Stop</button>
        </form>
      </section>
      <section className="pipeline" aria-label="Pipeline stages">
        {nodes.map((node, index) => <div className="node" key={node}><strong>{node}</strong>{index < nodes.length - 1 && <span className="arrow">→</span>}</div>)}
      </section>
      <section className="metrics">
        {[
          ['Input records', summary?.accepted_records ?? 0],
          ['Persisted records', summary?.persisted_records ?? 0],
          ['Parquet chunks', summary?.persisted_chunks ?? chunks.length],
          ['Storage status', status.status],
        ].map(([label, value]) => <article className="metric" key={label}><span>{label}</span><strong>{value}</strong></article>)}
      </section>
      <section className="split">
        <article className="panel"><div className="section-title"><h2>Event log</h2><span>Live</span></div><p className="empty">Events will appear here as the next run progresses.</p></article>
        <article className="panel"><div className="section-title"><h2>Storage explorer</h2><span>{chunks.length} chunks</span></div>{displayChunks.length === 0 ? <p className="empty">No Parquet chunks yet.</p> : <ul className="chunk-list">{displayChunks.map((chunk) => <li key={chunk.chunk_id}><code>{chunk.chunk_id.slice(0, 12)}</code><span>{chunk.record_count} records</span><b>{chunk.status}</b></li>)}</ul>}</article>
      </section>
    </main>
  )
}
