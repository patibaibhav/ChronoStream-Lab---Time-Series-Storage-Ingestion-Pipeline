import { FormEvent, useCallback, useEffect, useMemo, useState } from 'react'

const API_URL = import.meta.env.VITE_API_URL ?? ''

type Metrics = { input_records: number; persisted_records: number; rejected_records: number; retries: number; chunk_count: number; chunk_bytes: number; p50_latency_seconds: number | null; p95_latency_seconds: number | null }
type Status = { status: string; metrics?: Metrics; summary?: { accepted_records: number; persisted_records: number; persisted_chunks: number; metrics: Metrics } }
type Chunk = { chunk_id: string; status: string; record_count: number; byte_size?: number }
type Event = { run_id: string; event_id?: string; status: string; reason?: string; payload?: { metric?: string; value?: number } | null }

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
  const [events, setEvents] = useState<Event[]>([])
  const [error, setError] = useState('')
  const [connected, setConnected] = useState(false)
  const [chunkSize, setChunkSize] = useState(1000)
  const [writerDelay, setWriterDelay] = useState(0)
  const [invalidRatio, setInvalidRatio] = useState(0)
  const [eventFilter, setEventFilter] = useState('all')
  const [selectedChunk, setSelectedChunk] = useState<Chunk | null>(null)

  const refresh = useCallback(async () => {
    try {
      const [nextStatus, nextChunks, nextEvents] = await Promise.all([
        request('/api/runs/status') as Promise<Status>,
        request('/api/chunks') as Promise<Chunk[]>,
        request('/api/events') as Promise<Event[]>,
      ])
      setStatus(nextStatus)
      setChunks(nextChunks)
      setEvents(nextEvents)
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
          settings: {
            chunking: { max_records: chunkSize },
            processing: { writer_delay_ms: writerDelay },
            feed: { records_per_second: feedRate, invalid_record_ratio: invalidRatio },
          },
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
  const visibleEvents = useMemo(
    () => eventFilter === 'all' ? events : events.filter((event) => event.status === eventFilter || event.reason === eventFilter),
    [eventFilter, events],
  )
  const metrics = summary?.metrics ?? status.metrics
  const chartValues = [metrics?.input_records ?? 0, metrics?.persisted_records ?? 0, metrics?.rejected_records ?? 0, metrics?.chunk_count ?? chunks.length]
  const chartMax = Math.max(...chartValues, 1)

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
          <label>Chunk size<input type="number" min="1" max="1000000" value={chunkSize} onChange={(event) => setChunkSize(Number(event.target.value))} /></label>
          <label>Writer delay ms<input type="number" min="0" max="60000" value={writerDelay} onChange={(event) => setWriterDelay(Number(event.target.value))} /></label>
          <label>Invalid ratio<input type="number" min="0" max="1" step="0.05" value={invalidRatio} onChange={(event) => setInvalidRatio(Number(event.target.value))} /></label>
          <button type="submit" disabled={status.status === 'running'}>Start simulation</button>
          <button type="button" className="secondary" onClick={stop} disabled={status.status !== 'running'}>Stop</button>
        </form>
      </section>
      <section className="pipeline" aria-label="Pipeline stages">
        {nodes.map((node, index) => <div className="node" key={node}><strong>{node}</strong>{index < nodes.length - 1 && <span className="arrow">→</span>}</div>)}
      </section>
      <section className="metrics">
        {[
          ['Input records', metrics?.input_records ?? summary?.accepted_records ?? 0],
          ['Persisted records', metrics?.persisted_records ?? summary?.persisted_records ?? 0],
          ['Rejected records', metrics?.rejected_records ?? 0],
          ['Retries', metrics?.retries ?? 0],
          ['Chunks', summary?.persisted_chunks ?? chunks.length],
        ].map(([label, value]) => <article className="metric" key={label}><span>{label}</span><strong>{value}</strong></article>)}
      </section>
      <section className="split">
        <article className="panel">
          <div className="section-title"><h2>Live metrics</h2><span>records / chunks</span></div>
          <div className="chart">{chartValues.map((value, index) => <div className="bar-wrap" key={index}><div className="bar" style={{ height: `${Math.max((value / chartMax) * 100, 4)}%` }} /><small>{['in', 'out', 'reject', 'chunks'][index]}</small></div>)}</div>
          <p className="latency">Latency p50: {metrics?.p50_latency_seconds?.toFixed(3) ?? '—'}s · p95: {metrics?.p95_latency_seconds?.toFixed(3) ?? '—'}s</p>
        </article>
        <article className="panel">
          <div className="section-title"><h2>Event log</h2><select aria-label="Event filter" value={eventFilter} onChange={(event) => setEventFilter(event.target.value)}><option value="all">All events</option><option value="accepted">Accepted</option><option value="rejected">Rejected</option><option value="invalid_type">Invalid type</option></select></div>
          {visibleEvents.length === 0 ? <p className="empty">No events yet.</p> : <ul className="event-list">{visibleEvents.slice(0, 12).map((event, index) => <li key={`${event.event_id ?? 'event'}-${index}`}><b className={event.status}>{event.status}</b><span>{event.reason ?? event.payload?.metric ?? 'pipeline event'}</span></li>)}</ul>}
        </article>
        <article className="panel storage-panel">
          <div className="section-title"><h2>Storage explorer</h2><span>{chunks.length} chunks</span></div>
          {displayChunks.length === 0 ? <p className="empty">No Parquet chunks yet.</p> : <ul className="chunk-list">{displayChunks.map((chunk) => <li key={chunk.chunk_id} onClick={() => setSelectedChunk(chunk)}><code>{chunk.chunk_id.slice(0, 12)}</code><span>{chunk.record_count} records</span><b>{chunk.status}</b></li>)}</ul>}
          {selectedChunk && <div className="inspector"><strong>Selected chunk</strong><code>{selectedChunk.chunk_id}</code><span>{selectedChunk.byte_size ?? 0} bytes · {selectedChunk.record_count} records</span><a href={`${API_URL}/api/chunks/${selectedChunk.chunk_id}/download`} download>Download Parquet</a></div>}
        </article>
      </section>
    </main>
  )
}
