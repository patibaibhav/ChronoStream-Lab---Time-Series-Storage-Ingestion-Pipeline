import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import {
  ReactFlow,
  Background,
  Controls,
  useReactFlow,
  MarkerType,
  type Node,
  type Edge,
  type NodeProps,
} from '@xyflow/react'
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  AreaChart,
  Area,
  Legend,
} from 'recharts'
import '@xyflow/react/dist/style.css'

const API_URL = import.meta.env.VITE_API_URL ?? ''

// --- Types ---

type Metrics = {
  input_records: number
  persisted_records: number
  rejected_records: number
  enqueued_records: number
  pending_records: number
  failed_records: number
  input_rate_records_per_second: number
  persisted_rate_records_per_second: number
  queue_depth: number
  queue_capacity: number
  queue_utilization: number
  backpressure_active: boolean
  backpressure_events: number
  buffer_records: number
  buffer_capacity: number
  buffer_utilization: number
  buffer_partition_count: number
  chunk_count: number
  chunk_records: number
  chunk_bytes: number
  chunks_created: number
  chunks_persisted: number
  chunks_failed: number
  chunks_open: number
  chunks_awaiting_write: number
  chunks_being_written: number
  full_chunks: number
  age_chunks: number
  partial_chunks: number
  file_count: number
  bytes_written: number
  retries: number
  p50_latency_seconds: number | null
  p95_latency_seconds: number | null
  p99_latency_seconds: number | null
}

type Status = {
  status: 'idle' | 'starting' | 'running' | 'stopping' | 'completed' | 'stopped'
  run_id?: string
  metrics?: Metrics
  summary?: {
    run_id: string
    generated_records: number
    accepted_records: number
    rejected_records: number
    persisted_chunks: number
    persisted_records: number
    failed_chunks: number
    metrics: Metrics
  }
}

type Chunk = {
  chunk_id: string
  run_id: string
  status: string
  record_count: number
  byte_size?: number
  flush_reason?: string
  partition_id?: string
  sealed_reason?: string
  created_at?: string
  sealed_at?: string
  write_started_at?: string
  write_completed_at?: string
  retry_attempts?: number
}

type Event = {
  run_id: string
  event_id?: string
  status: string
  reason?: string
  payload?: { metric?: string; value?: number; timestamp?: string } | null
}

type Run = {
  run_id: string
  started_at: string
  finished_at?: string
  status: string
  input_records?: number
  accepted_records?: number
  rejected_records?: number
  persisted_chunks?: number
  persisted_records?: number
  persisted_bytes?: number
  failed_chunks?: number
  flush_reasons?: Record<string, number>
}

type Aggregate = {
  total_runs: number
  total_generated: number
  total_accepted: number
  total_rejected: number
  total_chunks_created: number
  total_chunks_persisted: number
  total_chunks_failed: number
  total_persisted_records: number
  total_files: number
  total_bytes: number
}

type MetricSample = {
  timestamp: string
  run_id: string
  status: string
  metric: string
  value: number
}

type Preset = {
  id: string
  name: string
  description: string
  config: {
    recordCount: number
    feedRate: number
    chunkSize: number
    writerDelay: number
    invalidRatio: number
    bufferMaxRecords?: number
    validationDelayMs?: number
  }
}

// --- Presets ---

const PRESETS: Preset[] = [
  {
    id: 'baseline',
    name: 'Baseline Ingestion',
    description: 'Normal workload with successful persistence',
    config: { recordCount: 1000, feedRate: 500, chunkSize: 100, writerDelay: 0, invalidRatio: 0 },
  },
  {
    id: 'high-throughput',
    name: 'High Throughput',
    description: 'Increased load to examine processing capacity',
    config: { recordCount: 5000, feedRate: 5000, chunkSize: 100, writerDelay: 0, invalidRatio: 0 },
  },
  {
    id: 'slow-storage',
    name: 'Slow Storage',
    description: 'Increased writer delay to observe backlog',
    config: { recordCount: 1000, feedRate: 1000, chunkSize: 100, writerDelay: 200, invalidRatio: 0 },
  },
  {
    id: 'queue-saturation',
    name: 'Queue Saturation',
    description: 'Constrained queue with high offered load',
    config: { recordCount: 2000, feedRate: 10000, chunkSize: 100, writerDelay: 0, invalidRatio: 0, bufferMaxRecords: 100, validationDelayMs: 5 },
  },
  {
    id: 'invalid-records',
    name: 'Invalid Records',
    description: 'High invalid ratio to observe rejection behavior',
    config: { recordCount: 1000, feedRate: 1000, chunkSize: 100, writerDelay: 0, invalidRatio: 0.25 },
  },
  {
    id: 'chunk-comparison',
    name: 'Chunk Size Comparison',
    description: 'Smaller chunks for more frequent flushes',
    config: { recordCount: 1000, feedRate: 1000, chunkSize: 25, writerDelay: 0, invalidRatio: 0 },
  },
  {
    id: 'recovery-test',
    name: 'Recovery Test',
    description: 'Stop and restart to verify recovery',
    config: { recordCount: 5000, feedRate: 2000, chunkSize: 50, writerDelay: 50, invalidRatio: 0.1 },
  },
]

// --- Custom node components ---

function NodeShell({ title, dotClass, children }: { title: string; dotClass: string; children: React.ReactNode }) {
  return (
    <div className="fs-node">
      <div className="node-header">
        <span className={`node-dot ${dotClass}`} />
        <span className="node-title">{title}</span>
      </div>
      <div className="node-metrics">{children}</div>
    </div>
  )
}

function MetricRow({ label, value, valueClass = '' }: { label: string; value: string | number; valueClass?: string }) {
  return (
    <div className="metric-row">
      <span className="metric-label">{label}</span>
      <span className={`metric-value ${valueClass}`}>{value}</span>
    </div>
  )
}

function GeneratorNode({ data }: NodeProps) {
  const m = data.metrics as Metrics | undefined
  const active = (m?.input_rate_records_per_second ?? 0) > 0
  return (
    <NodeShell title="Generator" dotClass={active ? 'active' : 'idle'}>
      <MetricRow label="Rate" value={`${(m?.input_rate_records_per_second ?? 0).toFixed(1)} rec/s`} />
      <MetricRow label="Generated" value={m?.input_records ?? 0} />
      <MetricRow label="Target" value={data.target as number} valueClass="neutral" />
    </NodeShell>
  )
}

function QueueNode({ data }: NodeProps) {
  const m = data.metrics as Metrics | undefined
  const bp = m?.backpressure_active ?? false
  return (
    <NodeShell title="Queue" dotClass={bp ? 'error' : m && m.queue_depth > 0 ? 'active' : 'idle'}>
      <MetricRow label="Depth" value={`${m?.queue_depth ?? 0} / ${m?.queue_capacity ?? 0}`} valueClass={bp ? 'warn' : ''} />
      <MetricRow label="Utilization" value={`${((m?.queue_utilization ?? 0) * 100).toFixed(1)}%`} />
      <MetricRow label="Backpressure" value={bp ? 'ACTIVE' : 'idle'} valueClass={bp ? 'error' : 'neutral'} />
    </NodeShell>
  )
}

function ProcessorNode({ data }: NodeProps) {
  const m = data.metrics as Metrics | undefined
  const total = (m?.input_records ?? 0) - (m?.rejected_records ?? 0)
  const rejectPct = total > 0 ? ((m?.rejected_records ?? 0) / total) * 100 : 0
  return (
    <NodeShell title="Processor" dotClass={m && m.input_records > 0 ? 'active' : 'idle'}>
      <MetricRow label="Accepted" value={total} />
      <MetricRow label="Rejected" value={m?.rejected_records ?? 0} valueClass="warn" />
      <MetricRow label="Reject %" value={`${rejectPct.toFixed(1)}%`} />
    </NodeShell>
  )
}

function BuffersNode({ data }: NodeProps) {
  const m = data.metrics as Metrics | undefined
  return (
    <NodeShell title="Buffers" dotClass={m && m.buffer_records > 0 ? 'active' : 'idle'}>
      <MetricRow label="Buffered" value={m?.buffer_records ?? 0} />
      <MetricRow label="Capacity" value={m?.buffer_capacity ?? 0} valueClass="neutral" />
      <MetricRow label="Utilization" value={`${((m?.buffer_utilization ?? 0) * 100).toFixed(1)}%`} />
      <MetricRow label="Partitions" value={m?.buffer_partition_count ?? 0} valueClass="neutral" />
    </NodeShell>
  )
}

function ChunkBuilderNode({ data }: NodeProps) {
  const m = data.metrics as Metrics | undefined
  return (
    <NodeShell title="Chunk Builder" dotClass={m && m.chunks_open > 0 ? 'active' : 'idle'}>
      <MetricRow label="Created" value={m?.chunks_created ?? 0} />
      <MetricRow label="Open" value={m?.chunks_open ?? 0} valueClass="neutral" />
      <MetricRow label="Full" value={m?.full_chunks ?? 0} valueClass="neutral" />
      <MetricRow label="Age" value={m?.age_chunks ?? 0} valueClass="neutral" />
      <MetricRow label="Partial" value={m?.partial_chunks ?? 0} valueClass="neutral" />
    </NodeShell>
  )
}

function WriterNode({ data }: NodeProps) {
  const m = data.metrics as Metrics | undefined
  return (
    <NodeShell title="Parquet Writer" dotClass={m && m.chunks_being_written > 0 ? 'active' : 'idle'}>
      <MetricRow label="Write rate" value={`${(m?.persisted_rate_records_per_second ?? 0).toFixed(1)} rec/s`} />
      <MetricRow label="Persisted" value={m?.chunks_persisted ?? 0} />
      <MetricRow label="Failed" value={m?.chunks_failed ?? 0} valueClass={m && m.chunks_failed > 0 ? 'error' : ''} />
      <MetricRow label="Retries" value={m?.retries ?? 0} valueClass="neutral" />
      <MetricRow label="p50 latency" value={`${((m?.p50_latency_seconds ?? 0) * 1000).toFixed(1)} ms`} valueClass="neutral" />
    </NodeShell>
  )
}

function StorageNode({ data }: NodeProps) {
  const m = data.metrics as Metrics | undefined
  return (
    <NodeShell title="Local Storage" dotClass={m && m.file_count > 0 ? 'active' : 'idle'}>
      <MetricRow label="Files" value={m?.file_count ?? 0} />
      <MetricRow label="Records" value={m?.persisted_records ?? 0} />
      <MetricRow label="Bytes" value={formatBytes(m?.bytes_written ?? 0)} />
    </NodeShell>
  )
}

const nodeTypes = {
  generator: GeneratorNode,
  queue: QueueNode,
  processor: ProcessorNode,
  buffers: BuffersNode,
  chunkBuilder: ChunkBuilderNode,
  writer: WriterNode,
  storage: StorageNode,
}

// --- Helpers ---

function FitViewButton() {
  const { fitView } = useReactFlow()
  return (
    <button
      type="button"
      onClick={() => fitView({ padding: 0.2, maxZoom: 1 })}
      style={{
        position: 'absolute',
        top: '0.75rem',
        right: '0.75rem',
        zIndex: 10,
        background: '#1a2530',
        border: '1px solid #3d5a75',
        borderRadius: '6px',
        color: '#e7edf5',
        padding: '0.35rem 0.7rem',
        fontSize: '0.72rem',
        cursor: 'pointer',
      }}
    >
      Fit view
    </button>
  )
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
}

async function request(path: string, init?: RequestInit) {
  const response = await fetch(`${API_URL}${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...init,
  })
  if (!response.ok) throw new Error((await response.json()).detail ?? `Request failed (${response.status})`)
  return response.json()
}

// --- Comparison table ---

function ComparisonTable({ runs }: { runs: Run[] }) {
  const [runA, setRunA] = useState<string>(runs[0]?.run_id ?? '')
  const [runB, setRunB] = useState<string>(runs[1]?.run_id ?? '')

  const metrics: { key: string; label: string }[] = [
    { key: 'input_records', label: 'Generated' },
    { key: 'accepted_records', label: 'Accepted' },
    { key: 'rejected_records', label: 'Rejected' },
    { key: 'persisted_chunks', label: 'Chunks' },
    { key: 'persisted_records', label: 'Persisted' },
    { key: 'persisted_bytes', label: 'Bytes' },
    { key: 'failed_chunks', label: 'Failed' },
  ]

  const runAData = runs.find((r) => r.run_id === runA)
  const runBData = runs.find((r) => r.run_id === runB)

  return (
    <div>
      <div style={{ display: 'flex', gap: '1rem', marginBottom: '0.75rem', alignItems: 'center' }}>
        <label style={{ fontSize: '0.75rem', color: '#8ea3b8' }}>Run A
          <select value={runA} onChange={(e) => setRunA(e.target.value)} style={{ marginLeft: '0.4rem' }}>
            {runs.map((r) => <option key={r.run_id} value={r.run_id}>{r.run_id.slice(0, 16)}</option>)}
          </select>
        </label>
        <label style={{ fontSize: '0.75rem', color: '#8ea3b8' }}>Run B
          <select value={runB} onChange={(e) => setRunB(e.target.value)} style={{ marginLeft: '0.4rem' }}>
            {runs.map((r) => <option key={r.run_id} value={r.run_id}>{r.run_id.slice(0, 16)}</option>)}
          </select>
        </label>
      </div>
      <table>
        <thead>
          <tr>
            <th>Metric</th>
            <th>Run A</th>
            <th>Run B</th>
            <th>Difference (B − A)</th>
          </tr>
        </thead>
        <tbody>
          {metrics.map(({ key, label }) => {
            const a = (runAData as Record<string, unknown>)?.[key] as number | undefined
            const b = (runBData as Record<string, unknown>)?.[key] as number | undefined
            const diff = a !== undefined && b !== undefined ? b - a : null
            return (
              <tr key={key}>
                <td>{label}</td>
                <td>{a ?? 'N/A'}</td>
                <td>{b ?? 'N/A'}</td>
                <td style={{ color: diff !== null && diff > 0 ? '#61e7bf' : diff !== null && diff < 0 ? '#ff8d8d' : '#8ea3b8' }}>
                  {diff !== null ? (diff >= 0 ? '+' : '') + diff : 'N/A'}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

// --- Charts ---

function ThroughputChart({ samples }: { samples: MetricSample[] }) {
  if (samples.length === 0) {
    return <div className="empty">No data yet. Start a run to see throughput.</div>
  }
  return (
    <ResponsiveContainer width="100%" height={200}>
      <LineChart data={samples}>
        <CartesianGrid strokeDasharray="3 3" stroke="#1f2327" />
        <XAxis dataKey="timestamp" tick={{ fontSize: 10, fill: '#6b7280' }} />
        <YAxis tick={{ fontSize: 10, fill: '#6b7280' }} />
        <Tooltip contentStyle={{ background: '#16181c', border: '1px solid #26292e', borderRadius: 6, fontSize: 12 }} />
        <Legend wrapperStyle={{ fontSize: 11 }} />
        <Line type="monotone" dataKey="value" stroke="#6ee7b7" dot={false} name="Throughput" />
      </LineChart>
    </ResponsiveContainer>
  )
}

function QueueDepthChart({ samples }: { samples: MetricSample[] }) {
  if (samples.length === 0) {
    return <div className="empty">No data yet. Start a run to see queue and buffer metrics.</div>
  }
  return (
    <ResponsiveContainer width="100%" height={160}>
      <LineChart data={samples}>
        <CartesianGrid strokeDasharray="3 3" stroke="#1f2327" />
        <XAxis dataKey="timestamp" tick={{ fontSize: 10, fill: '#6b7280' }} />
        <YAxis tick={{ fontSize: 10, fill: '#6b7280' }} />
        <Tooltip contentStyle={{ background: '#16181c', border: '1px solid #26292e', borderRadius: 6, fontSize: 12 }} />
        <Legend wrapperStyle={{ fontSize: 11 }} />
        <Line type="monotone" dataKey="value" stroke="#6ee7b7" dot={false} name="Queue Depth" />
      </LineChart>
    </ResponsiveContainer>
  )
}

// --- Main App ---

export function App() {
  const [recordCount, setRecordCount] = useState(100)
  const [feedRate, setFeedRate] = useState(1000)
  const [chunkSize, setChunkSize] = useState(1000)
  const [writerDelay, setWriterDelay] = useState(0)
  const [invalidRatio, setInvalidRatio] = useState(0)
  const [status, setStatus] = useState<Status>({ status: 'idle' })
  const [chunks, setChunks] = useState<Chunk[]>([])
  const [events, setEvents] = useState<Event[]>([])
  const [runs, setRuns] = useState<Run[]>([])
  const [aggregate, setAggregate] = useState<Aggregate | null>(null)
  const [error, setError] = useState('')
  const [connected, setConnected] = useState(false)
  const [currentRunId, setCurrentRunId] = useState<string | null>(null)
  const [selectedNode, setSelectedNode] = useState<string | null>(null)
  const [activePreset, setActivePreset] = useState<string | null>(null)
  const [metricSamples, setMetricSamples] = useState<MetricSample[]>([])
  const [bufferMaxRecords, setBufferMaxRecords] = useState<number | undefined>(undefined)
  const [validationDelayMs, setValidationDelayMs] = useState<number | undefined>(undefined)

  const sampleCounterRef = useRef(0)
  const refresh = useCallback(async () => {
    try {
      const nextStatus = await request('/api/runs/status') as Status
      const runId = currentRunId ?? nextStatus.run_id ?? nextStatus.summary?.run_id
      const runQuery = runId ? `?run_id=${encodeURIComponent(runId)}` : ''
      const [nextChunks, nextEvents, nextRuns, nextAggregate] = await Promise.all([
        request(`/api/chunks${runQuery}`) as Promise<Chunk[]>,
        request(`/api/events${runQuery}`) as Promise<Event[]>,
        request('/api/runs/history') as Promise<Run[]>,
        request('/api/metrics/aggregate') as Promise<Aggregate>,
      ])
      setStatus(nextStatus)
      setChunks(nextChunks)
      setEvents(nextEvents)
      setRuns(nextRuns)
      setAggregate(nextAggregate)
      setConnected(true)
      setError('')
      // Sample metrics for charts during polling too
      if (nextStatus.metrics && (nextStatus.status === 'running' || nextStatus.status === 'stopping')) {
        sampleCounterRef.current += 1
        setMetricSamples(prev => {
          const newSample: MetricSample = {
            timestamp: `${sampleCounterRef.current}`,
            run_id: nextStatus.run_id ?? '',
            status: nextStatus.status,
            metric: 'throughput',
            value: nextStatus.metrics?.input_rate_records_per_second ?? 0,
          }
          return [...prev.slice(-99), newSample]
        })
      }
    } catch (cause) {
      setConnected(false)
      setError(cause instanceof Error ? cause.message : 'Unable to reach the backend')
    }
  }, [currentRunId])

  // WebSocket + polling
  const currentRunIdRef = useRef<string | null>(null)
  useEffect(() => { currentRunIdRef.current = currentRunId }, [currentRunId])

  useEffect(() => {
    void refresh()
    let ws: WebSocket | null = null
    let reconnectTimer: number | null = null
    let disposed = false
    let lastSequence = 0
    let sampleCounter = 0

    const connect = () => {
      if (disposed) return
      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
      ws = new WebSocket(`${protocol}//${window.location.host}/api/ws`)

      ws.onmessage = (event) => {
        try {
          const snapshot = JSON.parse(event.data) as {
            type: string
            run_id?: string
            status: string
            sequence: number
            metrics?: Metrics
          }
          if (snapshot.sequence <= lastSequence) return
          lastSequence = snapshot.sequence
          const activeRunId = currentRunIdRef.current
          if (snapshot.run_id && activeRunId && snapshot.run_id !== activeRunId) return
          if (snapshot.status === 'running' || snapshot.status === 'stopping' || snapshot.status === 'starting') {
            setStatus(prev => {
              if (snapshot.status === 'stopping' && prev.status === 'completed') return prev
              return {
                status: snapshot.status as Status['status'],
                run_id: snapshot.run_id,
                metrics: snapshot.metrics,
              }
            })
            // Sample metrics every 10th message for charts
            if (snapshot.metrics && snapshot.sequence % 10 === 0) {
              sampleCounter++
              setMetricSamples(prev => {
                const newSample: MetricSample = {
                  timestamp: `${sampleCounter}`,
                  run_id: snapshot.run_id ?? '',
                  status: snapshot.status,
                  metric: 'throughput',
                  value: snapshot.metrics?.input_rate_records_per_second ?? 0,
                }
                return [...prev.slice(-99), newSample]
              })
            }
          } else if (snapshot.status === 'completed' || snapshot.status === 'idle') {
            void refresh()
          }
        } catch { /* ignore */ }
      }
      ws.onclose = () => { if (!disposed) reconnectTimer = window.setTimeout(connect, 2000) }
      ws.onerror = () => { ws?.close() }
    }

    connect()
    const timer = window.setInterval(() => void refresh(), 2000)
    return () => {
      disposed = true
      if (reconnectTimer) window.clearTimeout(reconnectTimer)
      ws?.close()
      window.clearInterval(timer)
    }
  }, [refresh])

  const applyPreset = (preset: Preset) => {
    setRecordCount(preset.config.recordCount)
    setFeedRate(preset.config.feedRate)
    setChunkSize(preset.config.chunkSize)
    setWriterDelay(preset.config.writerDelay)
    setInvalidRatio(preset.config.invalidRatio)
    setActivePreset(preset.id)
    // Store buffer and validation delay for use in start
    if (preset.config.bufferMaxRecords) {
      setBufferMaxRecords(preset.config.bufferMaxRecords)
    }
    if (preset.config.validationDelayMs) {
      setValidationDelayMs(preset.config.validationDelayMs)
    }
  }

  const start = async (event: React.FormEvent) => {
    event.preventDefault()
    const runId = `run-${Date.now()}`
    setChunks([]); setEvents([]); setSelectedNode(null)
    setMetricSamples([])
    try {
      const response = await request('/api/runs/start', {
        method: 'POST',
        body: JSON.stringify({
          run_id: runId,
          record_count: recordCount,
          settings: {
            chunking: { max_records: chunkSize },
            processing: { writer_delay_ms: writerDelay, ...(validationDelayMs !== undefined && { validation_delay_ms: validationDelayMs }) },
            feed: { records_per_second: feedRate, invalid_record_ratio: invalidRatio },
            ...(bufferMaxRecords !== undefined && { buffer: { max_records: bufferMaxRecords } }),
          },
        }),
      }) as { run_id: string; status: string }
      if (response.run_id) setCurrentRunId(response.run_id)
      setActivePreset(null)
      await refresh()
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'Unable to start run')
      setCurrentRunId(null)
    }
  }

  const stop = async () => {
    if (status.status === 'stopping') return
    try {
      await request('/api/runs/stop', { method: 'POST' })
      await refresh()
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'Unable to stop run')
    }
  }

  const metrics = status.summary?.metrics ?? status.metrics
  const isStarting = status.status === 'starting'
  const isRunning = status.status === 'running'
  const isStopping = status.status === 'stopping'
  const isTerminal = status.status === 'idle' || status.status === 'completed' || status.status === 'stopped'
  const isActive = isStarting || isRunning || isStopping
  const canvasMetrics = metrics

  const [nodes, setNodes] = useState<Node[]>([])
  useEffect(() => {
    const m = canvasMetrics
    setNodes([
      { id: 'generator', type: 'generator', position: { x: 0, y: 140 }, data: { metrics: m, target: recordCount } },
      { id: 'queue', type: 'queue', position: { x: 220, y: 140 }, data: { metrics: m } },
      { id: 'processor', type: 'processor', position: { x: 440, y: 140 }, data: { metrics: m } },
      { id: 'buffers', type: 'buffers', position: { x: 660, y: 140 }, data: { metrics: m } },
      { id: 'chunkBuilder', type: 'chunkBuilder', position: { x: 880, y: 140 }, data: { metrics: m } },
      { id: 'writer', type: 'writer', position: { x: 1100, y: 140 }, data: { metrics: m } },
      { id: 'storage', type: 'storage', position: { x: 1320, y: 140 }, data: { metrics: m } },
    ])
  }, [canvasMetrics, recordCount])

  const edges: Edge[] = useMemo(() => {
    const m = canvasMetrics
    const genRate = (m?.input_rate_records_per_second ?? 0).toFixed(1)
    const procRate = ((m?.input_records ?? 0) - (m?.rejected_records ?? 0)).toString()
    const writeRate = (m?.persisted_rate_records_per_second ?? 0).toFixed(1)
    const arrow = { type: MarkerType.ArrowClosed, color: '#6ee7b7', width: 20, height: 20 }
    return [
      { id: 'e1', source: 'generator', target: 'queue', label: `${genRate} rec/s`, animated: isRunning, markerEnd: arrow },
      { id: 'e2', source: 'queue', target: 'processor', label: `${procRate} accepted`, animated: isRunning, markerEnd: arrow },
      { id: 'e3', source: 'processor', target: 'buffers', label: `${procRate} rec/s`, animated: isRunning, markerEnd: arrow },
      { id: 'e4', source: 'buffers', target: 'chunkBuilder', label: `${m?.chunks_created ?? 0} chunks`, animated: isRunning, markerEnd: arrow },
      { id: 'e5', source: 'chunkBuilder', target: 'writer', label: `${m?.chunks_awaiting_write ?? 0} waiting`, animated: isRunning, markerEnd: arrow },
      { id: 'e6', source: 'writer', target: 'storage', label: `${writeRate} rec/s`, animated: isRunning, markerEnd: arrow },
    ]
  }, [canvasMetrics, isRunning])

  const visibleEvents = events.slice(0, 20)
  const visibleChunks = chunks.slice(0, 10)

  return (
    <div>
      {/* Top bar */}
      <header className="topbar">
        <span className="brand">FLOWSTORE</span>
        <div className="meta">
          <span>
            <i className={`status-dot ${isRunning ? 'running' : isActive ? 'offline' : connected ? 'online' : 'offline'}`} />
            {isStarting ? 'Starting…' : isRunning ? 'Running' : isStopping ? 'Stopping…' : connected ? 'Connected' : 'Offline'}
          </span>
          <span>Run: {currentRunId ?? '—'}</span>
          <span>Status: {status.status}</span>
        </div>
      </header>

      {error && <div className="alert" role="alert">{error}</div>}

      {/* Presets */}
      <section className="preset-bar">
        <div className="section-title"><h2>Experiment Presets</h2></div>
        <div className="preset-grid">
          {PRESETS.map((preset) => (
            <button
              key={preset.id}
              type="button"
              className={`preset-btn ${activePreset === preset.id ? 'active' : ''}`}
              onClick={() => applyPreset(preset)}
              title={preset.description}
            >
              {preset.name}
            </button>
          ))}
        </div>
      </section>

      {/* Config toolbar */}
      <section className="config-bar">
        <form onSubmit={start} style={{ display: 'contents' }}>
          <label>Records
            <input type="number" min="0" max="100000" value={recordCount} onChange={(e) => { setRecordCount(Number(e.target.value)); setActivePreset(null) }} />
          </label>
          <label>Feed rate /s
            <input type="number" min="1" max="100000" value={feedRate} onChange={(e) => { setFeedRate(Number(e.target.value)); setActivePreset(null) }} />
          </label>
          <label>Chunk size
            <input type="number" min="1" max="1000000" value={chunkSize} onChange={(e) => { setChunkSize(Number(e.target.value)); setActivePreset(null) }} />
          </label>
          <label>Writer delay ms
            <input type="number" min="0" max="60000" value={writerDelay} onChange={(e) => { setWriterDelay(Number(e.target.value)); setActivePreset(null) }} />
          </label>
          <label>Invalid ratio
            <input type="number" min="0" max="1" step="0.05" value={invalidRatio} onChange={(e) => { setInvalidRatio(Number(e.target.value)); setActivePreset(null) }} />
          </label>
          <button type="submit" disabled={isActive}>Start</button>
          <button type="button" className="secondary" onClick={stop} disabled={!isRunning || isStopping}>
            {isStopping ? 'Stopping…' : 'Stop'}
          </button>
        </form>
      </section>

      {/* KPI cards */}
      <section className="kpi-row">
        {[
          { label: 'Generated', value: metrics?.input_records ?? 0 },
          { label: 'Accepted', value: (metrics?.input_records ?? 0) - (metrics?.rejected_records ?? 0) },
          { label: 'Rejected', value: metrics?.rejected_records ?? 0, warn: true },
          { label: 'Persisted', value: metrics?.persisted_records ?? 0 },
          { label: 'Chunks', value: metrics?.chunks_created ?? 0 },
          { label: 'Files', value: metrics?.file_count ?? 0 },
          { label: 'Bytes', value: formatBytes(metrics?.bytes_written ?? 0) },
          { label: 'Queue', value: `${metrics?.queue_depth ?? 0}/${metrics?.queue_capacity ?? 0}` },
        ].map((kpi) => (
          <div key={kpi.label} className="kpi-card">
            <span className="kpi-label">{kpi.label}</span>
            <span className="kpi-value">{kpi.value}</span>
          </div>
        ))}
      </section>

      {/* Pipeline canvas */}
      <div className="canvas-wrap" style={{ position: 'relative' }}>
        <div style={{ position: 'absolute', top: '0.5rem', left: '0.75rem', zIndex: 10, fontSize: '0.68rem', color: '#4b5563', fontWeight: 600, letterSpacing: '0.05em', textTransform: 'uppercase' }}>
          Live Metrics {currentRunId ? `— ${currentRunId.slice(0, 10)}` : ''}
        </div>
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={nodeTypes}
          onNodeClick={(_, node) => setSelectedNode(node.id)}
          minZoom={0.3}
          maxZoom={2}
          zoomOnScroll={false}
          panOnScroll={false}
          panOnDrag={true}
          proOptions={{ hideAttribution: true }}
        >
          <Background color="#1a2530" gap={20} />
          <Controls />
          <FitViewButton />
        </ReactFlow>
      </div>

      {/* Charts */}
      <div className="charts-row">
        <div className="panel">
          <h3>Throughput</h3>
          <ThroughputChart samples={metricSamples} />
        </div>
        <div className="panel">
          <h3>Queue & Buffer</h3>
          <QueueDepthChart samples={metricSamples} />
        </div>
      </div>

      {/* Bottom panels */}
      <div className="panels">
        <div className="panel">
          <h3>Event Log</h3>
          {visibleEvents.length === 0 ? (
            <p className="empty">No events yet.</p>
          ) : (
            <ul className="event-list">
              {visibleEvents.map((event, index) => (
                <li key={`${event.event_id ?? 'event'}-${index}`}>
                  <b className={`event-status ${event.status}`}>{event.status}</b>
                  <span>{event.reason ?? event.payload?.metric ?? '—'}</span>
                </li>
              ))}
            </ul>
          )}
        </div>
        <div className="panel">
          <h3>Storage Explorer</h3>
          {visibleChunks.length === 0 ? (
            <p className="empty">No chunks yet.</p>
          ) : (
            <ul className="chunk-list">
              {visibleChunks.map((chunk) => (
                <li key={chunk.chunk_id}>
                  <span className="chunk-id">{chunk.chunk_id.slice(0, 10)}</span>
                  <span>{chunk.record_count} rec</span>
                  <b className={`chunk-status ${chunk.status}`}>{chunk.status}</b>
                </li>
              ))}
            </ul>
          )}
        </div>
        <div className="panel">
          <h3>All Runs (aggregate)</h3>
          {aggregate ? (
            <div className="aggregate-grid">
              <div className="agg-item"><span className="agg-label">Total runs</span><span className="agg-value">{aggregate.total_runs}</span></div>
              <div className="agg-item"><span className="agg-label">Generated</span><span className="agg-value">{aggregate.total_generated}</span></div>
              <div className="agg-item"><span className="agg-label">Accepted</span><span className="agg-value">{aggregate.total_accepted}</span></div>
              <div className="agg-item"><span className="agg-label">Rejected</span><span className="agg-value agg-warn">{aggregate.total_rejected}</span></div>
              <div className="agg-item"><span className="agg-label">Chunks</span><span className="agg-value">{aggregate.total_chunks_persisted}</span></div>
              <div className="agg-item"><span className="agg-label">Files</span><span className="agg-value">{aggregate.total_files}</span></div>
              <div className="agg-item"><span className="agg-label">Bytes</span><span className="agg-value">{formatBytes(aggregate.total_bytes)}</span></div>
            </div>
          ) : (
            <p className="empty">Loading…</p>
          )}
        </div>
      </div>

      {/* Run history */}
      <div className="run-history">
        <div className="panel">
          <h3>Run History <span style={{ fontSize: '0.68rem', color: '#4b5563', fontWeight: 400 }}>(persisted)</span></h3>
          {runs.length === 0 ? (
            <p className="empty">No runs yet.</p>
          ) : (
            <table>
              <thead>
                <tr>
                  <th>Run ID</th><th>Started</th><th>Duration</th><th>Status</th>
                  <th>Gen</th><th>Acc</th><th>Rej</th><th>Chunks</th><th>Bytes</th>
                </tr>
              </thead>
              <tbody>
                {runs.slice(0, 10).map((run) => {
                  const duration = run.finished_at && run.started_at
                    ? `${Math.max(0, (new Date(run.finished_at).getTime() - new Date(run.started_at).getTime()) / 1000).toFixed(1)}s`
                    : '—'
                  return (
                    <tr
                      key={run.run_id}
                      className={run.run_id === currentRunId ? 'selected' : ''}
                      onClick={() => setCurrentRunId(run.run_id)}
                    >
                      <td className="run-id-cell">{run.run_id.slice(0, 14)}</td>
                      <td>{run.started_at?.slice(11, 19)}</td>
                      <td>{duration}</td>
                      <td><span className={`status-badge status-${run.status}`}>{run.status}</span></td>
                      <td>{run.input_records ?? '—'}</td>
                      <td>{run.accepted_records ?? '—'}</td>
                      <td>{run.rejected_records ?? '—'}</td>
                      <td>{run.persisted_chunks ?? '—'}</td>
                      <td>{run.persisted_bytes ? formatBytes(run.persisted_bytes) : '—'}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          )}
        </div>
      </div>

      {/* Storage explorer full */}
      <div className="run-history">
        <div className="panel">
          <h3>Storage Explorer {currentRunId ? `— ${currentRunId.slice(0, 16)}` : ''}</h3>
          {visibleChunks.length === 0 ? (
            <p className="empty">No chunks for this run.</p>
          ) : (
            <ul className="chunk-list">
              {visibleChunks.map((chunk) => (
                <li key={chunk.chunk_id}>
                  <span className="chunk-id">{chunk.chunk_id.slice(0, 10)}</span>
                  <span>{chunk.record_count} rec</span>
                  <span>{chunk.flush_reason ?? '—'}</span>
                  <b className={`chunk-status ${chunk.status}`}>{chunk.status}</b>
                </li>
              ))}
            </ul>
          )}
        </div>
      </div>

      {/* Run comparison */}
      {runs.length >= 2 && (
        <div className="run-history">
          <div className="panel">
            <h3>Run Comparison</h3>
            <ComparisonTable runs={runs} />
          </div>
        </div>
      )}
    </div>
  )
}
