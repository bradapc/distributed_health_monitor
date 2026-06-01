import { useState, useEffect } from 'react'
import './App.css'

interface ConcurrencySummary {
  active_workers: number
  max_worker_limit: number
  available_tokens: number
}

interface AggregateSummary {
  total_checks_performed: number
  total_network_errors: number
}

interface TargetSummary {
  state: number // 0 = OPEN, 1 = HALF-OPEN, 2 = CLOSED
  failure_count: number
  last_check_latency_ms: number
  last_checked: string
  times_checked: number
  times_errored: number
  percent_success: number
}

interface TargetEvent {
  affected_url: string
  old_state: number
  new_state: number
  network_error: string
  failure_time: number
}

interface TelemetryPayload {
  status: string
  total_uptime: number
  concurrency: ConcurrencySummary
  aggregate: AggregateSummary
  targets: Record<string, TargetSummary>
  event_log: TargetEvent[]
}

function App() {
  const [data, setData] = useState<TelemetryPayload | null>(null)
  const [connectionError, setConnectionError] = useState<string | null>(null)
  const [configText, setConfigText] = useState<string>('{\n  "targets": []\n}')
  const [isEditing, setIsEditing] = useState<boolean>(false)
  const [configStatus, setConfigStatus] = useState<string>('')

  useEffect(() => {
    const eventSource = new EventSource('http://localhost:8080/stream')

    eventSource.onmessage = (event) => {
      try {
        const parsedData: TelemetryPayload = JSON.parse(event.data)
        setData(parsedData)
        setConnectionError(null)
      } catch (err) {
        console.error(err)
      }
    }

    eventSource.onerror = (event) => {
      console.log(event)
      setConnectionError("Telemetry stream disconnected. Attempting automatic reconnection...")
    }

    return () => {
      eventSource.close()
    }
  }, [])

  const fetchCurrentConfig = async () => {
    try {
      const res = await fetch('http://localhost:8080/config')
      if (res.ok) {
        const rawJson = await res.text()
        const formatted = JSON.stringify(JSON.parse(rawJson), null, 2)
        setConfigText(formatted)
        setConfigStatus('')
      }
    } catch (err) {
      setConfigStatus('Failed to load raw configuration file from server.')
    }
  }

  const handleSaveConfig = async () => {
    setConfigStatus('Validating and saving configuration...')
    try {
      JSON.parse(configText)

      const res = await fetch('http://localhost:8080/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: configText
      })

      if (res.ok) {
        setConfigStatus('Configuration hot-swapped successfully!')
        setIsEditing(false)
      } else {
        const errMsg = await res.text()
        setConfigStatus(`Server Validation Error: ${errMsg}`)
      }
    } catch (e: any) {
      setConfigStatus(`Invalid JSON Syntax: ${e.message}`)
    }
  }

  const getStatusDetails = (stateNum: number) => {
    switch (stateNum) {
      case 0:
        return { text: 'OPEN (Tripped)', className: 'status-badge danger-bg', color: 'var(--danger)' }
      case 1:
        return { text: 'HALF-OPEN (Testing)', className: 'status-badge warning-bg', color: '#ffc107' }
      case 2:
      default:
        return { text: 'CLOSED (HEALTHY)', className: 'status-badge success-bg', color: 'var(--success)' }
    }
  }

  return (
    <div className="app-container">
      <header className="dashboard-header">
        <div>
          <h1>Distributed Edge Health Watcher</h1>
          <p>Real-time telemetry engine dashboard</p>
        </div>
        <div className="status-indicator">
          <span className={`status-dot ${connectionError ? 'error' : 'active'}`}></span>
          <span>{connectionError ? 'Stream Interrupted' : 'Streaming Active'}</span>
        </div>
      </header>

      {connectionError && (
        <div className="alert-banner">
          {connectionError}
        </div>
      )}

      {data && (
        <>
          <section className="stats-grid">
            <div className="stat-card">
              <h3>Worker Pool Load</h3>
              <div className="stat-value">{data.concurrency.active_workers} / {data.concurrency.max_worker_limit}</div>
              <p>{data.concurrency.available_tokens} tokens sitting in semaphore</p>
            </div>
            <div className="stat-card">
              <h3>Total Global Probes</h3>
              <div className="stat-value">{data.aggregate.total_checks_performed}</div>
              <p>Requests performed lock-free</p>
            </div>
            <div className="stat-card">
              <h3>Global Outages</h3>
              <div className="stat-value error">{data.aggregate.total_network_errors}</div>
              <p>Network connection anomalies</p>
            </div>
          </section>

          <section style={{ marginBottom: '40px' }}>
            <div className="section-title-bar">
              <h2>Monitored Edge Targets ({Object.keys(data.targets).length})</h2>
              <button 
                className="btn-primary"
                onClick={() => { setIsEditing(!isEditing); if(!isEditing) fetchCurrentConfig(); }}
              >
                {isEditing ? 'Close File Editor' : 'Edit Targets File'}
              </button>
            </div>

            {isEditing && (
              <div className="editor-wrapper">
                <h3>Raw Configuration File Editor (`targets.json`)</h3>
                <p>Modifying this content directly mimics opening your text editor locally. Saving triggers atomic system operations.</p>
                <textarea
                  className="editor-textarea"
                  value={configText}
                  onChange={(e) => setConfigText(e.target.value)}
                  rows={12}
                />
                <div className="editor-controls">
                  <button className="btn-success" onClick={handleSaveConfig}>Save & Hot-Reload File</button>
                  <button className="btn-secondary" onClick={() => setIsEditing(false)}>Cancel</button>
                  {configStatus && <span className="editor-status">{configStatus}</span>}
                </div>
              </div>
            )}

            <div className="targets-grid">
              {Object.entries(data.targets).map(([url, target]) => {
                const badge = getStatusDetails(target.state)
                const cardClass = target.state === 0 ? 'target-card tripped' : target.state === 1 ? 'target-card half-tripped' : 'target-card'
                
                return (
                  <div key={url} className={cardClass}>
                    <div>
                      <div className="target-card-header">
                        <span className={badge.className} style={{ backgroundColor: badge.color, color: target.state === 1 ? '#000' : '#fff' }}>
                          {badge.text}
                        </span>
                        <span className="success-rate">{target.percent_success.toFixed(0)}% success</span>
                      </div>
                      <h3 className="target-url">{url}</h3>
                    </div>

                    <div className="target-meta-metrics">
                      <div>Latency: <strong>{target.last_check_latency_ms}ms</strong></div>
                      <div>Cycles: <strong>{target.times_checked}</strong></div>
                      <div>Failures Counter: <strong className={target.failure_count > 0 ? 'error-count' : ''}>{target.failure_count}</strong></div>
                      <div>Total Errors: <strong>{target.times_errored}</strong></div>
                    </div>
                    <div className="target-card-footer">
                      Last probed: {target.last_checked ? new Date(target.last_checked).toLocaleTimeString() : 'Never'}
                    </div>
                  </div>
                )
              })}
            </div>
          </section>

          <section style={{ marginBottom: '40px' }}>
            <h2 style={{ margin: '0 0 15px 0', fontSize: '20px', color: 'var(--text-white)' }}>System State Audit Log</h2>
            <div className="audit-log-container">
              {data.event_log && data.event_log.length > 0 ? (
                [...data.event_log].reverse().map((event, idx) => {
                  if (!event.affected_url) return null
                  
                  let rowStyleClass = 'audit-log-row healthy'
                  if (event.new_state === 0) rowStyleClass = 'audit-log-row failure'
                  if (event.new_state === 1) rowStyleClass = 'audit-log-row warning'

                  return (
                    <div key={idx} className={rowStyleClass}>
                      <span className="audit-log-timestamp">[{event.failure_time?.toString().split('.')[0].replace('T', ' ')}]</span>
                      <strong>{event.affected_url}</strong> transitioned from {getStatusDetails(event.old_state).text} to <strong style={{ textDecoration: 'underline' }}>{getStatusDetails(event.new_state).text}</strong>
                      {event.network_error && <div className="audit-log-reason">↳ Reason: {event.network_error}</div>}
                    </div>
                  )
                })
              ) : (
                <div className="audit-log-empty">No cluster state changes logged yet. System stable.</div>
              )}
            </div>
          </section>
        </>
      )}

      {!data && !connectionError && (
        <div className="loading-screen">
          <h2>Establishing pipe communication channel...</h2>
          <p>Polling stream endpoint allocations at port 8080</p>
        </div>
      )}
    </div>
  )
}

export default App