import { useState, useEffect } from 'react'

interface TargetEvent {
  affected_url: string
  old_state: number
  new_state: number
  network_error: string
  failure_time: number
}

interface TargetDetailModalProps {
  url: string
  onClose: () => void
  getStatusDetails: (state: number) => { text: string; className: string; color: string }
}

export const TargetDetailModal: React.FC<TargetDetailModalProps> = ({
  url,
  onClose,
  getStatusDetails,
}) => {
  const [events, setEvents] = useState<TargetEvent[]>([])
  const [isLoading, setIsLoading] = useState<boolean>(true)
  const [fetchError, setFetchError] = useState<string | null>(null)

  useEffect(() => {
    const fetchNodeHistory = async () => {
      setIsLoading(true)
      setFetchError(null)

      try {
        const b64Id = btoa(url)

        const res = await fetch(`http://localhost:8080/targets/${b64Id}`)
        
        if (!res.ok) {
          throw new Error(`Server returned status ${res.status}`)
        }

        const data: TargetEvent[] = await res.json()
        setEvents(data || [])
      } catch (err: any) {
        console.error("Failed to fetch target historical events:", err)
        setFetchError(err.message || "Failed to load audit history.")
      } finally {
        setIsLoading(false)
      }
    }

    fetchNodeHistory()
  }, [url])

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-content" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <div>
            <h2>Isolated Node Audit Log Profile</h2>
            <code className="modal-target-url">{url}</code>
          </div>
          <button className="modal-close-btn" onClick={onClose}>&times;</button>
        </div>

        <div className="modal-body">
          <h3>Failure Event Log ({events.length})</h3>
          
          {isLoading && (
            <div className="modal-loading">Querying event log...</div>
          )}

          {fetchError && (
            <div className="alert-banner" style={{ margin: '10px 0' }}>
              Error: {fetchError}
            </div>
          )}

          {!isLoading && !fetchError && (
            <div className="audit-log-container modal-log-override">
              {events.length > 0 ? (
                [...events].reverse().map((event, idx) => {
                  let rowStyleClass = 'audit-log-row healthy'
                  if (event.new_state === 0) rowStyleClass = 'audit-log-row failure'
                  if (event.new_state === 1) rowStyleClass = 'audit-log-row warning'

                  return (
                    <div key={idx} className={rowStyleClass}>
                      <span className="audit-log-timestamp">
                        [{event.failure_time ? new Date(event.failure_time).toLocaleTimeString() : 'Recent'}]
                      </span>
                      State Shift: <strong>{getStatusDetails(event.old_state).text}</strong> &rarr;{' '}
                      <strong style={{ textDecoration: 'underline' }}>
                        {getStatusDetails(event.new_state).text}
                      </strong>
                      {event.network_error && (
                        <div className="audit-log-reason">↳ Reason: {event.network_error}</div>
                      )}
                    </div>
                  )
                })
              ) : (
                <div className="audit-log-empty">
                  No failures have been recorded for this node.
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}