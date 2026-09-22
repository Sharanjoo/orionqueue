import './App.css'

const PHASES = [
  { id: 1, name: 'Project foundation', status: 'in progress' },
  { id: 2, name: 'Protobuf and API layer', status: 'not started' },
  { id: 3, name: 'Persistence', status: 'not started' },
  { id: 11, name: 'Dashboard (this page becomes real)', status: 'not started' },
] as const

/**
 * Phase 1 placeholder. This page deliberately shows no job, worker, or GPU
 * data — the real dashboard (Overview, Jobs, Workers, Cluster capacity)
 * is built in Phase 11 against the real REST API from Phase 2 onward.
 * Nothing here is a stand-in for real backend data.
 */
function App() {
  return (
    <main className="status-page">
      <h1>OrionQueue</h1>
      <p className="tagline">Distributed GPU Workload Orchestrator</p>

      <section aria-labelledby="status-heading">
        <h2 id="status-heading">Build status</h2>
        <p>
          This is a project-foundation placeholder, not the dashboard. See{' '}
          <code>PROJECT_STATUS.md</code> in the repository root for the authoritative, up-to-date
          phase tracker.
        </p>
        <ul className="phase-list">
          {PHASES.map((phase) => (
            <li key={phase.id} className={`phase phase-${phase.status.replace(' ', '-')}`}>
              <span className="phase-id">Phase {phase.id}</span>
              <span className="phase-name">{phase.name}</span>
              <span className="phase-status">{phase.status}</span>
            </li>
          ))}
        </ul>
      </section>
    </main>
  )
}

export default App
