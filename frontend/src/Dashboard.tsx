import { useState, useEffect, useReducer } from 'react'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js'
import { Bar, Line } from 'react-chartjs-2'
import './App.css'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
)

const STORAGE_KEY = 'api_key'

// Use relative URLs - Vite dev server will proxy to the API
const API_BASE_URL = ''

interface Item {
  id: number
  type: string
  title: string
  created_at: string
}

// API Response types
interface ScoreBucket {
  bucket: string
  count: number
}

interface PassRate {
  task: string
  avg_score: number
  attempts: number
}

interface TimelineEntry {
  date: string
  submissions: number
}

interface Lab {
  id: number
  title: string
}

interface ScoresData {
  status: 'idle' | 'loading' | 'success' | 'error'
  data: ScoreBucket[]
  message?: string
}

interface PassRatesData {
  status: 'idle' | 'loading' | 'success' | 'error'
  data: PassRate[]
  message?: string
}

interface TimelineData {
  status: 'idle' | 'loading' | 'success' | 'error'
  data: TimelineEntry[]
  message?: string
}

interface LabsData {
  status: 'idle' | 'loading' | 'success' | 'error'
  data: Lab[]
  message?: string
}

type ScoresAction =
  | { type: 'fetch_start' }
  | { type: 'fetch_success'; data: ScoreBucket[] }
  | { type: 'fetch_error'; message: string }

type PassRatesAction =
  | { type: 'fetch_start' }
  | { type: 'fetch_success'; data: PassRate[] }
  | { type: 'fetch_error'; message: string }

type TimelineAction =
  | { type: 'fetch_start' }
  | { type: 'fetch_success'; data: TimelineEntry[] }
  | { type: 'fetch_error'; message: string }

type LabsAction =
  | { type: 'fetch_start' }
  | { type: 'fetch_success'; data: Lab[] }
  | { type: 'fetch_error'; message: string }

function scoresReducer(_state: ScoresData, action: ScoresAction): ScoresData {
  switch (action.type) {
    case 'fetch_start':
      return { status: 'loading', data: [] }
    case 'fetch_success':
      return { status: 'success', data: action.data }
    case 'fetch_error':
      return { status: 'error', data: [], message: action.message }
  }
}

function passRatesReducer(
  _state: PassRatesData,
  action: PassRatesAction,
): PassRatesData {
  switch (action.type) {
    case 'fetch_start':
      return { status: 'loading', data: [] }
    case 'fetch_success':
      return { status: 'success', data: action.data }
    case 'fetch_error':
      return { status: 'error', data: [], message: action.message }
  }
}

function timelineReducer(
  _state: TimelineData,
  action: TimelineAction,
): TimelineData {
  switch (action.type) {
    case 'fetch_start':
      return { status: 'loading', data: [] }
    case 'fetch_success':
      return { status: 'success', data: action.data }
    case 'fetch_error':
      return { status: 'error', data: [], message: action.message }
  }
}

function labsReducer(_state: LabsData, action: LabsAction): LabsData {
  switch (action.type) {
    case 'fetch_start':
      return { status: 'loading', data: [] }
    case 'fetch_success':
      return { status: 'success', data: action.data }
    case 'fetch_error':
      return { status: 'error', data: [], message: action.message }
  }
}

interface DashboardProps {
  token: string
  onDisconnect: () => void
}

export default function Dashboard({ token, onDisconnect }: DashboardProps) {
  const [selectedLab, setSelectedLab] = useState<string>('')
  const [scoresState, scoresDispatch] = useReducer(scoresReducer, {
    status: 'idle',
    data: [],
  })
  const [passRatesState, passRatesDispatch] = useReducer(passRatesReducer, {
    status: 'idle',
    data: [],
  })
  const [timelineState, timelineDispatch] = useReducer(timelineReducer, {
    status: 'idle',
    data: [],
  })
  const [labsState, labsDispatch] = useReducer(labsReducer, {
    status: 'idle',
    data: [],
  })

  // Fetch labs on mount
  useEffect(() => {
    labsDispatch({ type: 'fetch_start' })

    fetch(`${API_BASE_URL}/items/`, {
      headers: { Authorization: `Bearer ${token}` },
    })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        return res.json()
      })
      .then((items: Item[]) => {
        const labs: Lab[] = items
          .filter((item) => item.type === 'lab')
          .map((item) => ({ id: item.id, title: item.title }))
        labsDispatch({ type: 'fetch_success', data: labs })
        if (labs.length > 0 && !selectedLab) {
          // Extract lab identifier from title (e.g., "Lab 04 — Testing" → "lab-04")
          const firstLab = labs[0]
          const match = firstLab.title.match(/Lab (\d+)/i)
          if (match) {
            setSelectedLab(`lab-${match[1].padStart(2, '0')}`)
          }
        }
      })
      .catch((err: Error) =>
        labsDispatch({ type: 'fetch_error', message: err.message }),
      )
  }, [token])

  // Fetch analytics data when selectedLab changes
  useEffect(() => {
    if (!selectedLab) return

    // Fetch scores
    scoresDispatch({ type: 'fetch_start' })
    fetch(`${API_BASE_URL}/analytics/scores?lab=${selectedLab}`, {
      headers: { Authorization: `Bearer ${token}` },
    })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        return res.json()
      })
      .then((data: ScoreBucket[]) =>
        scoresDispatch({ type: 'fetch_success', data }),
      )
      .catch((err: Error) =>
        scoresDispatch({ type: 'fetch_error', message: err.message }),
      )

    // Fetch pass rates
    passRatesDispatch({ type: 'fetch_start' })
    fetch(`${API_BASE_URL}/analytics/pass-rates?lab=${selectedLab}`, {
      headers: { Authorization: `Bearer ${token}` },
    })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        return res.json()
      })
      .then((data: PassRate[]) =>
        passRatesDispatch({ type: 'fetch_success', data }),
      )
      .catch((err: Error) =>
        passRatesDispatch({ type: 'fetch_error', message: err.message }),
      )

    // Fetch timeline
    timelineDispatch({ type: 'fetch_start' })
    fetch(`${API_BASE_URL}/analytics/timeline?lab=${selectedLab}`, {
      headers: { Authorization: `Bearer ${token}` },
    })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        return res.json()
      })
      .then((data: TimelineEntry[]) =>
        timelineDispatch({ type: 'fetch_success', data }),
      )
      .catch((err: Error) =>
        timelineDispatch({ type: 'fetch_error', message: err.message }),
      )
  }, [selectedLab, token])

  // Prepare chart data for scores
  const scoresChartData = {
    labels: scoresState.data.map((b) => b.bucket),
    datasets: [
      {
        label: 'Number of Students',
        data: scoresState.data.map((b) => b.count),
        backgroundColor: 'rgba(54, 162, 235, 0.6)',
        borderColor: 'rgba(54, 162, 235, 1)',
        borderWidth: 1,
      },
    ],
  }

  // Prepare chart data for timeline
  const timelineChartData = {
    labels: timelineState.data.map((t) => t.date),
    datasets: [
      {
        label: 'Submissions',
        data: timelineState.data.map((t) => t.submissions),
        borderColor: 'rgba(75, 192, 192, 1)',
        backgroundColor: 'rgba(75, 192, 192, 0.2)',
        tension: 0.1,
        fill: true,
      },
    ],
  }

  const chartOptions = {
    responsive: true,
    plugins: {
      legend: {
        position: 'top' as const,
      },
    },
  }

  return (
    <div>
      <header className="app-header">
        <h1>Dashboard</h1>
        <button className="btn-disconnect" onClick={onDisconnect}>
          Disconnect
        </button>
      </header>

      <div className="lab-selector">
        <label htmlFor="lab-select">Select Lab: </label>
        <select
          id="lab-select"
          value={selectedLab}
          onChange={(e) => setSelectedLab(e.target.value)}
        >
          <option value="">-- Select a lab --</option>
          {labsState.status === 'success' &&
            labsState.data.map((lab) => {
              // Extract lab identifier from title for the value
              const match = lab.title.match(/Lab (\d+)/i)
              const labId = match ? `lab-${match[1].padStart(2, '0')}` : `lab-${lab.id}`
              return (
                <option key={lab.id} value={labId}>
                  {lab.title}
                </option>
              )
            })}
        </select>
      </div>

      {labsState.status === 'loading' && <p>Loading labs...</p>}
      {labsState.status === 'error' && <p>Error: {labsState.message}</p>}

      {!selectedLab && labsState.status === 'success' && (
        <p>Please select a lab from the dropdown above.</p>
      )}

      {selectedLab && (
        <>
          {/* Scores Histogram */}
          <section className="dashboard-section">
            <h2>Score Distribution</h2>
            {scoresState.status === 'loading' && <p>Loading...</p>}
            {scoresState.status === 'error' && (
              <p>Error: {scoresState.message}</p>
            )}
            {scoresState.status === 'success' && (
              <Bar data={scoresChartData} options={chartOptions} />
            )}
          </section>

          {/* Timeline Chart */}
          <section className="dashboard-section">
            <h2>Submissions Timeline</h2>
            {timelineState.status === 'loading' && <p>Loading...</p>}
            {timelineState.status === 'error' && (
              <p>Error: {timelineState.message}</p>
            )}
            {timelineState.status === 'success' &&
              timelineState.data.length > 0 && (
                <Line data={timelineChartData} options={chartOptions} />
              )}
            {timelineState.status === 'success' &&
              timelineState.data.length === 0 && (
                <p>No submission data available.</p>
              )}
          </section>

          {/* Pass Rates Table */}
          <section className="dashboard-section">
            <h2>Pass Rates by Task</h2>
            {passRatesState.status === 'loading' && <p>Loading...</p>}
            {passRatesState.status === 'error' && (
              <p>Error: {passRatesState.message}</p>
            )}
            {passRatesState.status === 'success' &&
              passRatesState.data.length > 0 && (
                <table>
                  <thead>
                    <tr>
                      <th>Task</th>
                      <th>Average Score</th>
                      <th>Attempts</th>
                    </tr>
                  </thead>
                  <tbody>
                    {passRatesState.data.map((rate, index) => (
                      <tr key={index}>
                        <td>{rate.task}</td>
                        <td>{rate.avg_score}</td>
                        <td>{rate.attempts}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            {passRatesState.status === 'success' &&
              passRatesState.data.length === 0 && (
                <p>No pass rate data available.</p>
              )}
          </section>
        </>
      )}
    </div>
  )
}
