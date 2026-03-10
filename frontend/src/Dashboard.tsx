import { useEffect, useMemo, useState } from 'react'
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
  Filler,
} from 'chart.js'
import type { ChartOptions } from 'chart.js'
import { Bar, Line } from 'react-chartjs-2'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
  Filler,
)

const STORAGE_KEY = 'api_key'
const DEFAULT_LAB = 'lab-04'

interface ScoreBucket {
  bucket: string
  count: number
}

interface TimelinePoint {
  date: string
  submissions: number
}

interface PassRateRow {
  task: string
  avg_score: number
  attempts: number
}

type LoadState =
  | { status: 'idle' | 'loading' }
  | { status: 'success' }
  | { status: 'error'; message: string }

async function fetchJson<T>(url: string, token: string): Promise<T> {
  const response = await fetch(url, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  })

  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`)
  }

  return (await response.json()) as T
}

function Dashboard() {
  const [lab, setLab] = useState<string>(DEFAULT_LAB)
  const [scores, setScores] = useState<ScoreBucket[]>([])
  const [timeline, setTimeline] = useState<TimelinePoint[]>([])
  const [passRates, setPassRates] = useState<PassRateRow[]>([])
  const [loadState, setLoadState] = useState<LoadState>({ status: 'idle' })

  useEffect(() => {
    const token = localStorage.getItem(STORAGE_KEY)

    if (token === null) {
      setLoadState({
        status: 'error',
        message: 'API key not found in localStorage.',
      })
      return
    }

    const authToken: string = token
    let cancelled = false

    async function loadDashboard(): Promise<void> {
      setLoadState({ status: 'loading' })

      try {
        const encodedLab = encodeURIComponent(lab)

        const [scoresData, timelineData, passRatesData] = await Promise.all([
          fetchJson<ScoreBucket[]>(
            `/analytics/scores?lab=${encodedLab}`,
            authToken,
          ),
          fetchJson<TimelinePoint[]>(
            `/analytics/timeline?lab=${encodedLab}`,
            authToken,
          ),
          fetchJson<PassRateRow[]>(
            `/analytics/pass-rates?lab=${encodedLab}`,
            authToken,
          ),
        ])

        if (cancelled) {
          return
        }

        setScores(scoresData)
        setTimeline(timelineData)
        setPassRates(passRatesData)
        setLoadState({ status: 'success' })
      } catch (error: unknown) {
        if (cancelled) {
          return
        }

        const message =
          error instanceof Error ? error.message : 'Unknown error'

        setLoadState({ status: 'error', message })
      }
    }

    void loadDashboard()

    return () => {
      cancelled = true
    }
  }, [lab])

  const scoreChartData = useMemo(
    () => ({
      labels: scores.map((item) => item.bucket),
      datasets: [
        {
          label: 'Scores',
          data: scores.map((item) => item.count),
          backgroundColor: 'rgba(59, 130, 246, 0.7)',
          borderColor: 'rgba(59, 130, 246, 1)',
          borderWidth: 1,
        },
      ],
    }),
    [scores],
  )

  const timelineChartData = useMemo(
    () => ({
      labels: timeline.map((item) => item.date),
      datasets: [
        {
          label: 'Submissions',
          data: timeline.map((item) => item.submissions),
          borderColor: 'rgba(16, 185, 129, 1)',
          backgroundColor: 'rgba(16, 185, 129, 0.2)',
          fill: true,
          tension: 0.25,
        },
      ],
    }),
    [timeline],
  )

  const scoreChartOptions = useMemo<ChartOptions<'bar'>>(
    () => ({
      responsive: true,
      plugins: {
        legend: {
          display: false,
        },
        title: {
          display: true,
          text: 'Score distribution',
        },
      },
      scales: {
        y: {
          beginAtZero: true,
          ticks: {
            precision: 0,
          },
        },
      },
    }),
    [],
  )

  const timelineChartOptions = useMemo<ChartOptions<'line'>>(
    () => ({
      responsive: true,
      plugins: {
        title: {
          display: true,
          text: 'Submissions over time',
        },
      },
      scales: {
        y: {
          beginAtZero: true,
          ticks: {
            precision: 0,
          },
        },
      },
    }),
    [],
  )

  return (
    <section className="dashboard">
      <header className="app-header">
        <div>
          <h2>Dashboard</h2>
          <p>Analytics for the selected lab.</p>
        </div>

        <label>
          Lab:{' '}
          <select value={lab} onChange={(e) => setLab(e.target.value)}>
            <option value="lab-04">lab-04</option>
            <option value="lab-03">lab-03</option>
            <option value="lab-02">lab-02</option>
            <option value="lab-01">lab-01</option>
          </select>
        </label>
      </header>

      {loadState.status === 'loading' && <p>Loading dashboard...</p>}

      {loadState.status === 'error' && (
        <p role="alert" className="error-text">
          Error: {loadState.message}
        </p>
      )}

      {loadState.status === 'success' && (
        <>
          <div className="card">
            <Bar data={scoreChartData} options={scoreChartOptions} />
          </div>

          <div className="card">
            <Line data={timelineChartData} options={timelineChartOptions} />
          </div>

          <div className="card">
            <h3>Pass rates by task</h3>

            {passRates.length === 0 ? (
              <p>No pass-rate data available.</p>
            ) : (
              <table>
                <thead>
                  <tr>
                    <th>Task</th>
                    <th>Average score</th>
                    <th>Attempts</th>
                  </tr>
                </thead>
                <tbody>
                  {passRates.map((row) => (
                    <tr key={row.task}>
                      <td>{row.task}</td>
                      <td>{row.avg_score.toFixed(1)}</td>
                      <td>{row.attempts}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </>
      )}
    </section>
  )
}

export default Dashboard
