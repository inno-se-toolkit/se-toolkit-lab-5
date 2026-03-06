import { useState, useEffect } from 'react'
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

interface Lab {
  id: string
  name: string
}

interface ScoreBucket {
  bucket: string
  count: number
}

interface ScoresResponse {
  lab_id: string
  buckets: ScoreBucket[]
}

interface TimelinePoint {
  date: string
  submissions: number
}

interface TimelineResponse {
  lab_id: string
  data: TimelinePoint[]
}

interface TaskPassRate {
  task_id: string
  task_name: string
  pass_rate: number
}

interface PassRatesResponse {
  lab_id: string
  tasks: TaskPassRate[]
}

type FetchState<T> =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; data: T }
  | { status: 'error'; message: string }

function getAuthHeaders(): HeadersInit {
  const token = localStorage.getItem(STORAGE_KEY)
  return {
    Authorization: `Bearer ${token}`,
    'Content-Type': 'application/json',
  }
}

async function fetchWithAuth<T>(url: string): Promise<T> {
  const res = await fetch(url, { headers: getAuthHeaders() })
  if (!res.ok) {
    const text = await res.text()
    console.error(`API Error ${res.status}:`, text)
    throw new Error(`HTTP ${res.status}: ${text.slice(0, 100)}`)
  }
  return res.json() as Promise<T>
}

interface DashboardProps {
  labs: Lab[]
}

export default function Dashboard({ labs }: DashboardProps) {
  const [selectedLab, setSelectedLab] = useState<string>(() => {
    return labs.length > 0 ? labs[0].id : ''
  })

  const [scoresState, setScoresState] = useState<FetchState<ScoresResponse>>({
    status: 'idle',
  })
  const [timelineState, setTimelineState] = useState<
    FetchState<TimelineResponse>
  >({ status: 'idle' })
  const [passRatesState, setPassRatesState] = useState<
    FetchState<PassRatesResponse>
  >({ status: 'idle' })

  useEffect(() => {
    if (!selectedLab) return

    async function fetchData() {
      setScoresState({ status: 'loading' })
      setTimelineState({ status: 'loading' })
      setPassRatesState({ status: 'loading' })

      try {
        const [scores, timeline, passRates] = await Promise.all([
          fetchWithAuth<ScoresResponse>(`/analytics/scores?lab=${selectedLab}`),
          fetchWithAuth<TimelineResponse>(
            `/analytics/timeline?lab=${selectedLab}`,
          ),
          fetchWithAuth<PassRatesResponse>(
            `/analytics/pass-rates?lab=${selectedLab}`,
          ),
        ])

        setScoresState({ status: 'success', data: scores })
        setTimelineState({ status: 'success', data: timeline })
        setPassRatesState({ status: 'success', data: passRates })
      } catch (err) {
        const message = err instanceof Error ? err.message : 'Unknown error'
        setScoresState({ status: 'error', message })
        setTimelineState({ status: 'error', message })
        setPassRatesState({ status: 'error', message })
      }
    }

    fetchData()
  }, [selectedLab])

  const scoresData =
    scoresState.status === 'success' ? scoresState.data : null
  const timelineData =
    timelineState.status === 'success' ? timelineState.data : null
  const passRatesData =
    passRatesState.status === 'success' ? passRatesState.data : null

  const barChartData = scoresData
    ? {
        labels: scoresData.buckets.map((b) => b.bucket),
        datasets: [
          {
            label: 'Submissions',
            data: scoresData.buckets.map((b) => b.count),
            backgroundColor: 'rgba(54, 162, 235, 0.6)',
            borderColor: 'rgba(54, 162, 235, 1)',
            borderWidth: 1,
          },
        ],
      }
    : null

  const lineChartData = timelineData
    ? {
        labels: timelineData.data.map((p) => p.date),
        datasets: [
          {
            label: 'Submissions per day',
            data: timelineData.data.map((p) => p.submissions),
            borderColor: 'rgba(75, 192, 192, 1)',
            backgroundColor: 'rgba(75, 192, 192, 0.2)',
            tension: 0.1,
          },
        ],
      }
    : null

  return (
    <div className="dashboard">
      <h1>Dashboard</h1>

      <div className="lab-selector">
        <label htmlFor="lab-select">Select Lab: </label>
        <select
          id="lab-select"
          value={selectedLab}
          onChange={(e) => setSelectedLab(e.target.value)}
        >
          {labs.map((lab) => (
            <option key={lab.id} value={lab.id}>
              {lab.name}
            </option>
          ))}
        </select>
      </div>

      <div className="charts-container">
        <section className="chart-section">
          <h2>Score Buckets</h2>
          {scoresState.status === 'loading' && <p>Loading...</p>}
          {scoresState.status === 'error' && (
            <p>Error: {scoresState.message}</p>
          )}
          {barChartData && <Bar data={barChartData} />}
        </section>

        <section className="chart-section">
          <h2>Submissions Timeline</h2>
          {timelineState.status === 'loading' && <p>Loading...</p>}
          {timelineState.status === 'error' && (
            <p>Error: {timelineState.message}</p>
          )}
          {lineChartData && <Line data={lineChartData} />}
        </section>
      </div>

      <section className="pass-rates-section">
        <h2>Pass Rates per Task</h2>
        {passRatesState.status === 'loading' && <p>Loading...</p>}
        {passRatesState.status === 'error' && (
          <p>Error: {passRatesState.message}</p>
        )}
        {passRatesData && (
          <table>
            <thead>
              <tr>
                <th>Task ID</th>
                <th>Task Name</th>
                <th>Pass Rate</th>
              </tr>
            </thead>
            <tbody>
              {passRatesData.tasks.map((task) => (
                <tr key={task.task_id}>
                  <td>{task.task_id}</td>
                  <td>{task.task_name}</td>
                  <td>{(task.pass_rate * 100).toFixed(1)}%</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
    </div>
  )
}
