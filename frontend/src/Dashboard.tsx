import { useState, useEffect } from 'react'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  LineElement,
  PointElement,
} from 'chart.js'
import { Bar, Line } from 'react-chartjs-2'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  LineElement,
  PointElement,
)

const STORAGE_KEY = 'api_key'

interface ScoreBucket {
  bucket: string
  count: number
}

interface ScoresResponse {
  lab_id: string
  buckets: ScoreBucket[]
}

interface TimelineEntry {
  date: string
  count: number
}

interface TimelineResponse {
  lab_id: string
  submissions: TimelineEntry[]
}

interface PassRateEntry {
  task: string
  pass_rate: number
  total: number
  passed: number
}

interface PassRatesResponse {
  lab_id: string
  tasks: PassRateEntry[]
}

interface DashboardProps {
  onNavigate: (page: 'items' | 'dashboard') => void
}

const DEFAULT_LAB = 'lab-04'

export default function Dashboard({ onNavigate }: DashboardProps) {
  const [labId, setLabId] = useState(DEFAULT_LAB)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const [scoresData, setScoresData] = useState<ScoresResponse | null>(null)
  const [timelineData, setTimelineData] = useState<TimelineResponse | null>(
    null,
  )
  const [passRatesData, setPassRatesData] = useState<PassRatesResponse | null>(
    null,
  )

  useEffect(() => {
    const token = localStorage.getItem(STORAGE_KEY)
    if (!token) {
      setError('No API token found')
      setLoading(false)
      return
    }

    async function fetchAnalytics() {
      setLoading(true)
      setError(null)

      try {
        const headers = { Authorization: `Bearer ${token}` }

        const [scoresRes, timelineRes, passRatesRes] = await Promise.all([
          fetch(`/analytics/scores?lab=${labId}`, { headers }),
          fetch(`/analytics/timeline?lab=${labId}`, { headers }),
          fetch(`/analytics/pass-rates?lab=${labId}`, { headers }),
        ])

        if (!scoresRes.ok) {
          throw new Error(`Scores: HTTP ${scoresRes.status}`)
        }
        if (!timelineRes.ok) {
          throw new Error(`Timeline: HTTP ${timelineRes.status}`)
        }
        if (!passRatesRes.ok) {
          throw new Error(`Pass rates: HTTP ${passRatesRes.status}`)
        }

        const scores: ScoresResponse = await scoresRes.json()
        const timeline: TimelineResponse = await timelineRes.json()
        const passRates: PassRatesResponse = await passRatesRes.json()

        setScoresData(scores)
        setTimelineData(timeline)
        setPassRatesData(passRates)
      } catch (err: unknown) {
        const message = err instanceof Error ? err.message : 'Unknown error'
        setError(message)
      } finally {
        setLoading(false)
      }
    }

    fetchAnalytics()
  }, [labId])

  const barChartData = scoresData
    ? {
        labels: scoresData.buckets.map((b) => b.bucket),
        datasets: [
          {
            label: 'Score Distribution',
            data: scoresData.buckets.map((b) => b.count),
            backgroundColor: 'rgba(54, 162, 235, 0.6)',
            borderColor: 'rgba(54, 162, 235, 1)',
            borderWidth: 1,
          },
        ],
      }
    : { labels: [], datasets: [] }

  const lineChartData = timelineData
    ? {
        labels: timelineData.submissions.map((s) => s.date),
        datasets: [
          {
            label: 'Submissions Over Time',
            data: timelineData.submissions.map((s) => s.count),
            borderColor: 'rgba(75, 192, 192, 1)',
            backgroundColor: 'rgba(75, 192, 192, 0.2)',
            tension: 0.1,
            fill: true,
          },
        ],
      }
    : { labels: [], datasets: [] }

  return (
    <div>
      <header className="app-header">
        <h1>Dashboard</h1>
        <div>
          <button onClick={() => onNavigate('items')}>Items</button>
          <select
            value={labId}
            onChange={(e) => setLabId(e.target.value)}
            style={{ marginLeft: '10px' }}
          >
            <option value="lab-04">lab-04</option>
            <option value="lab-05">lab-05</option>
            <option value="lab-06">lab-06</option>
          </select>
        </div>
      </header>

      {loading && <p>Loading analytics...</p>}
      {error && <p style={{ color: 'red' }}>Error: {error}</p>}

      {!loading && !error && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '30px' }}>
          <section>
            <h2>Score Distribution</h2>
            {scoresData && scoresData.buckets.length > 0 ? (
              <Bar
                data={barChartData}
                options={{
                  responsive: true,
                  plugins: {
                    title: {
                      display: true,
                      text: 'Scores by Bucket',
                    },
                  },
                }}
              />
            ) : (
              <p>No score data available</p>
            )}
          </section>

          <section>
            <h2>Submissions Over Time</h2>
            {timelineData && timelineData.submissions.length > 0 ? (
              <Line
                data={lineChartData}
                options={{
                  responsive: true,
                  plugins: {
                    title: {
                      display: true,
                      text: 'Daily Submissions',
                    },
                  },
                }}
              />
            ) : (
              <p>No timeline data available</p>
            )}
          </section>

          <section>
            <h2>Pass Rates by Task</h2>
            {passRatesData && passRatesData.tasks.length > 0 ? (
              <table>
                <thead>
                  <tr>
                    <th>Task</th>
                    <th>Pass Rate</th>
                    <th>Passed / Total</th>
                  </tr>
                </thead>
                <tbody>
                  {passRatesData.tasks.map((task) => (
                    <tr key={task.task}>
                      <td>{task.task}</td>
                      <td>{(task.pass_rate * 100).toFixed(1)}%</td>
                      <td>
                        {task.passed} / {task.total}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            ) : (
              <p>No pass rate data available</p>
            )}
          </section>
        </div>
      )}
    </div>
  )
}
