import { useEffect, useState } from "react"
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
} from "chart.js"
import { Bar, Line } from "react-chartjs-2"

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend
)

type ScoreBucket = { bucket: string; count: number }
type TimelinePoint = { date: string; submissions: number }

const API = "/analytics"
const TOKEN = "my-secret-api-key"

export default function Dashboard() {
  const [scores, setScores] = useState<ScoreBucket[]>([])
  const [timeline, setTimeline] = useState<TimelinePoint[]>([])
  const [error, setError] = useState<string>("")
  const lab = "lab-01"

  useEffect(() => {
    const headers = { Authorization: `Bearer ${TOKEN}` }
    const fetchJson = async <T,>(url: string): Promise<T> => {
      const response = await fetch(url, { headers })
      if (!response.ok) throw new Error(await response.text())
      return response.json() as Promise<T>
    }
    Promise.all([
      fetchJson<ScoreBucket[]>(`${API}/scores?lab=${lab}`),
      fetchJson<TimelinePoint[]>(`${API}/timeline?lab=${lab}`),
    ])
      .then(([s, t]) => { setScores(s); setTimeline(t) })
      .catch((err: unknown) => {
        const message = err instanceof Error ? err.message : "Unknown error"
        setError(message)
      })
  }, [])

  const scoreChartData = {
    labels: scores.map((i) => i.bucket),
    datasets: [{ label: "Submissions", data: scores.map((i) => i.count) }],
  }
  const timelineChartData = {
    labels: timeline.map((i) => i.date),
    datasets: [{ label: "Submissions", data: timeline.map((i) => i.submissions) }],
  }

  return (
    <div style={{ padding: "20px" }}>
      <h1>Lab Analytics Dashboard</h1>
      {error && <p style={{ color: "red" }}>Error: {error}</p>}
      <h2>Score Distribution</h2>
      <Bar data={scoreChartData} />
      <h2>Timeline</h2>
      <Line data={timelineChartData} />
    </div>
  )
}
