import { useEffect, useState } from "react"
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  ArcElement,
  Title,
  Tooltip,
  Legend,
} from "chart.js"
import { Bar, Line, Pie } from "react-chartjs-2"

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  ArcElement,
  Title,
  Tooltip,
  Legend
)

const API = "/analytics"
const TOKEN = "my-secret-api-key"

type ScoreBucket = {
  bucket: string
  count: number
}

type PassRate = {
  task: string
  avg_score: number
  attempts: number
}

type TimelinePoint = {
  date: string
  submissions: number
}

type GroupStat = {
  group: string
  avg_score: number
  students: number
}

export default function Dashboard() {
  const [scores, setScores] = useState<ScoreBucket[]>([])
  const [passRates, setPassRates] = useState<PassRate[]>([])
  const [timeline, setTimeline] = useState<TimelinePoint[]>([])
  const [groups, setGroups] = useState<GroupStat[]>([])
  const [error, setError] = useState<string>("")

  const lab = "lab-01"

  useEffect(() => {
    const headers = {
      Authorization: `Bearer ${TOKEN}`,
    }

    const fetchJson = async <T,>(url: string): Promise<T> => {
      const response = await fetch(url, { headers })
      if (!response.ok) {
        throw new Error(await response.text())
      }
      return response.json() as Promise<T>
    }

    Promise.all([
      fetchJson<ScoreBucket[]>(`${API}/scores?lab=${lab}`),
      fetchJson<PassRate[]>(`${API}/pass-rates?lab=${lab}`),
      fetchJson<TimelinePoint[]>(`${API}/timeline?lab=${lab}`),
      fetchJson<GroupStat[]>(`${API}/groups?lab=${lab}`),
    ])
      .then(([scoresData, passRatesData, timelineData, groupsData]) => {
        setScores(scoresData)
        setPassRates(passRatesData)
        setTimeline(timelineData)
        setGroups(groupsData)
      })
      .catch((err: unknown) => {
        const message =
          err instanceof Error ? err.message : "Unknown error"
        setError(message)
      })
  }, [])

  const scoreChartData = {
    labels: scores.map((item) => item.bucket),
    datasets: [
      {
        label: "Submissions",
        data: scores.map((item) => item.count),
      },
    ],
  }

  const timelineChartData = {
    labels: timeline.map((item) => item.date),
    datasets: [
      {
        label: "Submissions",
        data: timeline.map((item) => item.submissions),
      },
    ],
  }

  const passRateChartData = {
    labels: passRates.map((item) => item.task),
    datasets: [
      {
        label: "Average score",
        data: passRates.map((item) => Number(item.avg_score.toFixed(1))),
      },
    ],
  }

  const groupsChartData = {
    labels: groups.map((item) => item.group),
    datasets: [
      {
        label: "Students",
        data: groups.map((item) => item.students),
      },
    ],
  }

  return (
    <div style={{ padding: "20px" }}>
      <h1>Lab Analytics Dashboard</h1>

      {error && <p style={{ color: "red" }}>Error: {error}</p>}

      <h2>Score Distribution</h2>
      <Bar data={scoreChartData} />

      <h2>Pass Rates</h2>
      <Bar data={passRateChartData} />

      <h2>Timeline</h2>
      <Line data={timelineChartData} />

      <h2>Groups</h2>
      <Pie data={groupsChartData} />
    </div>
  )
}