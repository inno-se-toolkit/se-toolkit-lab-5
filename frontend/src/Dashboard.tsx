import { useEffect, useState } from "react"

const API = "/analytics"
const TOKEN = "my-secret-api-key"

export default function Dashboard() {
  const [scores, setScores] = useState([])
  const [passRates, setPassRates] = useState([])
  const [timeline, setTimeline] = useState([])
  const [groups, setGroups] = useState([])

  const lab = "lab-01"

  useEffect(() => {
    const headers = {
      Authorization: `Bearer ${TOKEN}`,
    }

    fetch(`${API}/scores?lab=${lab}`, { headers })
      .then(r => r.json())
      .then(setScores)

    fetch(`${API}/pass-rates?lab=${lab}`, { headers })
      .then(r => r.json())
      .then(setPassRates)

    fetch(`${API}/timeline?lab=${lab}`, { headers })
      .then(r => r.json())
      .then(setTimeline)

    fetch(`${API}/groups?lab=${lab}`, { headers })
      .then(r => r.json())
      .then(setGroups)
  }, [])

  return (
    <div style={{ padding: "20px" }}>
      <h1>Lab Analytics Dashboard</h1>

      <h2>Score Distribution</h2>
      <pre>{JSON.stringify(scores, null, 2)}</pre>

      <h2>Pass Rates</h2>
      <pre>{JSON.stringify(passRates, null, 2)}</pre>

      <h2>Timeline</h2>
      <pre>{JSON.stringify(timeline, null, 2)}</pre>

      <h2>Groups</h2>
      <pre>{JSON.stringify(groups, null, 2)}</pre>
    </div>
  )
}