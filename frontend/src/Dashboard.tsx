import { useEffect, useState } from "react"

interface Analytics {
  total_items: number
  items_last_24h: number
}

export default function Dashboard() {
  const [data, setData] = useState<Analytics | null>(null)

  useEffect(() => {
    fetch("/analytics/items")
      .then(res => res.json())
      .then(setData)
  }, [])

  if (!data) {
    return <p>Loading analytics...</p>
  }

  return (
    <div>
      <h2>Analytics</h2>
      <p>Total items: {data.total_items}</p>
      <p>Items in last 24h: {data.items_last_24h}</p>
    </div>
  )
}