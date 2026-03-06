import { useState, useEffect, useReducer, FormEvent } from 'react'
import './App.css'
import Dashboard from './Dashboard'

const STORAGE_KEY = 'api_key'

interface Item {
  id: number
  type: string
  title: string
  created_at: string
}

interface Lab {
  id: string
  name: string
}

type FetchState<T> =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; data: T }
  | { status: 'error'; message: string }

type FetchAction =
  | { type: 'fetch_start' }
  | { type: 'fetch_success'; data: unknown }
  | { type: 'fetch_error'; message: string }

function fetchReducer<T>(
  _state: FetchState<T>,
  action: FetchAction,
): FetchState<T> {
  switch (action.type) {
    case 'fetch_start':
      return { status: 'loading' }
    case 'fetch_success':
      return { status: 'success', data: action.data as T }
    case 'fetch_error':
      return { status: 'error', message: action.message }
  }
}

function App() {
  const [token, setToken] = useState(
    () => localStorage.getItem(STORAGE_KEY) ?? '',
  )
  const [draft, setDraft] = useState('')
  const [fetchState, dispatch] = useReducer(fetchReducer<Item[]>, {
    status: 'idle',
  })
  const [labsState, setLabsState] = useState<FetchState<Lab[]>>({
    status: 'idle',
  })

  useEffect(() => {
    if (!token) return

    dispatch({ type: 'fetch_start' })
    setLabsState({ status: 'loading' })

    Promise.all([
      fetch('/items/', {
        headers: { Authorization: `Bearer ${token}` },
      })
        .then((res) => {
          if (!res.ok) throw new Error(`HTTP ${res.status}`)
          return res.json()
        })
        .then((data: Item[]) => dispatch({ type: 'fetch_success', data })),
      fetch('/labs/', {
        headers: { Authorization: `Bearer ${token}` },
      })
        .then((res) => {
          if (!res.ok) throw new Error(`HTTP ${res.status}`)
          return res.json()
        })
        .then((data: Lab[]) => setLabsState({ status: 'success', data }))
        .catch((err: Error) =>
          setLabsState({ status: 'error', message: err.message }),
        ),
    ]).catch((err: Error) =>
      dispatch({ type: 'fetch_error', message: err.message }),
    )
  }, [token])

  function handleConnect(e: FormEvent) {
    e.preventDefault()
    const trimmed = draft.trim()
    if (!trimmed) return
    localStorage.setItem(STORAGE_KEY, trimmed)
    setToken(trimmed)
  }

  function handleDisconnect() {
    localStorage.removeItem(STORAGE_KEY)
    setToken('')
    setDraft('')
  }

  if (!token) {
    return (
      <form className="token-form" onSubmit={handleConnect}>
        <h1>API Key</h1>
        <p>Enter your API key to connect.</p>
        <input
          type="password"
          placeholder="Token"
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
        />
        <button type="submit">Connect</button>
      </form>
    )
  }

  return (
    <div>
      <header className="app-header">
        <h1>LMS Dashboard</h1>
        <button className="btn-disconnect" onClick={handleDisconnect}>
          Disconnect
        </button>
      </header>

      {fetchState.status === 'loading' && <p>Loading...</p>}
      {fetchState.status === 'error' && <p>Error: {fetchState.message}</p>}
      {labsState.status === 'error' && <p>Error: {labsState.message}</p>}

      {labsState.status === 'success' && labsState.data.length > 0 ? (
        <Dashboard labs={labsState.data} />
      ) : labsState.status === 'success' ? (
        <p>No labs available</p>
      ) : null}

      {fetchState.status === 'success' && (
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>ItemType</th>
              <th>Title</th>
              <th>Created at</th>
            </tr>
          </thead>
          <tbody>
            {fetchState.data.map((item) => (
              <tr key={item.id}>
                <td>{item.id}</td>
                <td>{item.type}</td>
                <td>{item.title}</td>
                <td>{item.created_at}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  )
}

export default App
