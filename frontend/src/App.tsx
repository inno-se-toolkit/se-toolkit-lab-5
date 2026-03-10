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

type ViewMode = 'items' | 'dashboard'

type FetchState =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; items: Item[] }
  | { status: 'error'; message: string }

type FetchAction =
  | { type: 'fetch_start' }
  | { type: 'fetch_success'; data: Item[] }
  | { type: 'fetch_error'; message: string }

function fetchReducer(_state: FetchState, action: FetchAction): FetchState {
  switch (action.type) {
    case 'fetch_start':
      return { status: 'loading' }
    case 'fetch_success':
      return { status: 'success', items: action.data }
    case 'fetch_error':
      return { status: 'error', message: action.message }
    default:
      return { status: 'idle' }
  }
}

function App() {
  const [token, setToken] = useState<string>(
    () => localStorage.getItem(STORAGE_KEY) ?? '',
  )
  const [draft, setDraft] = useState<string>('')
  const [view, setView] = useState<ViewMode>('items')
  const [fetchState, dispatch] = useReducer(fetchReducer, { status: 'idle' })

  useEffect(() => {
    if (!token) {
      dispatch({ type: 'fetch_error', message: 'API key is missing.' })
      return
    }

    dispatch({ type: 'fetch_start' })

    fetch('/items/', {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    })
      .then((res) => {
        if (!res.ok) {
          throw new Error(`HTTP ${res.status}`)
        }

        return res.json()
      })
      .then((data: Item[]) => {
        dispatch({ type: 'fetch_success', data })
      })
      .catch((err: Error) => {
        dispatch({ type: 'fetch_error', message: err.message })
      })
  }, [token])

  function handleConnect(e: FormEvent<HTMLFormElement>) {
    e.preventDefault()

    const trimmed = draft.trim()
    if (!trimmed) {
      return
    }

    localStorage.setItem(STORAGE_KEY, trimmed)
    setToken(trimmed)
    setDraft('')
    setView('items')
  }

  function handleDisconnect() {
    localStorage.removeItem(STORAGE_KEY)
    setToken('')
    setDraft('')
    setView('items')
    dispatch({ type: 'fetch_error', message: 'API key is missing.' })
  }

  if (!token) {
    return (
      <main className="app">
        <section
          style={{
            maxWidth: 480,
            margin: '48px auto',
            padding: 24,
            border: '1px solid #ddd',
            borderRadius: 12,
            background: '#fff',
          }}
        >
          <h1>Connect API</h1>
          <p>Paste your API key to access items and analytics.</p>

          <form
            onSubmit={handleConnect}
            style={{ display: 'grid', gap: 12, marginTop: 16 }}
          >
            <input
              type="password"
              value={draft}
              onChange={(e) => setDraft(e.target.value)}
              placeholder="Enter API key"
            />
            <button type="submit">Connect</button>
          </form>
        </section>
      </main>
    )
  }

  return (
    <main className="app">
      <header
        className="app-header"
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          gap: 16,
          flexWrap: 'wrap',
          marginBottom: 24,
        }}
      >
        <div>
          <h1>Lab 5 App</h1>
          <p>Browse items or open the analytics dashboard.</p>
        </div>

        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
          <button
            type="button"
            onClick={() => setView('items')}
            disabled={view === 'items'}
          >
            Items
          </button>
          <button
            type="button"
            onClick={() => setView('dashboard')}
            disabled={view === 'dashboard'}
          >
            Dashboard
          </button>
          <button type="button" onClick={handleDisconnect}>
            Disconnect
          </button>
        </div>
      </header>

      {view === 'items' && (
        <section>
          <h2>Items</h2>

          {fetchState.status === 'loading' && <p>Loading...</p>}

          {fetchState.status === 'error' && (
            <p role="alert">Error: {fetchState.message}</p>
          )}

          {fetchState.status === 'success' && (
            <>
              {fetchState.items.length === 0 ? (
                <p>No items found.</p>
              ) : (
                <table>
                  <thead>
                    <tr>
                      <th>ID</th>
                      <th>Item type</th>
                      <th>Title</th>
                      <th>Created at</th>
                    </tr>
                  </thead>
                  <tbody>
                    {fetchState.items.map((item) => (
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
            </>
          )}
        </section>
      )}

      {view === 'dashboard' && <Dashboard />}
    </main>
  )
}

export default App
