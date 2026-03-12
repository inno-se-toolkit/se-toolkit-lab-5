import React, { useState } from 'react';
import { Dashboard } from './Dashboard';

export const App: React.FC = () => {
  const [token, setToken] = useState<string>('');
  const [draft, setDraft] = useState<string>('');
  const [view, setView] = useState<'items' | 'dashboard'>('items');

  const handleConnect = (e: React.FormEvent) => {
    e.preventDefault();
    setToken(draft);
  };

  if (!token) {
    return (
      <form onSubmit={handleConnect} style={{ padding: '20px' }}>
        <h1>API Key</h1>
        <p>Enter your API key to connect.</p>
        <input
          type="password"
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          placeholder="Token"
        />
        <button type="submit">Connect</button>
      </form>
    );
  }

  return (
    <div style={{ padding: '20px' }}>
      <nav style={{ padding: '10px', background: '#eee', marginBottom: '20px' }}>
        <button onClick={() => setView('items')} style={{ marginRight: '10px' }}>Items</button>
        <button onClick={() => setView('dashboard')}>Dashboard</button>
      </nav>

      {view === 'items' ? (
        <div>
          <h2>Items Page</h2>
          <p>Your items list goes here.</p>
        </div>
      ) : (
        <Dashboard />
      )}
    </div>
  );
};
