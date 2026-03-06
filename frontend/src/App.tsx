import React, { useState } from 'react';
import { Dashboard } from './Dashboard';

const App: React.FC = () => {
  const [view, setView] = useState<'items' | 'dashboard'>('items');

  return (
    <div style={{ padding: '20px' }}>
      <nav style={{ marginBottom: '20px', borderBottom: '1px solid #ccc', paddingBottom: '10px' }}>
        <button onClick={() => setView('items')} style={{ marginRight: '10px' }}>Items Page</button>
        <button onClick={() => setView('dashboard')}>Analytics Dashboard</button>
      </nav>

      {view === 'dashboard' ? (
        <Dashboard />
      ) : (
        <div>
          <h1>Items Page</h1>
          <p>This is where your items list would be.</p>
        </div>
      )}
    </div>
  );
};

export default App;
