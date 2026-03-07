import React, { useState } from 'react';
import Items from './Items';      // Существующий компонент со списком элементов
import Dashboard from './Dashboard'; // Наш новый компонент

function App() {
  // Состояние для отслеживания текущей страницы
  // Может быть либо 'items', либо 'dashboard'
  const [currentPage, setCurrentPage] = useState<'items' | 'dashboard'>('items');

  // Стили для кнопок навигации
  const buttonStyle = {
    padding: '10px 20px',
    marginRight: '10px',
    border: 'none',
    borderRadius: '5px',
    cursor: 'pointer',
    fontSize: '16px',
  };

  const activeButtonStyle = {
    ...buttonStyle,
    backgroundColor: '#007bff',
    color: 'white',
    fontWeight: 'bold' as const,
  };

  const inactiveButtonStyle = {
    ...buttonStyle,
    backgroundColor: '#f0f0f0',
    color: '#333',
  };

  return (
    <div>
      {/* Панель навигации */}
      <nav style={{ 
        padding: '15px 20px', 
        borderBottom: '2px solid #007bff',
        backgroundColor: '#f8f9fa',
        marginBottom: '20px'
      }}>
        <button
          onClick={() => setCurrentPage('items')}
          style={currentPage === 'items' ? activeButtonStyle : inactiveButtonStyle}
        >
          📋 Items
        </button>
        <button
          onClick={() => setCurrentPage('dashboard')}
          style={currentPage === 'dashboard' ? activeButtonStyle : inactiveButtonStyle}
        >
          📊 Dashboard
        </button>
      </nav>

      {/* Условный рендеринг страниц */}
      {currentPage === 'items' && <Items />}
      {currentPage === 'dashboard' && <Dashboard />}
    </div>
  );
}

export default App;