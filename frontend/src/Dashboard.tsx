// Импортируем необходимые хуки из React
import React, { useState, useEffect } from 'react';

// Импортируем компоненты графиков из react-chartjs-2
import { Bar, Line } from 'react-chartjs-2';

// Импортируем все необходимые компоненты Chart.js
import {
  Chart as ChartJS,
  CategoryScale,     // Для категорий на оси X (например, названия корзин)
  LinearScale,       // Для числовых значений на оси Y
  BarElement,        // Для отрисовки столбцов
  LineElement,       // Для отрисовки линий
  PointElement,      // Для точек на линейном графике
  Title,             // Для заголовков графиков
  Tooltip,           // Для всплывающих подсказок
  Legend,            // Для легенды графика
} from 'chart.js';

// !!! ВАЖНО: Регистрируем все компоненты Chart.js
// Без этого графики не будут работать
ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend
);

// --- ОПРЕДЕЛЯЕМ ТИПЫ ДАННЫХ (это важно для TypeScript, никакого 'any'!) ---

// Тип для данных из /analytics/scores
interface ScoreBucket {
  bucket: string;   // например "0-25", "26-50"
  count: number;    // количество работ в этом диапазоне
}

// Тип для данных из /analytics/pass-rates
interface TaskPassRate {
  task: string;     // название задания
  avg_score: number; // средняя оценка
  attempts: number;  // количество попыток
}

// Тип для данных из /analytics/timeline
interface DailySubmission {
  date: string;     // дата в формате YYYY-MM-DD
  submissions: number; // количество работ в этот день
}

// Тип для пропсов компонента Dashboard
interface DashboardProps {
  labId?: string;   // ID лабораторной работы (необязательный, по умолчанию 'lab-04')
}

// --- ОСНОВНОЙ КОМПОНЕНТ ---
const Dashboard: React.FC<DashboardProps> = ({ labId = 'lab-04' }) => {
  // --- СОСТОЯНИЯ КОМПОНЕНТА ---
  
  // Выбранная лабораторная работа
  const [selectedLab, setSelectedLab] = useState<string>(labId);
  
  // Данные для графиков
  const [scoreData, setScoreData] = useState<ScoreBucket[]>([]);
  const [timelineData, setTimelineData] = useState<DailySubmission[]>([]);
  const [passRatesData, setPassRatesData] = useState<TaskPassRate[]>([]);
  
  // Состояния загрузки и ошибок
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  // --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
  
  // Функция для получения токена из localStorage
  // Токен сохраняется там после логина через Items страницу
  const getAuthToken = (): string | null => {
    return localStorage.getItem('api_key');
  };

  // --- ЗАГРУЗКА ДАННЫХ ПРИ ИЗМЕНЕНИИ selectedLab ---
  useEffect(() => {
    // Объявляем асинхронную функцию для загрузки
    const fetchData = async () => {
      // Включаем состояние загрузки и сбрасываем ошибку
      setLoading(true);
      setError(null);
      
      // Получаем токен
      const token = getAuthToken();

      // Если токена нет - показываем ошибку
      if (!token) {
        setError('API key not found. Please log in on the Items page first.');
        setLoading(false);
        return;
      }

      // Заголовки для HTTP-запросов
      const headers = {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json',
      };

      // Базовый URL API из переменных окружения (настраивается в .env файле)
      // В Vite переменные окружения должны начинаться с VITE_
      const apiBaseUrl = import.meta.env.VITE_API_TARGET || '';

      try {
        // Запускаем ВСЕ ТРИ запроса параллельно для скорости
        // Promise.all ждет выполнения всех промисов
        const [scoresRes, timelineRes, passRatesRes] = await Promise.all([
          fetch(`${apiBaseUrl}/analytics/scores?lab=${selectedLab}`, { headers }),
          fetch(`${apiBaseUrl}/analytics/timeline?lab=${selectedLab}`, { headers }),
          fetch(`${apiBaseUrl}/analytics/pass-rates?lab=${selectedLab}`, { headers }),
        ]);

        // Проверяем, что все запросы успешны (статус 200-299)
        if (!scoresRes.ok || !timelineRes.ok || !passRatesRes.ok) {
          throw new Error('Failed to fetch analytics data');
        }

        // Преобразуем ответы в JSON
        const scoresJson = await scoresRes.json();
        const timelineJson = await timelineRes.json();
        const passRatesJson = await passRatesRes.json();

        // Сохраняем данные в состояние
        setScoreData(scoresJson);
        setTimelineData(timelineJson);
        setPassRatesData(passRatesJson);
      } catch (err) {
        // Обрабатываем ошибки
        setError(err instanceof Error ? err.message : 'An unknown error occurred');
      } finally {
        // В любом случае выключаем состояние загрузки
        setLoading(false);
      }
    };

    fetchData();
  }, [selectedLab]); // Эффект перезапускается при изменении selectedLab

  // --- ПОДГОТОВКА ДАННЫХ ДЛЯ ГРАФИКОВ CHART.JS ---

  // Данные для гистограммы (распределение оценок)
  const barChartData = {
    // Метки для оси X (названия корзин)
    labels: scoreData.map(item => item.bucket),
    // Наборы данных (у нас один набор)
    datasets: [
      {
        label: 'Number of Submissions',
        data: scoreData.map(item => item.count),
        backgroundColor: 'rgba(53, 162, 235, 0.5)', // Полупрозрачный синий
      },
    ],
  };

  // Данные для линейного графика (активность по дням)
  const lineChartData = {
    // Метки для оси X (даты)
    labels: timelineData.map(item => item.date),
    datasets: [
      {
        label: 'Submissions per Day',
        data: timelineData.map(item => item.submissions),
        borderColor: 'rgb(75, 192, 192)', // Бирюзовый цвет линии
        backgroundColor: 'rgba(75, 192, 192, 0.2)', // Полупрозрачная заливка
        tension: 0.1, // Небольшое сглаживание линии
      },
    ],
  };

  // Общие опции для графиков
  const chartOptions = {
    responsive: true, // График подстраивается под размер контейнера
    plugins: {
      legend: {
        position: 'top' as const, // Легенда сверху
      },
    },
  };

  // --- ОТРИСОВКА КОМПОНЕНТА (JSX) ---
  
  return (
    <div style={{ padding: '20px' }}>
      <h1>Analytics Dashboard</h1>

      {/* БЛОК ВЫБОРА ЛАБОРАТОРНОЙ РАБОТЫ */}
      <div style={{ marginBottom: '20px', padding: '10px', backgroundColor: '#f5f5f5', borderRadius: '5px' }}>
        <label htmlFor="lab-select" style={{ marginRight: '10px', fontWeight: 'bold' }}>
          Select Lab:
        </label>
        <select
          id="lab-select"
          value={selectedLab}
          onChange={(e) => setSelectedLab(e.target.value)}
          style={{ padding: '5px', borderRadius: '3px', border: '1px solid #ccc' }}
        >
          <option value="lab-01">Lab 01</option>
          <option value="lab-02">Lab 02</option>
          <option value="lab-03">Lab 03</option>
          <option value="lab-04">Lab 04</option>
          <option value="lab-05">Lab 05</option>
        </select>
      </div>

      {/* БЛОК ЗАГРУЗКИ - показываем, пока loading = true */}
      {loading && (
        <div style={{ textAlign: 'center', padding: '40px' }}>
          <p>Loading dashboard data...</p>
        </div>
      )}

      {/* БЛОК ОШИБКИ - показываем, если error не null */}
      {error && (
        <div style={{ color: 'red', padding: '20px', border: '1px solid red', borderRadius: '5px', marginBottom: '20px' }}>
          <strong>Error:</strong> {error}
        </div>
      )}

      {/* БЛОК С ГРАФИКАМИ - показываем, если не грузится и нет ошибки */}
      {!loading && !error && (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '20px' }}>
          
          {/* ЛЕВАЯ КОЛОНКА: Гистограмма */}
          <div style={{ border: '1px solid #ddd', padding: '15px', borderRadius: '8px', backgroundColor: 'white' }}>
            <h2>Score Distribution</h2>
            {scoreData.length > 0 ? (
              <Bar data={barChartData} options={chartOptions} />
            ) : (
              <p style={{ color: '#666', fontStyle: 'italic' }}>No score data available for this lab.</p>
            )}
          </div>

          {/* ПРАВАЯ КОЛОНКА: Линейный график */}
          <div style={{ border: '1px solid #ddd', padding: '15px', borderRadius: '8px', backgroundColor: 'white' }}>
            <h2>Submission Timeline</h2>
            {timelineData.length > 0 ? (
              <Line data={lineChartData} options={chartOptions} />
            ) : (
              <p style={{ color: '#666', fontStyle: 'italic' }}>No timeline data available for this lab.</p>
            )}
          </div>

          {/* НИЖНЯЯ ЧАСТЬ (НА ВСЮ ШИРИНУ): Таблица */}
          <div style={{ gridColumn: 'span 2', border: '1px solid #ddd', padding: '15px', borderRadius: '8px', backgroundColor: 'white' }}>
            <h2>Task Pass Rates</h2>
            {passRatesData.length > 0 ? (
              <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                <thead>
                  <tr style={{ backgroundColor: '#f2f2f2' }}>
                    <th style={{ border: '1px solid #ddd', padding: '8px', textAlign: 'left' }}>Task</th>
                    <th style={{ border: '1px solid #ddd', padding: '8px', textAlign: 'right' }}>Average Score</th>
                    <th style={{ border: '1px solid #ddd', padding: '8px', textAlign: 'right' }}>Attempts</th>
                  </tr>
                </thead>
                <tbody>
                  {passRatesData.map((item, index) => (
                    <tr key={index} style={{ borderBottom: '1px solid #ddd' }}>
                      <td style={{ padding: '8px' }}>{item.task}</td>
                      <td style={{ padding: '8px', textAlign: 'right' }}>{item.avg_score.toFixed(1)}</td>
                      <td style={{ padding: '8px', textAlign: 'right' }}>{item.attempts}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            ) : (
              <p style={{ color: '#666', fontStyle: 'italic' }}>No pass rate data available for this lab.</p>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

export default Dashboard;