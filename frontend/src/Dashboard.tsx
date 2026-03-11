import React, { FC, useState, useEffect } from 'react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js';
import { Bar, Line } from 'react-chartjs-2';

// Register Chart.js components
ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
);

// ---------------------------------------------------------------------------
// TypeScript interfaces for API responses
// ---------------------------------------------------------------------------

interface ScoresBucket {
  bucket: string;
  count: number;
}

interface TimelineEntry {
  date: string;
  submissions: number;
}

interface PassRateEntry {
  task: string;
  avg_score: number;
  attempts: number;
}

interface DashboardData {
  scores: ScoresBucket[] | null;
  timeline: TimelineEntry[] | null;
  passRates: PassRateEntry[] | null;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const LAB_OPTIONS: string[] = ['lab-01', 'lab-02', 'lab-03', 'lab-04'];
const DEFAULT_LAB = 'lab-04';
const API_BASE_URL = '/analytics';

// ---------------------------------------------------------------------------
// Helper function to fetch with Bearer token auth
// ---------------------------------------------------------------------------

async function fetchWithAuth(url: string): Promise<Response> {
  const apiKey = localStorage.getItem('api_key') || '';
  return fetch(url, {
    headers: {
      Authorization: `Bearer ${apiKey}`,
    },
  });
}

// ---------------------------------------------------------------------------
// Dashboard Component
// ---------------------------------------------------------------------------

const Dashboard: FC = () => {
  // State for selected lab
  const [selectedLab, setSelectedLab] = useState<string>(DEFAULT_LAB);

  // State for loading and error
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  // State for fetched data
  const [data, setData] = useState<DashboardData>({
    scores: null,
    timeline: null,
    passRates: null,
  });

  // Fetch data when component mounts or lab changes
  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      setError(null);

      try {
        // Fetch all three endpoints in parallel
        const [scoresRes, timelineRes, passRatesRes] = await Promise.all([
          fetchWithAuth(`${API_BASE_URL}/scores?lab=${selectedLab}`),
          fetchWithAuth(`${API_BASE_URL}/timeline?lab=${selectedLab}`),
          fetchWithAuth(`${API_BASE_URL}/pass-rates?lab=${selectedLab}`),
        ]);

        // Check for errors
        if (!scoresRes.ok) {
          throw new Error(`Failed to fetch scores: ${scoresRes.status}`);
        }
        if (!timelineRes.ok) {
          throw new Error(`Failed to fetch timeline: ${timelineRes.status}`);
        }
        if (!passRatesRes.ok) {
          throw new Error(`Failed to fetch pass rates: ${passRatesRes.status}`);
        }

        // Parse JSON responses
        const scoresData: ScoresBucket[] = await scoresRes.json();
        const timelineData: TimelineEntry[] = await timelineRes.json();
        const passRatesData: PassRateEntry[] = await passRatesRes.json();

        setData({
          scores: scoresData,
          timeline: timelineData,
          passRates: passRatesData,
        });
      } catch (err) {
        const errorMessage = err instanceof Error ? err.message : 'Unknown error occurred';
        setError(errorMessage);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [selectedLab]);

  // Handle loading state
  if (loading) {
    return <div className="dashboard-loading">Loading...</div>;
  }

  // Handle error state
  if (error) {
    return <div className="dashboard-error">Error: {error}</div>;
  }

  // -----------------------------------------------------------------------
  // Prepare chart data for score distribution (Bar chart)
  // -----------------------------------------------------------------------

  const scoresLabels = data.scores?.map((item) => item.bucket) || [];
  const scoresCounts = data.scores?.map((item) => item.count) || [];

  const scoresChartData = {
    labels: scoresLabels,
    datasets: [
      {
        label: 'Number of Students',
        data: scoresCounts,
        backgroundColor: 'rgba(54, 162, 235, 0.6)',
        borderColor: 'rgba(54, 162, 235, 1)',
        borderWidth: 1,
      },
    ],
  };

  const scoresChartOptions = {
    responsive: true,
    plugins: {
      legend: {
        display: false,
      },
      title: {
        display: true,
        text: 'Score Distribution',
      },
    },
    scales: {
      y: {
        beginAtZero: true,
        ticks: {
          stepSize: 1,
        },
      },
    },
  };

  // -----------------------------------------------------------------------
  // Prepare chart data for timeline (Line chart)
  // -----------------------------------------------------------------------

  const timelineLabels = data.timeline?.map((item) => item.date) || [];
  const timelineSubmissions = data.timeline?.map((item) => item.submissions) || [];

  const timelineChartData = {
    labels: timelineLabels,
    datasets: [
      {
        label: 'Submissions',
        data: timelineSubmissions,
        backgroundColor: 'rgba(75, 192, 192, 0.2)',
        borderColor: 'rgba(75, 192, 192, 1)',
        borderWidth: 2,
        tension: 0.3,
      },
    ],
  };

  const timelineChartOptions = {
    responsive: true,
    plugins: {
      legend: {
        display: false,
      },
      title: {
        display: true,
        text: 'Submissions Over Time',
      },
    },
    scales: {
      y: {
        beginAtZero: true,
        ticks: {
          stepSize: 1,
        },
      },
    },
  };

  // -----------------------------------------------------------------------
  // Render component
  // -----------------------------------------------------------------------

  return (
    <div className="dashboard">
      <h1>Dashboard</h1>

      {/* Lab Selector Dropdown */}
      <div className="lab-selector">
        <label htmlFor="lab-select">Select Lab: </label>
        <select
          id="lab-select"
          value={selectedLab}
          onChange={(e) => setSelectedLab(e.target.value)}
        >
          {LAB_OPTIONS.map((lab) => (
            <option key={lab} value={lab}>
              {lab}
            </option>
          ))}
        </select>
      </div>

      {/* Score Distribution Bar Chart */}
      <div className="chart-container">
        <Bar data={scoresChartData} options={scoresChartOptions} />
      </div>

      {/* Timeline Line Chart */}
      <div className="chart-container">
        <Line data={timelineChartData} options={timelineChartOptions} />
      </div>

      {/* Pass Rates Table */}
      <div className="table-container">
        <h2>Pass Rates by Task</h2>
        <table>
          <thead>
            <tr>
              <th>Task</th>
              <th>Avg Score</th>
              <th>Attempts</th>
            </tr>
          </thead>
          <tbody>
            {data.passRates?.map((entry) => (
              <tr key={entry.task}>
                <td>{entry.task}</td>
                <td>{entry.avg_score.toFixed(1)}</td>
                <td>{entry.attempts}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default Dashboard;
