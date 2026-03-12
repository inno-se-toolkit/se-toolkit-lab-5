import React from 'react';
import { Bar } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
} from 'chart.js';

ChartJS.register(CategoryScale, LinearScale, BarElement, Title, Tooltip);

export const Dashboard: React.FC = () => {
  const data = {
    labels: ['Task 1', 'Task 2', 'Task 3'],
    datasets: [
      {
        label: 'Average Score',
        data: [80, 95, 60],
        backgroundColor: 'rgba(54, 162, 235, 0.6)',
      },
    ],
  };

  return (
    <div>
      <h2>Analytics Dashboard</h2>
      <Bar data={data} />
    </div>
  );
};
