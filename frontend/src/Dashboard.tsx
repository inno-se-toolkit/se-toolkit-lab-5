import React from 'react';
import { Chart as ChartJS, CategoryScale, LinearScale, BarElement, Title, Tooltip, Legend } from 'chart.js';
import { Bar } from 'react-chartjs-2';

ChartJS.register(CategoryScale, LinearScale, BarElement, Title, Tooltip, Legend);

export const Dashboard: React.FC = () => {
  const data = {
    labels: ['Completed', 'Pending'],
    datasets: [{ label: 'Tasks', data: [75, 25] }]
  };
  return (
    <div>
      <h2>Analytics Dashboard</h2>
      <Bar data={data} />
    </div>
  );
};
