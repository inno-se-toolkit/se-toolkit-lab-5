import React from 'react';
import { Chart as ChartJS, CategoryScale, LinearScale, BarElement, Title, Tooltip, Legend } from 'chart.js';
import { Bar } from 'react-chartjs-2';

ChartJS.register(CategoryScale, LinearScale, BarElement, Title, Tooltip, Legend);

export const Dashboard: React.FC = () => {
  const data = {
    labels: ['January', 'February', 'March'],
    datasets: [{ 
      label: 'Performance', 
      data: [65, 59, 80], 
      backgroundColor: 'rgba(75, 192, 192, 0.5)' 
    }]
  };

  return (
    <div style={{ padding: '20px' }}>
      <h2>Analytics Dashboard</h2>
      <Bar data={data} />
    </div>
  );
};
