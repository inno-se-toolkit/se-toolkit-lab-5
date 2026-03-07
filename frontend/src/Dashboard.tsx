import React from "react";
import { Bar } from "react-chartjs-2";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend,
} from "chart.js";

ChartJS.register(CategoryScale, LinearScale, BarElement, Title, Tooltip, Legend);

const Dashboard: React.FC = () => {
  const data = {
    labels: ["Jan", "Feb", "Mar"],
    datasets: [
      {
        label: "Items created",
        data: [5, 10, 7],
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

export default Dashboard;