import React, { useState } from "react";
import Dashboard from "./Dashboard";

function App() {
  const [page, setPage] = useState<"items" | "dashboard">("items");

  return (
    <div>
      <h1>LMS</h1>

      <button onClick={() => setPage("items")}>Items</button>
      <button onClick={() => setPage("dashboard")}>Dashboard</button>

      {page === "items" && <div>Items Page</div>}
      {page === "dashboard" && <Dashboard />}
    </div>
  );
}

export default App;