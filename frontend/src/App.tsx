import { Routes, Route, Navigate } from "react-router-dom";
import Layout from "./components/Layout";
import SignalsPage from "./pages/SignalsPage";
import BacktestPage from "./pages/BacktestPage";
import FundsPage from "./pages/FundsPage";
import FundDetailPage from "./pages/FundDetailPage";
import DualTrackPage from "./pages/DualTrackPage";
import StockPage from "./pages/StockPage";
import TimelinePage from "./pages/TimelinePage";
import DnaPage from "./pages/DnaPage";
import FlowPage from "./pages/FlowPage";

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route path="/" element={<Navigate to="/signals" replace />} />
        <Route path="/signals" element={<SignalsPage />} />
        <Route path="/backtest" element={<BacktestPage />} />
        <Route path="/funds" element={<FundsPage />} />
        <Route path="/fund/:code" element={<FundDetailPage />} />
        <Route path="/dual-track" element={<DualTrackPage />} />
        <Route path="/stock/:ticker" element={<StockPage />} />
        <Route path="/timeline" element={<TimelinePage />} />
        <Route path="/dna" element={<DnaPage />} />
        <Route path="/flow" element={<FlowPage />} />
      </Route>
    </Routes>
  );
}
