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
import SearchPage from "./pages/SearchPage";
import HermitPage from "./pages/HermitPage";
import ScoresPage from "./pages/ScoresPage";
import OperationsPage from "./pages/OperationsPage";
import PositionsPage from "./pages/PositionsPage";

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
        <Route path="/search" element={<SearchPage />} />
        <Route path="/hermit" element={<HermitPage />} />
        <Route path="/scores" element={<ScoresPage />} />
        <Route path="/operations" element={<OperationsPage />} />
        <Route path="/positions" element={<PositionsPage />} />
      </Route>
    </Routes>
  );
}
