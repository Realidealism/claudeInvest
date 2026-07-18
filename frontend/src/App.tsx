import { Routes, Route, Navigate } from "react-router-dom";
import Layout from "./components/Layout";
import SignalsPage from "./pages/SignalsPage";
import FundsPage from "./pages/FundsPage";
import FundDetailPage from "./pages/FundDetailPage";
import DualTrackPage from "./pages/DualTrackPage";
import FlowPage from "./pages/FlowPage";
import ChipPicksPage from "./pages/ChipPicksPage";
import HermitPage from "./pages/HermitPage";
import RevenueScreensPage from "./pages/RevenueScreensPage";
import ScoresPage from "./pages/ScoresPage";
import OperationsPage from "./pages/OperationsPage";
import PositionsPage from "./pages/PositionsPage";
import StockPage from "./pages/StockPage";
import BreadthPage from "./pages/BreadthPage";
import MarginPage from "./pages/MarginPage";
import FearGreedPage from "./pages/FearGreedPage";
import VixPage from "./pages/VixPage";
import YieldCurvePage from "./pages/YieldCurvePage";
import FtseTaiwanPage from "./pages/FtseTaiwanPage";
import CommoditiesPage from "./pages/CommoditiesPage";
import InsiderPledgePage from "./pages/InsiderPledgePage";

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route path="/" element={<Navigate to="/signals" replace />} />
        <Route path="/signals" element={<SignalsPage />} />
        <Route path="/funds" element={<FundsPage />} />
        <Route path="/fund/:code" element={<FundDetailPage />} />
        <Route path="/dual-track" element={<DualTrackPage />} />
        <Route path="/flow" element={<FlowPage />} />
        <Route path="/chip-picks" element={<ChipPicksPage />} />
        <Route path="/hermit" element={<HermitPage />} />
        <Route path="/revenue" element={<RevenueScreensPage />} />
        <Route path="/scores" element={<ScoresPage />} />
        <Route path="/operations" element={<OperationsPage />} />
        <Route path="/positions" element={<PositionsPage />} />
        <Route path="/stock/:ticker" element={<StockPage />} />
        <Route path="/breadth" element={<BreadthPage />} />
        <Route path="/margin" element={<MarginPage />} />
        <Route path="/fear-greed" element={<FearGreedPage />} />
        <Route path="/vix" element={<VixPage />} />
        <Route path="/yield-curve" element={<YieldCurvePage />} />
        <Route path="/ftse-taiwan" element={<FtseTaiwanPage />} />
        <Route path="/commodities" element={<CommoditiesPage />} />
        <Route path="/insider-pledge" element={<InsiderPledgePage />} />
      </Route>
    </Routes>
  );
}
