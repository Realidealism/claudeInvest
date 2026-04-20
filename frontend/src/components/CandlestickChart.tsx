import { useEffect, useRef } from "react";
import { createChart, CandlestickSeries, HistogramSeries, createSeriesMarkers, type IChartApi } from "lightweight-charts";

interface OHLCV {
  t: string;
  o: number | null;
  h: number | null;
  l: number | null;
  c: number | null;
  v: number;
}

interface Signal {
  date: string;
  type: "long" | "short";
  label: string;
}

interface Props {
  data: OHLCV[];
  signals?: Signal[];
  height?: number;
}

export default function CandlestickChart({ data, signals, height = 400 }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);

  useEffect(() => {
    if (!containerRef.current || data.length === 0) return;

    const chart = createChart(containerRef.current, {
      height,
      layout: {
        background: { color: "#12121a" },
        textColor: "#8a8a9a",
      },
      grid: {
        vertLines: { color: "#1a1a25" },
        horzLines: { color: "#1a1a25" },
      },
      crosshair: {
        mode: 0,
      },
      timeScale: {
        borderColor: "#2a2a35",
      },
      rightPriceScale: {
        borderColor: "#2a2a35",
      },
    });
    chartRef.current = chart;

    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: "#ef4444",
      downColor: "#22c55e",
      borderUpColor: "#ef4444",
      borderDownColor: "#22c55e",
      wickUpColor: "#ef4444",
      wickDownColor: "#22c55e",
    });

    const candleData = data
      .filter((d) => d.o != null && d.c != null)
      .map((d) => ({
        time: d.t,
        open: d.o!,
        high: d.h!,
        low: d.l!,
        close: d.c!,
      }));
    candleSeries.setData(candleData as any);

    // Volume as histogram
    const volumeSeries = chart.addSeries(HistogramSeries, {
      priceFormat: { type: "volume" },
      priceScaleId: "",
    });
    volumeSeries.priceScale().applyOptions({
      scaleMargins: { top: 0.8, bottom: 0 },
    });
    const volumeData = data
      .filter((d) => d.c != null)
      .map((d) => ({
        time: d.t,
        value: d.v,
        color: (d.c! >= d.o!) ? "rgba(239, 68, 68, 0.3)" : "rgba(34, 197, 94, 0.3)",
      }));
    volumeSeries.setData(volumeData as any);

    // Signal markers
    if (signals && signals.length > 0) {
      const markers = signals.map((s) => ({
        time: s.date,
        position: s.type === "long" ? "belowBar" as const : "aboveBar" as const,
        color: s.type === "long" ? "#ef4444" : "#22c55e",
        shape: s.type === "long" ? "arrowUp" as const : "arrowDown" as const,
        text: s.label,
      }));
      createSeriesMarkers(candleSeries, markers as any);
    }

    chart.timeScale().fitContent();

    const observer = new ResizeObserver(() => {
      if (containerRef.current) {
        chart.applyOptions({ width: containerRef.current.clientWidth });
      }
    });
    observer.observe(containerRef.current);

    return () => {
      observer.disconnect();
      chart.remove();
    };
  }, [data, signals, height]);

  return <div ref={containerRef} />;
}
