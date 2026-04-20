import { useEffect, useRef } from "react";
import { createChart, LineSeries, type IChartApi } from "lightweight-charts";

interface Point {
  date: string;
  value: number;
}

interface Props {
  data: Point[];
  height?: number;
}

export default function EquityCurve({ data, height = 300 }: Props) {
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
      timeScale: {
        borderColor: "#2a2a35",
      },
      rightPriceScale: {
        borderColor: "#2a2a35",
      },
    });
    chartRef.current = chart;

    const lineSeries = chart.addSeries(LineSeries, {
      color: "#3b82f6",
      lineWidth: 2,
      priceFormat: {
        type: "custom",
        formatter: (price: number) => `${((price - 1) * 100).toFixed(1)}%`,
      },
    });

    lineSeries.setData(
      data.map((d) => ({ time: d.date, value: d.value })) as any
    );

    // Baseline at 1.0
    lineSeries.createPriceLine({
      price: 1.0,
      color: "#8a8a9a",
      lineWidth: 1,
      lineStyle: 2,
      axisLabelVisible: false,
    });

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
  }, [data, height]);

  return <div ref={containerRef} />;
}
