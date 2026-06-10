import { useEffect, useRef } from "react";
import { createChart, LineSeries, type IChartApi, type ISeriesApi } from "lightweight-charts";

export interface SeriesSpec {
  key: string;
  label: string;
  color: string;
  dashed?: boolean;
  lineWidth?: number;
  // Hide last value label / title on the price axis (for context lines like band/MA).
  hideLastValue?: boolean;
}

interface Props {
  data: Array<Record<string, number | string | null>>;
  series: SeriesSpec[];
  height?: number;
  // "pct" → 0.34 → 34.0%, "int" → 12345, "raw" → as-is to 2 dp,
  // "kTwd" → 仟元金額換算成 億 / 兆
  format?: "pct" | "int" | "raw" | "kTwd";
  // 設了之後不呼叫 fitContent，改用固定 bar 間距，讓視窗只顯示能裝下的最右側 bars
  barSpacing?: number;
  // crosshair 移到第 N 個 bar 時觸發；離開 chart 給 null
  onCrosshairMove?: (idx: number | null) => void;
  // 外部設定 crosshair 位置（用來跨圖同步）
  externalCrosshairIdx?: number | null;
}

function fmt(v: number, mode: "pct" | "int" | "raw" | "kTwd"): string {
  if (mode === "pct") return `${(v * 100).toFixed(1)}%`;
  if (mode === "int") return v.toLocaleString();
  if (mode === "kTwd") {
    // v in 仟元 (NTD thousands). 1 億 = 1e5 仟元, 1 兆 = 1e9 仟元
    if (v >= 1e9) return `${(v / 1e9).toFixed(2)}兆`;
    if (v >= 1e5) return `${(v / 1e5).toFixed(0)}億`;
    if (v >= 1e2) return `${(v / 1e2).toFixed(1)}千萬`;
    return `${v.toLocaleString()}仟`;
  }
  return v.toFixed(2);
}

export default function MultiLineChart({
  data, series, height = 220, format = "raw", barSpacing,
  onCrosshairMove, externalCrosshairIdx,
}: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const firstSeriesRef = useRef<ISeriesApi<"Line"> | null>(null);
  const onMoveRef = useRef(onCrosshairMove);
  onMoveRef.current = onCrosshairMove;

  useEffect(() => {
    if (!containerRef.current || data.length === 0) return;

    const parseTime = (time: any): { y: number; m: number; d: number } | null => {
      if (time && typeof time === "object" && "year" in time) {
        return { y: time.year, m: time.month, d: time.day };
      }
      if (typeof time === "string") {
        const parts = time.split("-");
        if (parts.length < 3) return null;
        return { y: +parts[0], m: +parts[1], d: +parts[2] };
      }
      if (typeof time === "number") {
        const dt = new Date(time * 1000);
        return { y: dt.getUTCFullYear(), m: dt.getUTCMonth() + 1, d: dt.getUTCDate() };
      }
      return null;
    };

    const chart = createChart(containerRef.current, {
      width: containerRef.current.clientWidth,
      height,
      layout: {
        background: { color: "#12121a" },
        textColor: "#8a8a9a",
      },
      grid: {
        vertLines: { color: "#1a1a25" },
        horzLines: { color: "#1a1a25" },
      },
      localization: {
        timeFormatter: (time: any) => {
          const p = parseTime(time);
          return p ? `${p.y}/${p.m}/${p.d}` : String(time);
        },
      },
      timeScale: {
        borderColor: "#2a2a35",
        tickMarkFormatter: (time: any, tickMarkType: number) => {
          const p = parseTime(time);
          if (!p) return String(time);
          if (tickMarkType === 0) return `${p.y}`;
          if (tickMarkType === 1) return `${p.y}/${p.m}`;
          return `${p.m}/${p.d}`;
        },
        ...(barSpacing !== undefined ? { barSpacing, rightOffset: 4 } : {}),
      },
      rightPriceScale: { visible: false },
      leftPriceScale: {
        visible: true,
        borderColor: "#2a2a35",
      },
      crosshair: { mode: 1 },
    });
    chartRef.current = chart;

    let firstSeries: ISeriesApi<"Line"> | null = null;
    for (const s of series) {
      const line = chart.addSeries(LineSeries, {
        color: s.color,
        lineWidth: (s.lineWidth ?? 2) as 1 | 2 | 3 | 4,
        lineStyle: s.dashed ? 2 : 0,
        title: s.hideLastValue ? undefined : s.label,
        lastValueVisible: !s.hideLastValue,
        priceLineVisible: false,
        priceScaleId: "left",
        priceFormat: {
          type: "custom",
          formatter: (p: number) => fmt(p, format),
        },
      });
      line.setData(
        data
          .map((d) => {
            const v = d[s.key];
            return typeof v === "number"
              ? { time: d.date as string, value: v }
              : null;
          })
          .filter((p): p is { time: string; value: number } => p !== null) as any
      );
      if (firstSeries === null) firstSeries = line;
    }
    firstSeriesRef.current = firstSeries;

    chart.subscribeCrosshairMove((param) => {
      const cb = onMoveRef.current;
      if (!cb) return;
      if (param.logical === undefined || param.logical === null || param.logical < 0) {
        cb(null);
      } else {
        const idx = Math.round(param.logical as number);
        cb(idx >= 0 && idx < data.length ? idx : null);
      }
    });

    if (barSpacing === undefined) {
      chart.timeScale().fitContent();
    } else {
      chart.timeScale().scrollToRealTime();
    }

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
  }, [data, series, height, format, barSpacing]);

  // External crosshair sync: when externalCrosshairIdx changes, drive this
  // chart's crosshair to the same bar (or clear it). The chart that
  // originated the move keeps its native crosshair, others mirror it.
  useEffect(() => {
    const chart = chartRef.current;
    const fs = firstSeriesRef.current;
    if (!chart || !fs) return;
    if (externalCrosshairIdx === null || externalCrosshairIdx === undefined
        || externalCrosshairIdx < 0 || externalCrosshairIdx >= data.length) {
      chart.clearCrosshairPosition();
      return;
    }
    const row = data[externalCrosshairIdx];
    const firstKey = series[0].key;
    const v = row[firstKey];
    if (typeof v === "number") {
      chart.setCrosshairPosition(v, row.date as string, fs);
    }
  }, [externalCrosshairIdx, data, series]);

  return <div ref={containerRef} />;
}
