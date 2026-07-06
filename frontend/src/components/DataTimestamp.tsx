// Small uniform "data as of" line. Renders nothing when the value is absent
// so pages can pass fields straight from a not-yet-loaded fetch.
export default function DataTimestamp({
  value,
  note,
}: {
  value?: string | null;
  note?: string;
}) {
  if (!value) return null;
  return (
    <p className="text-[11px] text-text-secondary/80">
      資料時間：<span className="font-mono">{value}</span>
      {note && <span className="ml-2">｜{note}</span>}
    </p>
  );
}
