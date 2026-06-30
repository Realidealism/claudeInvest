from data.csv_loader import load_bars_csv


def test_load_bars_csv(tmp_path):
    p = tmp_path / "bars.csv"
    p.write_text(
        "ts,open,high,low,close,volume\n"
        "2024-12-18T09:05:00,100,102,99,101,10\n"
        "2024/12/18 09:10,101,103,100,102,12\n",
        encoding="utf-8",
    )
    bars = load_bars_csv(str(p), symbol="TMF00", timeframe="5m")
    assert len(bars) == 2
    assert bars[0].open == 100 and bars[0].close == 101 and bars[0].volume == 10
    assert bars[0].symbol == "TMF00" and bars[0].timeframe == "5m"
    assert bars[1].high == 103
    assert bars[0].ts < bars[1].ts   # sorted ascending
