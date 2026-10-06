# 資料庫備份與還原

這份文件說明月備怎麼運作、怎麼確認它還活著，以及要還原時每一步該下什麼指令。
2026-10-06 重建：原本的備份腳本和這份文件都是沒有進 git 的檔案，在 2026-07-02 遺失，之後三個月沒有產出任何備份。

## 這份備份涵蓋什麼、不涵蓋什麼

- **保護從第一份新封存檔的日期開始**。重建腳本只能讓往後救得回來，補不回過去。
- **2026-08-01、09-01、10-01 沒有備份**。排程一直掛著，但它要執行的檔案已經不存在，所以這三個日期都沒有產出封存檔，也沒有任何告警。
- **2026-07-01 之後才建立的表，在第一份新封存檔之前沒有任何備份**。重建當時這樣的表有 35 張，包含人工裁定的產業分類資料。
- 目錄裡較舊的兩份（`invest_20260630.dump`、`invest_20260701.dump`）是舊腳本產出的，不包含上面那些表。

## 備份怎麼運作

- 資料庫在 Docker 容器 `invest-db`（postgres:16，主機連接埠 5433）裡，資料庫名稱是 `invest`。
- 排程 `Invest\BackupMonthly` 設定在每月 1 日 03:00 執行 `backup_monthly.bat`，它呼叫 `backup_monthly.py`。排程只在使用者登入時才會執行，錯過不補跑，所以「排程存在」不等於「那個月有備份」，要照下面「確認備份還活著」的方法檢查。
- 腳本在容器內跑 `pg_dump -Fc`，把自訂格式的封存檔寫到 `D:\invest_backup\invest_YYYYMMDD.dump`，保留最新的 6 份。
- 封存檔先寫成 `.partial`，用 `pg_restore -l` 確認讀得回來、裡面有 `tw.daily_prices` 的資料，才改成正式檔名。任何一步失敗，舊的備份都不會被動到。
- 腳本另外會比對大小：新的一份不到上一份的一半就判定失敗。這只是抓「明顯被截斷」的警訊，通過不代表內容完整。要確認一份封存檔真的能用，唯一的方法是實際還原一次（見下面「先在隔離的容器裡試一次」）。
- 失敗時腳本以非零結束（排程會記成失敗），並送出 `[備份]` 開頭的 Telegram 告警；紀錄在 `logs\backup_monthly.log`。

### 只備份結構、不備份資料的表

這些是盤中歷史或每個交易時段重建的暫存表：

`tw.intraday_opening_range`、`tw.intraday_orb_signals`、`tw.intraday_quotes`、`tw.open_positions_intraday`、`tw.orb_backtest_trades`、`tw.score_snapshot_intraday`、`tw.signal_snapshot_intraday`、`tw.stock_halts_today`

還原之後這 8 張表是空的。其餘每一張表的資料都在封存檔裡；新建立的表不必登記，預設就會被備份。

`tw.intraday_value_profile` 雖然也是盤中表，但資料有備份（舊腳本沒有）：它不到 1 MB、無法回補，而預估量曲線和開盤量能簡報的校準都需要它累積數週。

`data/signal_versions.db`（回測版本紀錄）不在這份備份裡，靠 git 保存。

## 確認備份還活著

```powershell
Get-ChildItem D:\invest_backup\invest_*.dump | Sort-Object Name | Select-Object Name, Length, LastWriteTime
Get-Content C:\Claude\Invest\logs\backup_monthly.log -Tail 6
Get-ScheduledTask -TaskPath '\Invest\' -TaskName 'BackupMonthly' | Get-ScheduledTaskInfo | Select-Object LastRunTime, LastTaskResult, NextRunTime
```

最新一份的日期應該是這個月的 1 日，`LastTaskResult` 應該是 0。

排程的設定是「使用者登入時才執行」，而且錯過不補跑：1 日 03:00 電腦沒開或沒登入，那個月就沒有備份。遇到這種情況，手動補跑：

```powershell
Start-ScheduledTask -TaskPath '\Invest\' -TaskName 'BackupMonthly'
```

## 看一份封存檔裡有什麼

```powershell
cmd /c "docker exec -i invest-db pg_restore -l < D:\invest_backup\invest_YYYYMMDD.dump"
```

要用 `cmd /c` 包起來：PowerShell 自己的 `<` 和管線會破壞二進位內容。

## 還原

### 先在隔離的容器裡試一次（不碰正式資料庫）

```powershell
docker run -d --name invest-restore-check -e POSTGRES_HOST_AUTH_METHOD=trust postgres:16
docker exec invest-restore-check pg_isready -U postgres
docker exec invest-restore-check createdb -U postgres invest
cmd /c "docker exec -i invest-restore-check pg_restore -U postgres -d invest --no-owner --exit-on-error < D:\invest_backup\invest_YYYYMMDD.dump"
docker exec invest-restore-check psql -U postgres -d invest -c "select count(*) from tw.daily_prices"
docker rm -f -v invest-restore-check
```

這個容器沒有對外連接埠，用完整個刪掉。

### 還原到正式資料庫

這一步會覆蓋現有資料，做之前先停掉所有會寫入資料庫的服務和排程。

```powershell
C:\Claude\Invest\tools\nssm.exe stop InvestIntradaySnapshot
C:\Claude\Invest\tools\nssm.exe stop InvestIntradaySweeper
C:\Claude\Invest\tools\nssm.exe stop InvestTelegramBot
C:\Claude\Invest\tools\nssm.exe stop InvestMicroPaper
cmd /c "docker exec -i invest-db pg_restore -U postgres -d invest --clean --if-exists --no-owner < D:\invest_backup\invest_YYYYMMDD.dump"
```

還原完成後：

1. 啟動服務（把上面四行的 `stop` 換成 `start`）。服務啟動時會執行 `init_db`，把備份之後才新增的 migration 補上。
2. 備份日之後的日線資料用 `C:\Claude\Invest\dist\daily_update.exe 起日 迄日` 回補。有些來源只保留最近幾天的資料，超過就補不回來，所以還原後要盡快回補。
3. 上面列的 8 張盤中表是空的，下一個交易時段會重新累積。
