# 06 常駐服務安全

適用範圍：寫或改任何常駐、服務化的長迴圈（NSSM 服務、排程 daemon）。本檔只收「服務活著但不工作」這類 liveness 規則；其他工程規範不要往這裡塞。

## 單輪看門狗

**觸發**：寫或改任何常駐服務化的長迴圈，且單輪工作可能無限期等待：multiprocessing Pool、外部 I/O（DB、網路、SDK）或鎖。

**規則**：
- 給單輪工作一個時間上限看門狗；逾時時要結束整個程序，並讓外部看得到這次失敗。目前的 Python 程式要在看門狗裡用 `os._exit(1)`（`faulthandler` 的 `exit=True` 就是這樣做）：`sys.exit` 在 timer thread 只會結束那條 thread，程序會繼續卡住。依執行方式：
  - 有服務管理器會自動重啟的（目前是 NSSM）：非零結束代碼讓服務管理器判定失敗並重啟。
  - 由排程器啟動、沒有自動重啟的工作：除了以非零 exit code 結束，還要確保既有告警機制能收到這次失敗；不能假設排程器記錄非零 exit code 就等於已告警，因為記錄與通知是兩件事。排程器不會立刻重跑，要到下次觸發才會再執行；等不到下次的工作要另外安排重跑。
- 看門狗結束程序前，先自動把所有 thread 的 stack 寫進 log（例如 Python 的 `faulthandler.dump_traceback(all_threads=True)`），因為逾時結束後現場就消失了。留現場和發告警都不能擋住結束：dump／告警各自要有錯誤處理與必要的 timeout，最終的程序終止必須位於無論前述動作成功或失敗都會到達的路徑，不得因 dump 或告警例外而跳過。錯誤處理與 timeout 擋不住卡在鎖上或 GIL 被佔住的情形，所以終止要由一個不依賴 Python 層 dump／告警返回的硬期限保證，而且要在單輪工作開始、看門狗開始計時的同時就布好，不能等逾時後才在 Python 層的回呼裡補布。Python 的做法是 `faulthandler.dump_traceback_later(t, exit=True)`：它由 C 層的 watchdog 執行緒執行，Python 死結時仍能 dump，dump 完直接 `_exit(1)`。一個程序只有一個這種計時器，再設一次會取代前一個，所以不能巢狀使用。Python 層的計時器（例如只做 `os._exit` 的 `threading.Timer`）要先拿到 GIL 才會執行，只救得了卡住時會釋放 GIL 的情形（等鎖、等 I/O）。`exit=True` 會跳過程序內的告警，所以由排程器啟動的工作，告警要交給外層處理（例如 .bat 依 exit code 呼叫 `cron_alert`）。實例：根目錄 `intraday_snapshot.py` 的 `_deadline`（2026-10-08）。自動紀錄只涵蓋本程序；Pool worker 等子程序的 stack 要另外取得（例如 py-spy）。
- 門檻取自該迴圈實測的單輪時長分布（至少看 p50／p99／max），不憑感覺定。
- 服務管理器的自動重啟（NSSM `AppExit Default=Restart`）只救得了**會退出**的程序，救不了**活著但不動**的程序；服務狀態 Running 不代表有在工作。
- 看門狗還沒觸發、人在場時，可以先用 `py-spy dump`（服務程序需 Admin）取得現場 stack，作為自動紀錄的補充。

**驗收**：看門狗要實際觸發過一次，確認程序真的被結束、有留下 stack 紀錄、服務管理器真的重啟（或排程器記到非零結束代碼，而且告警機制確實收到這次失敗），不能只看程式碼。這比 [02_judgment.md](02_judgment.md) Rubric 2 對排程／服務的要求更嚴：不能用查到下次排程時間代替實際觸發。觸發方式依序：
1. 先找非 production 的驗證路徑，能在那裡驗就不碰 production。是不是 production 看實際會寫到哪裡（連的 DB、寫出的檔案與共用產出、用的憑證），不看是不是另一個程序或另一個實例：本 repo 的 intraday_snapshot 一啟動就跑 `init_db`，會把待套用的 migration 套到正式 DB，所以另開一個實例仍然是 production。只連隔離資料庫、不寫共用產出、不使用會作用在正式外部資源的憑證（例如正式 bot token、券商 API key）、不對外發送的環境，才算非 production（見 Rubric 11）。驗告警要特別注意：既有告警（`telegram_bot.cron_alert`，以及直接呼叫 `notify.send_sync` 的程式）都用同一個正式 bot token，本 repo 沒有測試用 token，改送另一個 chat_id 仍然是用正式 token 對外發送，屬 Rubric 3 必問。用測試替身只能證明程式有呼叫告警；「告警機制確實收到」這項證據仍然缺著，要照第 2 步取得同意後實發，或照第 3 步記為未完成。非 production 環境驗不到的項目（例如 NSSM 的實際重啟），照第 2 步處理，或記為未完成。
2. 找不到時，在 production 觸發屬 [02_judgment.md](02_judgment.md) Rubric 3 必問：先取得使用者同意，同意的範圍要包含具體做法（例如暫時調低門檻就是修改 production 設定，要一併說明，驗完後還原）。
3. 使用者不同意時，驗收狀態記為未完成，不是通過，並記下缺的是哪一項證據。

**來源**：2026-09-09 盤中 snapshot daemon 停擺 2 小時：`pool.imap_unordered` 遺失一個結果 chunk，主程序在 `pool.py:861 _cond.wait()` 永久等待，8 個 worker 全部空閒，NSSM 全程顯示 Running。該次實作細節見 memory `project_intraday_snapshot_watchdog`（門檻 1200s 取自 p50 264s／p99 407s／max 913s）。原為 02_judgment 踩坑紀錄，2026-10-06 升格為本檔。
