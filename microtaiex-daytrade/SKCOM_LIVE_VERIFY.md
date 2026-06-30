# 群益 SKCOM live 連線 — 現場驗證手冊

> 本機（開發機）**無法**執行這段：沒有 SKCOM.dll / COM apartment。
> 以下全部在「已安裝群益 API 元件的 64-bit Windows 機器」上跑。
> 程式碼已寫好在 `src/broker/capital_skcom.py`，comtypes 為 lazy import。

## 0. 前置（一次性）

1. 官方下載包（會員登入區）選「下載元件 & 範例」，解壓到 `C:\CapitalAPI_x.xx_...\`。
2. 進 `元件\x64\`，以**系統管理員**執行 `install.bat`（註冊 64-bit SKCOM.dll）。
3. 安裝 comtypes：
   ```
   python -m pip install comtypes pywin32
   ```
4. 確認 `config.yaml` 的 `broker.skcom_dll_path` 指到 x64 SKCOM.dll（或元件已註冊則填 `'SKCOM.dll'`）。

## 1. 設定環境變數（PowerShell，當次 session）

```powershell
$env:BROKER_ID   = "你的身分證字號"
$env:BROKER_PWD  = "你的登入密碼"
$env:FUT_ACCOUNT = "券商代碼-期貨帳號"   # 例 F020000-1234567
$env:CERT_ID     = $env:BROKER_ID
```

## 2. 分階段驗證（對應 plan P1→P5）

各階段獨立、未過不進下一階段。把片段存成檔再跑，或用 `python -c`。

### A. 登入（P1）
```powershell
python -c "import sys; sys.path.insert(0,'src'); import os; from broker.factory import make_broker; b=make_broker({'name':'capital_skcom','user_id':os.environ['BROKER_ID'],'password':os.environ['BROKER_PWD'],'full_account':os.environ['FUT_ACCOUNT'],'cert_id':os.environ['CERT_ID'],'skcom_dll_path':r'C:\SKCOM\x64\SKCOM.dll'}); b.connect(); import time; time.sleep(5); print('connected'); b.disconnect()"
```
通過標準：印出 `Capital login OK` 與 `connected`，無例外。

### B. 收微台 tick（P1/P2）
建 `verify_tick.py`：
```python
import sys, os, time
sys.path.insert(0, "src")
from broker.factory import make_broker

b = make_broker({"name": "capital_skcom",
                 "user_id": os.environ["BROKER_ID"], "password": os.environ["BROKER_PWD"],
                 "full_account": os.environ["FUT_ACCOUNT"], "cert_id": os.environ["CERT_ID"],
                 "skcom_dll_path": r"C:\SKCOM\x64\SKCOM.dll"})
b.set_on_tick(lambda t: print(t.ts, t.symbol, t.price, t.volume))
b.set_on_connection(lambda s: print("CONN", s))
b.connect()
time.sleep(3)
b.subscribe("TMF00")     # TODO(verify): 微台近月 SKCOM 代碼（小台為 MTX00）
time.sleep(30)
b.disconnect()
```
```powershell
python verify_tick.py
```
通過標準：盤中跑時印出連續 tick，價格已正確縮放（非整數放大值）。

### C. 歷史 K（P2 warmup）
```powershell
python -c "import sys,os; sys.path.insert(0,'src'); from broker.factory import make_broker; b=make_broker({'name':'capital_skcom','user_id':os.environ['BROKER_ID'],'password':os.environ['BROKER_PWD'],'full_account':os.environ['FUT_ACCOUNT'],'cert_id':os.environ['CERT_ID'],'skcom_dll_path':r'C:\SKCOM\x64\SKCOM.dll'}); b.connect(); bars=b.get_kbars('TMF00','20240601','20240605'); print('bars:',len(bars)); print(bars[:3]); b.disconnect()"
```
通過標準：回傳非空 Bar 串列、時間/OHLCV 合理。

### D. 模擬下單 + 回報（P5，務必先用模擬/UAT 帳號）
```powershell
python -c "import sys,os; sys.path.insert(0,'src'); from broker.factory import make_broker; from broker.types import OrderRequest, Side, OpenClose, TimeInForce; b=make_broker({'name':'capital_skcom','user_id':os.environ['BROKER_ID'],'password':os.environ['BROKER_PWD'],'full_account':os.environ['FUT_ACCOUNT'],'cert_id':os.environ['CERT_ID'],'skcom_dll_path':r'C:\SKCOM\x64\SKCOM.dll'}); b.set_on_trade(lambda t: print('FILL',t)); b.connect(); import time; time.sleep(3); r=b.place_order(OrderRequest('TMF00',Side.BUY,1,None,tif=TimeInForce.IOC,open_close=OpenClose.AUTO)); print('order',r.accepted,r.msg); time.sleep(5); b.disconnect()"
```
通過標準：`order True`，且 `OnNewData` 解析出 `FILL`。**若 FILL 欄位錯位 → 見下方 TODO 修正欄位索引。**

## 3. 已依官方範例(2.13.58)鎖定 vs 仍需現場核對

**已從官方範例/文件確認（已寫死）：**
- 環境：`SetAuthority(0正式/2測試)` 登入前呼叫；`config.test_env: true` → 測試環境
- 登入成功碼 0；`OnConnection(nKind=3003, nCode=0)` = 報價就緒
- Tick：`OnNotifyTicksLONG(...)` 簽章；價格 `/100.0`
- 下單：`SendFutureOrderCLR(user, bAsync, FUTUREORDER)` 回傳 `(message, nCode)`；FUTUREORDER 欄位編碼
- 回報：**成交是 `Type=='D'`**（`values[2]`），`P` 是改價（先前 stub 寫錯已修正）
- 未平倉：`GetOpenInterestGW(user, account, 1)` → `OnOpenInterest` 事件

**仍需現場用「第一筆真實/模擬成交」核對（程式標 `TODO(verify)`，且 handler 已 `log.debug` 印出 enumerate 後的逐欄位）：**

| 位置 | 待確認 | 影響 |
|------|--------|------|
| `OnNewData` 索引 | 期貨回報 BuySell 群組是否佔 5 欄 → `_RPT_PRICE=15`/`_RPT_QTY=24` 是否正確 | 成交價/量解析 |
| `OnOpenInterest` 索引 | 未平倉欄位（商品/方向/口數/均價在第幾欄） | `list_positions` 目前回空陣列 |
| `OnNotifyKLineData` | K 行字串格式 | `get_kbars` 解析 |
| 微台代碼 | 近月代碼（暫定 `TMF00`，小台為 `MTX00`） | 訂閱與下單 |

**核對方法**：跑階段 D 模擬單成交後，在 LOG 找 `OnNewData split:` 那行（會印 `[(0,'...'),(1,'TF'),(2,'D'),...]`），對照確認 price/qty 的索引；若不是 15/24，改 `capital_skcom.py` 頂部的 `_RPT_PRICE/_RPT_QTY` 常數即可。`OnOpenInterest split:` 同理。把那兩行 LOG 貼回來，我直接幫你鎖定索引並補完 `list_positions`。
