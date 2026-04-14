# Claude Code Rules

## Language
- 所有回應使用繁體中文
- Code comments must be written in English
- Commit messages must be written in English

## Task Continuity
- 僅在預期任務較大、可能跨對話時，才將計畫和關鍵進度記錄到記憶系統（memory/）
- 任務完成後應清理相關的進度記錄，避免記憶膨脹

## Data Sources
- 若找不到明確的 API 端點或資料來源，應先詢問使用者是否有已知的來源，再繼續實作
- 若已自行嘗試搜尋三次仍未找到合適的 API 端點，應停止搜尋並詢問使用者是否有明確來源

## Suggestion Batching
- 當使用者要求提供建議或改良方案時，若建議項目超過三個，一次只先提出前三個，剩餘項目等當前批次處理完後再接續提出
- 例外：若建議項目之間有互相影響或依賴關係（例如 A 的實作會影響 B 的設計），可一次提出超過三個，以便整體評估

## Commands
- 提示使用者執行的指令必須是完整、可直接複製貼上執行的成品，不可省略參數或用 placeholder

