"""Business groups: the name, the type, and the company at the top.

Only these three things are authored. Membership is not — db.derive_groups
computes it from tw.company_affiliations, because who owns whom is a fact we
already hold and a hand-kept member list goes stale silently.

The anchor is what the derivation descends from, so it has to be the company
that actually holds the others. Getting it wrong yields an empty group rather
than a wrong one.

Seeded from 台股產業分類_Master_V3_7, whose own column was labelled
「代表個股（示範，需核對）」 — the anchors below are the first listed name from
each of those lists and have not been independently verified.

Sync with: python -m db.derive_groups
"""

# (group_code, group_name, group_type, anchor_company_name, note)
GROUPS = [
    ('GT01', '台塑集團', '正式集團', '台塑', '台塑、南亞、台化、台塑化（台塑四寶）'),
    ('GT02', '鴻海集團', '正式集團', '鴻海', '鴻海、鴻準、臻鼎-KY、廣宇'),
    ('GT03', '華新集團', '正式集團', '華新', '華新、華邦電、華新科、瀚宇博'),
    ('GT04', '台積電體系', '正式集團', '台積電', '台積電、精材、創意、世界先進'),
    ('GT05', '聯電集團', '正式集團', '聯電', '聯電、聯詠、智原、聯陽、欣興'),
    ('GT06', '廣達集團', '正式集團', '廣達', '廣達、廣明'),
    ('GT07', '緯創集團', '正式集團', '緯創', '緯創、緯穎、緯軟、啟碁'),
    ('GT08', '日月光集團', '正式集團', '日月光投控', '日月光投控、環電'),
    ('GT09', '聯發科生態系', '正式集團', '聯發科', '聯發科、達發、晶心科'),
    ('GT10', '中信集團', '正式集團', '中信金', '中信金'),
    ('GT11', '富邦集團', '正式集團', '富邦金', '富邦金、台灣大'),
    ('GT12', '國泰集團', '正式集團', '國泰金', '國泰金'),
    ('GT13', '元大集團', '正式集團', '元大金', '元大金'),
    ('GT14', '台達電集團', '正式集團', '台達電', '台達電、乾坤'),
    ('GT15', '光寶集團', '正式集團', '光寶科', '光寶科'),
    ('GT16', '和碩集團', '正式集團', '和碩', '和碩、景碩、鎧勝-KY'),
    ('GT17', '仁寶集團', '正式集團', '仁寶', '仁寶、金寶、康舒'),
    ('GT18', '明基友達集團', '正式集團', '佳世達', '佳世達、友達、明基材'),
    ('GT19', '遠東集團', '正式集團', '遠東新', '遠東新、遠傳、亞泥、遠百'),
    ('GT20', '中租集團', '正式集團', '中租-KY', '中租-KY、合迪'),
]
