#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RA KPI Dashboard - パイプライン① 新規開拓 自動更新
====================================================
毎日 0:00 に Claude Code スケジューラーから実行。

スプレッドシート: 1rze7tkzjaXqakGTsMix-F5mnKbQThB00OnkPvdtsLww (GID: 0)

カウント対象列:
  B列 (index 1) : 流入日時【アポ獲得日】 → アポイント数
  H列 (index 7) : 担当者                  → メンバー特定 (森/浅沼/安木/山本)
  U列 (index 20): 初回商談日              → 商談数
  X列 (index 23): 契約締結日              → 契約締結数
  Y列 (index 24): 現在のステータス        → 「既存対応」は全項目から除外

週の定義 (index.html の weekDateRange() に合わせる):
  第1週: 1日〜7日
  第2週: 8日〜14日
  第3週: 15日〜21日
  第4週: 22日〜月末

【事前準備】
  スプレッドシートを「リンクを知っている全員が閲覧可」に設定してください。
"""

import sys
import os
import csv
import io
import re
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
import calendar

# Windows ターミナルの文字コードエラーを回避
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ('utf-8', 'utf8'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if sys.stderr.encoding and sys.stderr.encoding.lower() not in ('utf-8', 'utf8'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# =====================================================================
# 設定
# =====================================================================
SPREADSHEET_ID = "1rze7tkzjaXqakGTsMix-F5mnKbQThB00OnkPvdtsLww"
SHEET_GID      = "0"

COL_APO        = 1   # B列: 流入日時（アポ獲得日）
COL_MEMBER     = 7   # H列: 担当者
COL_SHADAN     = 20  # U列: 初回商談日
COL_KEIYAKU    = 23  # X列: 契約締結日
COL_STATUS     = 24  # Y列: 現在のステータス

MEMBERS        = ["森", "浅沼", "安木", "山本"]
EXCLUDE_STATUS = "既存対応"

HTML_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
JST       = timezone(timedelta(hours=9))

# 週の日付範囲定義
WEEK_RANGES = {
    1: (1, 7),
    2: (8, 14),
    3: (15, 21),
    # 第4週は22日〜月末（動的に計算）
}


# =====================================================================
# CSV 取得
# =====================================================================
def fetch_csv() -> str:
    url = (
        f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}"
        f"/export?format=csv&gid={SHEET_GID}"
    )
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (compatible; RA-KPI-Updater/1.0)"
    })
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        if e.code == 401:
            raise RuntimeError(
                "スプレッドシートへのアクセスが拒否されました（401）。\n"
                "「リンクを知っている全員が閲覧可」に設定してください。"
            ) from e
        raise RuntimeError(f"HTTP error {e.code}: {e.reason}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"URL error: {e.reason}") from e


# =====================================================================
# 日付文字列パース
# =====================================================================
def parse_date(raw: str):
    """
    各種フォーマットの日付文字列を datetime に変換。
    失敗時は None を返す。
    対応フォーマット例:
      2026/3/24 17:58
      2026/3/24
      2026-03-24
      2026-03-24 17:58:00
    """
    raw = raw.strip()
    if not raw:
        return None
    for fmt in (
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y/%m/%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


# =====================================================================
# 週番号と日付範囲を取得
# =====================================================================
def get_week_ranges(year: int, month: int) -> dict:
    """
    {1: (start_day, end_day), 2: ..., 3: ..., 4: ...} を返す。
    第4週の終わりは月末日。
    """
    last_day = calendar.monthrange(year, month)[1]
    return {
        1: (1, 7),
        2: (8, 14),
        3: (15, 21),
        4: (22, last_day),
    }


def get_week_num(day: int, week_ranges: dict) -> int | None:
    """日付 (day) が属する週番号を返す。"""
    for wn, (start, end) in week_ranges.items():
        if start <= day <= end:
            return wn
    return None


# =====================================================================
# CSVを集計
# =====================================================================
def aggregate(csv_text: str, year: int, month: int) -> dict:
    """
    {
      week_num: {
        "アポイント数":  {"森": N, "浅沼": N, "安木": N, "山本": N},
        "商談数":        {"森": N, ...},
        "契約締結数":    {"森": N, ...},
      },
      ...
    }
    """
    week_ranges = get_week_ranges(year, month)

    # 結果を 0 初期化
    result = {}
    for wn in range(1, 5):
        result[wn] = {
            "アポイント数":  {m: 0 for m in MEMBERS},
            "商談数":        {m: 0 for m in MEMBERS},
            "契約締結数":    {m: 0 for m in MEMBERS},
        }

    rows = list(csv.reader(io.StringIO(csv_text)))
    if not rows:
        return result

    # ヘッダー行スキップ（1行目がヘッダーと仮定）
    data_rows = rows[1:]

    skipped = 0
    counted = 0

    for row_idx, row in enumerate(data_rows, start=2):
        # 列数が足りない行はスキップ
        if len(row) <= max(COL_APO, COL_MEMBER, COL_SHADAN, COL_KEIYAKU, COL_STATUS):
            continue

        # 担当者
        member = row[COL_MEMBER].strip()
        if member not in MEMBERS:
            continue

        # ステータスチェック（既存対応は除外）
        status = row[COL_STATUS].strip()
        if status == EXCLUDE_STATUS:
            skipped += 1
            continue

        counted += 1

        # ── アポイント数（B列: 流入日時） ──────────────────────────
        apo_raw = row[COL_APO].strip() if len(row) > COL_APO else ""
        if apo_raw:
            dt = parse_date(apo_raw)
            if dt and dt.year == year and dt.month == month:
                wn = get_week_num(dt.day, week_ranges)
                if wn:
                    result[wn]["アポイント数"][member] += 1

        # ── 商談数（U列: 初回商談日） ──────────────────────────────
        shadan_raw = row[COL_SHADAN].strip() if len(row) > COL_SHADAN else ""
        if shadan_raw:
            dt = parse_date(shadan_raw)
            if dt and dt.year == year and dt.month == month:
                wn = get_week_num(dt.day, week_ranges)
                if wn:
                    result[wn]["商談数"][member] += 1

        # ── 契約締結数（X列: 契約締結日） ─────────────────────────
        keiyaku_raw = row[COL_KEIYAKU].strip() if len(row) > COL_KEIYAKU else ""
        if keiyaku_raw:
            dt = parse_date(keiyaku_raw)
            if dt and dt.year == year and dt.month == month:
                wn = get_week_num(dt.day, week_ranges)
                if wn:
                    result[wn]["契約締結数"][member] += 1

    print(f"  処理行数: {counted}行（既存対応除外: {skipped}行）")
    return result


# =====================================================================
# HTML 更新: 月・週・フィールド を指定して値を置換
# =====================================================================
def get_month_segment(content: str, ym: str):
    """
    (month_before, month_seg, month_after) を返す。
    """
    month_pos = content.find(f'"{ym}"')
    if month_pos == -1:
        return None

    next_m = re.search(r'"20\d\d-\d\d"', content[month_pos + 8:])
    if next_m:
        month_end = month_pos + 8 + next_m.start()
    else:
        month_end = len(content)

    return content[:month_pos], content[month_pos:month_end], content[month_end:]


def get_week_segment(month_seg: str, week_num: int):
    """
    月セグメント内から特定週ブロックを brace counting で抽出。
    (before, week_seg, after) を返す。
    """
    week_key_pat = rf'\b{week_num}\s*:\s*\{{'
    m = re.search(week_key_pat, month_seg)
    if not m:
        return None

    brace_start = m.end() - 1  # '{' の位置
    depth = 0
    i = brace_start
    while i < len(month_seg):
        if month_seg[i] == '{':
            depth += 1
        elif month_seg[i] == '}':
            depth -= 1
            if depth == 0:
                week_end = i + 1
                break
        i += 1
    else:
        return None

    return month_seg[:m.start()], month_seg[m.start():week_end], month_seg[week_end:]


def replace_p1_field(week_seg: str, field: str, vals: dict) -> tuple[str, bool]:
    """
    week_seg 内の p1 フィールド（アポイント数/商談数/契約締結数）を更新。
    """
    森    = vals.get("森", 0)
    浅沼  = vals.get("浅沼", 0)
    安木  = vals.get("安木", 0)
    山本  = vals.get("山本", 0)

    pattern = (
        rf'({re.escape(field)}\s*:\s*\{{\s*)'
        r'森\s*:\s*\d+'
        r'(\s*,\s*)浅沼\s*:\s*\d+'
        r'(\s*,\s*)安木\s*:\s*\d+'
        r'(\s*,\s*)山本\s*:\s*\d+'
        r'(\s*\})'
    )
    replacement = (
        rf'\g<1>'
        rf'森:{森}'
        rf'\g<2>浅沼:{浅沼}'
        rf'\g<3>安木:{安木}'
        rf'\g<4>山本:{山本}'
        rf'\g<5>'
    )
    new_seg, count = re.subn(pattern, replacement, week_seg, count=1)
    return new_seg, count > 0


def update_html(content: str, ym: str, aggregated: dict) -> tuple[str, int]:
    """
    index.html を更新して (新しいcontent, 更新件数) を返す。
    """
    segs = get_month_segment(content, ym)
    if segs is None:
        print(f"  ⚠️  '{ym}' が index.html に見つかりません")
        return content, 0

    month_before, month_seg, month_after = segs
    total_updates = 0

    for week_num in range(1, 5):
        week_segs = get_week_segment(month_seg, week_num)
        if week_segs is None:
            print(f"  ⚠️  第{week_num}週ブロックが見つかりません")
            continue

        w_before, week_seg, w_after = week_segs
        week_data = aggregated.get(week_num, {})
        week_changed = False

        for field in ("アポイント数", "商談数", "契約締結数"):
            vals = week_data.get(field, {m: 0 for m in MEMBERS})
            new_week_seg, ok = replace_p1_field(week_seg, field, vals)
            if ok:
                week_seg = new_week_seg
                week_changed = True
                total_updates += 1
                print(
                    f"  ✅ 第{week_num}週 {field}: "
                    f"森={vals['森']} 浅沼={vals['浅沼']} 安木={vals['安木']} 山本={vals['山本']}"
                )
            else:
                print(f"  ⚠️  第{week_num}週 {field} のパターンが見つかりませんでした")

        if week_changed:
            month_seg = w_before + week_seg + w_after

    return month_before + month_seg + month_after, total_updates


# =====================================================================
# メイン
# =====================================================================
def main():
    now = datetime.now(JST)
    print(f"[{now.strftime('%Y-%m-%d %H:%M:%S JST')}] パイプライン① 自動更新開始")

    year  = now.year
    month = now.month
    ym    = now.strftime("%Y-%m")
    print(f"  対象月: {ym}  ({year}年{month}月)")

    # CSV取得
    print("  スプレッドシート取得中...")
    csv_text = fetch_csv()
    line_count = csv_text.count('\n')
    print(f"  取得完了 ({line_count}行)")

    # 集計
    print("  集計中...")
    aggregated = aggregate(csv_text, year, month)

    # 集計結果サマリー表示
    for wn in range(1, 5):
        for field in ("アポイント数", "商談数", "契約締結数"):
            vals = aggregated[wn][field]
            total = sum(vals.values())
            if total > 0:
                print(f"  📊 第{wn}週 {field}: {vals}")

    # HTML更新
    print("  index.html 更新中...")
    with open(HTML_PATH, "r", encoding="utf-8") as f:
        content = f.read()

    new_content, updates = update_html(content, ym, aggregated)

    if updates == 0:
        print("  ⚠️  更新できた項目がありませんでした")
        return

    with open(HTML_PATH, "w", encoding="utf-8") as f:
        f.write(new_content)

    print(f"  💾 index.html を保存しました（{updates}項目更新）")
    print(f"[完了] {datetime.now(JST).strftime('%Y-%m-%d %H:%M:%S JST')}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
