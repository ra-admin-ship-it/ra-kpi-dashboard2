#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RA KPI Dashboard - アライアンス応募数 自動更新
================================================
毎週月曜日 0:00 に Claude Code スケジューラーから実行。

スプレッドシート構成（GID: 562995597）:
  行0  : ヘッダー（週次集計 等）
  行1  : 今週
  行2  : 先週
  行3  : 先々週
  行4  : （空行）
  行5  : ヘッダー（月次集計 等）
  行6  : 今月    ← F列を現在月のactualに反映
  行7  : 先月    ← F列を1ヶ月前のactualに反映
  行8  : 先々月  （使用しない）

【事前準備】
  スプレッドシートを「リンクを知っている全員が閲覧可」に設定してください。
  （共有 → リンクを取得 → 閲覧者 に変更）
"""

import sys
import os
import csv
import io
import re
import json
import subprocess
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta

# Windows ターミナルの文字コードエラーを回避
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ('utf-8', 'utf8'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if sys.stderr.encoding and sys.stderr.encoding.lower() not in ('utf-8', 'utf8'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# =====================================================================
# 設定
# =====================================================================

SPREADSHEET_ID    = "1oBg1uS1dFN73p2G24BRyYo80T1eL_ubHjyz0XsdjSus"
SHEET_GID         = "562995597"

COL_VALUE         = 5   # F列（0始まり）: アライアンス応募数
ROW_CURRENT_MONTH = 6   # 行インデックス（0始まり）: 今月行
ROW_PREV_MONTH    = 7   # 行インデックス（0始まり）: 先月行

HTML_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
JST       = timezone(timedelta(hours=9))

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
                "スプレッドシートを「リンクを知っている全員が閲覧可」に\n"
                "設定してください。（共有 → リンクを取得 → 閲覧者）"
            ) from e
        raise RuntimeError(f"HTTP error {e.code}: {e.reason}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"URL error: {e.reason}") from e


# =====================================================================
# 固定行から値を取得
# =====================================================================
def get_values_from_csv(csv_text: str) -> tuple[int, int]:
    """
    (今月の値, 先月の値) を返す。
    行が存在しない or 数値でない場合は None。
    """
    rows = list(csv.reader(io.StringIO(csv_text)))

    def parse_cell(row_idx: int) -> int | None:
        if row_idx >= len(rows):
            print(f"  ⚠️  行{row_idx}が存在しません（CSV全{len(rows)}行）")
            return None
        row = rows[row_idx]
        if COL_VALUE >= len(row):
            print(f"  ⚠️  行{row_idx}にF列（index {COL_VALUE}）がありません")
            return None
        raw = row[COL_VALUE].strip().replace(",", "").replace("，", "")
        if not raw or not raw.lstrip("-").isdigit():
            print(f"  ⚠️  行{row_idx} F列が数値でありません: '{raw}'")
            return None
        return int(raw)

    cur_val  = parse_cell(ROW_CURRENT_MONTH)
    prev_val = parse_cell(ROW_PREV_MONTH)
    return cur_val, prev_val


# =====================================================================
# 月キーを生成
# =====================================================================
def month_keys() -> tuple[str, str]:
    """(今月キー, 先月キー) 例: ("2026-03", "2026-02")"""
    now       = datetime.now(JST)
    cur_ym    = now.strftime("%Y-%m")
    first_day = now.replace(day=1)
    prev_dt   = first_day - timedelta(days=1)
    prev_ym   = prev_dt.strftime("%Y-%m")
    return cur_ym, prev_ym


# =====================================================================
# index.html 内の特定月の アライアンス応募数 actual を更新
# =====================================================================
def update_month_in_html(content: str, ym: str, new_val: int) -> tuple[str, bool]:
    """
    content 内の "YYYY-MM": { ... アライアンス応募数: { actual:N, ... } ... }
    を探して actual を new_val に置換する。
    該当月が存在しない場合は (content, False) を返す。
    """
    # 月キーの位置を特定
    month_key_pos = content.find(f'"{ym}"')
    if month_key_pos == -1:
        return content, False

    # 次の月キー（"20XX-XX" 形式）の位置を探して範囲を絞る
    next_month_pos = content.find('"20', month_key_pos + len(f'"{ym}"'))
    if next_month_pos == -1:
        segment = content[month_key_pos:]
        before  = content[:month_key_pos]
        after   = ""
    else:
        segment = content[month_key_pos:next_month_pos]
        before  = content[:month_key_pos]
        after   = content[next_month_pos:]

    # その月のセグメント内だけ置換
    pattern = r'(アライアンス応募数:\s*\{\s*actual\s*:)\s*\d+(\s*,\s*target\s*:\s*\d+\s*\})'
    new_segment, count = re.subn(pattern, rf'\g<1>{new_val}\g<2>', segment, count=1)

    if count == 0:
        return content, False

    return before + new_segment + after, True


# =====================================================================
# data.json 書き込み
# =====================================================================
DATA_JSON_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.json")

def write_alliance_data_json(cur_ym, cur_val, prev_ym, prev_val):
    """アライアンス応募数を data.json に書き込む。"""
    try:
        with open(DATA_JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        data = {}

    for ym, val in [(cur_ym, cur_val), (prev_ym, prev_val)]:
        if val is None:
            continue
        data.setdefault(ym, {})
        data[ym].setdefault("selection", {})
        data[ym]["selection"].setdefault("アライアンス応募数", {})
        data[ym]["selection"]["アライアンス応募数"]["actual"] = val

    with open(DATA_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  💾 data.json を更新しました（アライアンス応募数）")


# =====================================================================
# GitHub へ自動プッシュ
# =====================================================================
def git_push(now: datetime):
    """index.html を git add → commit → push する。失敗してもスクリプトは続行。"""
    repo_dir = os.path.dirname(os.path.abspath(__file__))
    date_str  = now.strftime("%Y-%m-%d")
    msg       = f"auto: update alliance {date_str}"
    try:
        subprocess.run(["git", "-C", repo_dir, "add", "index.html"],
                       check=True, capture_output=True)
        result = subprocess.run(["git", "-C", repo_dir, "diff", "--cached", "--quiet"],
                                capture_output=True)
        if result.returncode == 0:
            print("  ℹ️  index.html に変更なし。git push をスキップしました")
            return
        subprocess.run(["git", "-C", repo_dir, "commit", "-m", msg],
                       check=True, capture_output=True)
        for attempt in range(3):
            try:
                subprocess.run(["git", "-C", repo_dir, "pull", "--rebase", "origin", "main"],
                               check=True, capture_output=True)
                subprocess.run(["git", "-C", repo_dir, "push", "origin", "main"],
                               check=True, capture_output=True)
                print(f"  🚀 GitHub へ push 完了: {msg}")
                return
            except subprocess.CalledProcessError:
                if attempt < 2: continue
                raise
    except subprocess.CalledProcessError as e:
        print(f"  ⚠️  git push 失敗（ローカルは更新済み）: {e.stderr.decode(errors='replace').strip()}")


# =====================================================================
# メイン更新処理
# =====================================================================
def update_html(cur_val: int | None, prev_val: int | None):
    cur_ym, prev_ym = month_keys()

    with open(HTML_PATH, "r", encoding="utf-8") as f:
        content = f.read()

    updated = False

    # ── 今月を更新 ──────────────────────────────────────────────────
    if cur_val is not None:
        content, ok = update_month_in_html(content, cur_ym, cur_val)
        if ok:
            print(f"  ✅ {cur_ym} アライアンス応募数 actual → {cur_val}")
            updated = True
        else:
            print(f"  ⚠️  {cur_ym} の 'アライアンス応募数' が index.html に見つかりません")

    # ── 先月を更新 ──────────────────────────────────────────────────
    if prev_val is not None:
        content, ok = update_month_in_html(content, prev_ym, prev_val)
        if ok:
            print(f"  ✅ {prev_ym} アライアンス応募数 actual → {prev_val}")
            updated = True
        else:
            print(f"  ℹ️  {prev_ym} は index.html に存在しないためスキップしました")
            print(f"       （月が変わって {prev_ym} のデータが HTML に追加されたら自動で反映されます）")

    if not updated:
        print("  ⚠️  index.html を更新できませんでした。HTML の構造を確認してください。")
        return

    with open(HTML_PATH, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"  💾 index.html を保存しました")

    # data.json にも書き込む
    write_alliance_data_json(cur_ym, cur_val, prev_ym, prev_val)
    git_push(datetime.now(JST))


# =====================================================================
# メイン
# =====================================================================
def main():
    now = datetime.now(JST)
    print(f"[{now.strftime('%Y-%m-%d %H:%M:%S JST')}] アライアンス応募数 更新開始")

    cur_ym, prev_ym = month_keys()
    print(f"  対象月: 今月={cur_ym}  先月={prev_ym}")

    print("  スプレッドシート取得中...")
    csv_text = fetch_csv()
    print(f"  取得完了（全{csv_text.count(chr(10))}行）")

    cur_val, prev_val = get_values_from_csv(csv_text)
    print(f"  F列 row{ROW_CURRENT_MONTH+1}（今月）: {cur_val}")
    print(f"  F列 row{ROW_PREV_MONTH+1}（先月）: {prev_val}")

    update_html(cur_val, prev_val)
    print(f"[完了] {datetime.now(JST).strftime('%Y-%m-%d %H:%M:%S JST')}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
