#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RA KPI Dashboard - 応募数管理データ 自動更新
================================================
毎週月曜日 0:00 に Claude Code スケジューラーから実行。
3 つのスプレッドシートからデータを取得し
applications_data.json に保存する（最大 104 スナップショット = 約 2 年分）。

スプレッドシート構成
  gid=1214557495 : 全DBの応募数推移
  gid=562995597  : 総応募数（自社・アライアンス切り分け）
  gid=2034051626 : 応募数（企業別）

【事前準備】
  各スプレッドシートを「リンクを知っている全員が閲覧可」に設定してください。
  （共有 → リンクを取得 → 閲覧者 に変更）
"""

import sys
import os
import csv
import io
import json
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta

# Windows ターミナル文字コード対策
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr.encoding and sys.stderr.encoding.lower() not in ("utf-8", "utf8"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# =====================================================================
# 設定
# =====================================================================
SPREADSHEET_ID = "1oBg1uS1dFN73p2G24BRyYo80T1eL_ubHjyz0XsdjSus"
GID_DB         = "1214557495"   # 全DB応募数推移
GID_SPLIT      = "562995597"    # 自社・アライアンス
GID_COMPANY    = "2034051626"   # 企業別

SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PATH  = os.path.join(SCRIPT_DIR, "applications_data.json")
MAX_SNAPS    = 104   # 保持する最大スナップショット数

JST = timezone(timedelta(hours=9))

# =====================================================================
# CSV 取得
# =====================================================================
def fetch_csv(gid: str) -> list:
    url = (
        f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}"
        f"/export?format=csv&gid={gid}"
    )
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (compatible; RA-KPI-Updater/1.0)"
    })
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            text = resp.read().decode("utf-8")
        return list(csv.reader(io.StringIO(text)))
    except urllib.error.HTTPError as e:
        if e.code == 401:
            raise RuntimeError(
                "スプレッドシートへのアクセスが拒否されました（401）。\n"
                "スプレッドシートを「リンクを知っている全員が閲覧可」に設定してください。"
            ) from e
        raise RuntimeError(f"HTTP error {e.code}: {e.reason}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"URL error: {e.reason}") from e


# =====================================================================
# ユーティリティ
# =====================================================================
def num(s: str):
    """文字列 → float。変換できない場合は None。"""
    s = s.strip().replace(",", "").replace("，", "").replace(" ", "")
    if not s or s == "-":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def intn(s: str):
    v = num(s)
    return int(v) if v is not None else 0


def cell(row: list, idx: int, default=""):
    return row[idx].strip() if len(row) > idx else default


# =====================================================================
# シート①: 全DB応募数推移
# =====================================================================
def parse_db(rows: list) -> dict:
    """
    列インデックス（0始まり）:
      D(3)=ラベル, E(4)=合計, F(5)=CIRCUS, G(6)=CrowdAgent, H(7)=JoBins
      I(8)=日次平均合計, J(9)=CIRCUS平均, K(10)=CA平均, L(11)=JoBins平均
      M(12)=増減合計, N(13)=CIRCUS増減, O(14)=CA増減, P(15)=JoBins増減
    行インデックス:
      1=今週, 2=先週, 3=先々週
      6=今月, 7=先月, 8=先々月
    """
    def parse_row(r):
        if r >= len(rows):
            return None
        row = rows[r]
        return {
            "total":        intn(cell(row, 4)),
            "circus":       intn(cell(row, 5)),
            "crowdAgent":   intn(cell(row, 6)),
            "jobins":       intn(cell(row, 7)),
            "growthTotal":  cell(row, 12),
            "growthCircus": cell(row, 13),
            "growthCA":     cell(row, 14),
            "growthJobins": cell(row, 15),
        }

    return {
        "weekly": {
            "thisWeek":    parse_row(1),
            "lastWeek":    parse_row(2),
            "twoWeeksAgo": parse_row(3),
        },
        "monthly": {
            "thisMonth":    parse_row(6),
            "lastMonth":    parse_row(7),
            "twoMonthsAgo": parse_row(8),
        },
    }


# =====================================================================
# シート②: 自社・アライアンス
# =====================================================================
def parse_split(rows: list) -> dict:
    """
    列インデックス:
      D(3)=ラベル, E(4)=合計, F(5)=アライアンス, G(6)=自社
      H(7)=日次平均合計, I(8)=アライアンス平均, J(9)=自社平均
      K(10)=増減合計, L(11)=アライアンス増減, M(12)=自社増減
    行インデックス: 同上
    """
    def parse_row(r):
        if r >= len(rows):
            return None
        row = rows[r]
        return {
            "total":         intn(cell(row, 4)),
            "alliance":      intn(cell(row, 5)),
            "own":           intn(cell(row, 6)),
            "growthTotal":   cell(row, 10),
            "growthAlliance": cell(row, 11),
            "growthOwn":     cell(row, 12),
        }

    return {
        "weekly": {
            "thisWeek":    parse_row(1),
            "lastWeek":    parse_row(2),
            "twoWeeksAgo": parse_row(3),
        },
        "monthly": {
            "thisMonth":    parse_row(6),
            "lastMonth":    parse_row(7),
            "twoMonthsAgo": parse_row(8),
        },
    }


# =====================================================================
# シート③: 企業別
# =====================================================================
def parse_companies(rows: list) -> list:
    """
    列インデックス:
      D(3)=企業名
      E(4)=週次今週, F(5)=週次先週, G(6)=週次先々週
      H(7)=週次増減今週, I(8)=週次増減先週
      J(9)=月次今月, K(10)=月次先月, L(11)=月次先々月
      M(12)=月次増減今月, N(13)=月次増減先月
    row0/row1 はヘッダー行 → row2 から企業データ
    """
    companies = []
    for row in rows[2:]:
        name = cell(row, 3)
        if not name:
            continue
        companies.append({
            "name":        name,
            "weeklyThis":  intn(cell(row, 4)),
            "weeklyLast":  intn(cell(row, 5)),
            "weeklyPrev":  intn(cell(row, 6)),
            "monthlyThis": intn(cell(row, 9)),
            "monthlyLast": intn(cell(row, 10)),
            "monthlyPrev": intn(cell(row, 11)),
        })
    return companies


# =====================================================================
# メイン
# =====================================================================
def main():
    now = datetime.now(JST)
    print(f"[{now.strftime('%Y-%m-%d %H:%M:%S JST')}] 応募数管理データ 更新開始")

    # --- 取得 ---
    print("  [1/3] 全DB応募数推移 取得中 (gid=%s)..." % GID_DB)
    db_rows = fetch_csv(GID_DB)
    print(f"       → {len(db_rows)} 行取得")

    print("  [2/3] 自社・アライアンス 取得中 (gid=%s)..." % GID_SPLIT)
    split_rows = fetch_csv(GID_SPLIT)
    print(f"       → {len(split_rows)} 行取得")

    print("  [3/3] 企業別 取得中 (gid=%s)..." % GID_COMPANY)
    company_rows = fetch_csv(GID_COMPANY)
    print(f"       → {len(company_rows)} 行取得")

    # --- 解析 ---
    db_data    = parse_db(db_rows)
    split_data = parse_split(split_rows)
    companies  = parse_companies(company_rows)

    # 今週月曜日の日付ラベル（スプレッドシートの B2 セル）
    week_label = cell(db_rows[1], 1) if len(db_rows) > 1 else now.strftime("%Y/%m/%d")

    # --- スナップショット作成 ---
    snapshot = {
        "fetchedAt": now.isoformat(),
        "weekLabel": week_label,
        "db":        db_data,
        "split":     split_data,
        "companies": companies,
    }

    # --- 既存データ読み込み ---
    if os.path.exists(OUTPUT_PATH):
        with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
            existing = json.load(f)
    else:
        existing = {"snapshots": []}

    snapshots = existing.get("snapshots", [])

    # 同じ週ラベル or 同日取得のスナップは上書き、それ以外は追加
    replaced = False
    today_str = now.strftime("%Y-%m-%d")
    for i, s in enumerate(snapshots):
        fetched_day = (s.get("fetchedAt") or "")[:10]
        if fetched_day == today_str or s.get("weekLabel") == week_label:
            snapshots[i] = snapshot
            replaced = True
            print(f"  既存スナップショット ({week_label}) を上書きしました")
            break

    if not replaced:
        snapshots.append(snapshot)
        print(f"  新しいスナップショット ({week_label}) を追加しました（合計 {len(snapshots)} 件）")

    # 上限を超えたら古いものを削除
    if len(snapshots) > MAX_SNAPS:
        snapshots = snapshots[-MAX_SNAPS:]
        print(f"  古いスナップショットを削除し {MAX_SNAPS} 件に制限しました")

    # --- 保存 ---
    output = {
        "lastUpdated": now.isoformat(),
        "snapshots":   snapshots,
    }
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"  💾 {OUTPUT_PATH} に保存しました")

    # --- サマリー表示 ---
    mt = db_data["monthly"]["thisMonth"] or {}
    st = split_data["monthly"]["thisMonth"] or {}
    print()
    print("  【今月サマリー】")
    print(f"  全DB応募数合計 : {mt.get('total', 0)}"
          f"  (CIRCUS:{mt.get('circus',0)} / CA:{mt.get('crowdAgent',0)} / JoBins:{mt.get('jobins',0)})")
    print(f"  アライアンス   : {st.get('alliance', 0)}")
    print(f"  自社           : {st.get('own', 0)}")
    print(f"  企業数         : {len(companies)}")

    print(f"\n[完了] {datetime.now(JST).strftime('%Y-%m-%d %H:%M:%S JST')}")


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as e:
        msg = str(e)
        if "401" in msg:
            print(f"\n⚠️  {msg}", file=sys.stderr)
            print("スプレッドシートの共有設定を『リンクを知っている全員が閲覧可』に変更してください。")
        else:
            print(f"[ERROR] {msg}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
