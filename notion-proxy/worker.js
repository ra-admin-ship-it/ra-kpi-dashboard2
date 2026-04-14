// Cloudflare Worker: Notion API Proxy for RA KPI Dashboard
// CORS制限を回避してブラウザからNotion APIを呼び出す中継サーバー

const NOTION_API = "https://api.notion.com/v1";
const NOTION_VERSION = "2022-06-28";
const DATABASE_ID = "a75237142d61458e8d821d7fe12a7b89";

// 許可するオリジン
const ALLOWED_ORIGINS = [
  "https://ra-admin-ship-it.github.io",
  "http://localhost",
  "http://127.0.0.1",
];

function corsHeaders(origin) {
  const allowed = ALLOWED_ORIGINS.some(o => origin?.startsWith(o));
  return {
    "Access-Control-Allow-Origin": allowed ? origin : ALLOWED_ORIGINS[0],
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Max-Age": "86400",
  };
}

export default {
  async fetch(request, env) {
    const origin = request.headers.get("Origin") || "";
    const cors = corsHeaders(origin);

    // Preflight
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: cors });
    }

    const url = new URL(request.url);
    const path = url.pathname;

    try {
      // Health check
      if (path === "/health") {
        return new Response(JSON.stringify({ status: "ok" }), {
          headers: { ...cors, "Content-Type": "application/json" },
        });
      }

      // Create page in Notion
      if (path === "/create-page" && request.method === "POST") {
        const body = await request.json();

        // Build Notion page properties
        const properties = {
          "名前": { title: [{ text: { content: body.companyName || "" } }] },
        };

        if (body.recommendMethod?.length) {
          properties["推薦方法"] = {
            multi_select: body.recommendMethod.map(name => ({ name })),
          };
        }
        if (body.contractCompany) {
          properties["契約企業名"] = { select: { name: body.contractCompany } };
        }
        if (body.volumeEstimate) {
          properties["目安処理件数"] = { select: { name: body.volumeEstimate } };
        }
        if (body.url) {
          properties["userDefined:URL"] = { url: body.url };
        }
        if (body.notes) {
          properties["RAからの注意点"] = {
            rich_text: [{ text: { content: body.notes } }],
          };
        }

        // ページ本文（担当者連絡先を記載）
        const children = [];
        if (body.contactName || body.contactEmail || body.contactPhone) {
          children.push({
            object: "block", type: "heading_2",
            heading_2: { rich_text: [{ text: { content: "📞 先方担当者情報" } }] },
          });
          if (body.contactName) {
            children.push({
              object: "block", type: "bulleted_list_item",
              bulleted_list_item: { rich_text: [{ text: { content: `担当者名: ${body.contactName}` } }] },
            });
          }
          if (body.contactEmail) {
            children.push({
              object: "block", type: "bulleted_list_item",
              bulleted_list_item: { rich_text: [{ text: { content: `メール: ${body.contactEmail}` } }] },
            });
          }
          if (body.contactPhone) {
            children.push({
              object: "block", type: "bulleted_list_item",
              bulleted_list_item: { rich_text: [{ text: { content: `電話番号: ${body.contactPhone}` } }] },
            });
          }
        }

        const notionRes = await fetch(`${NOTION_API}/pages`, {
          method: "POST",
          headers: {
            "Authorization": `Bearer ${env.NOTION_TOKEN}`,
            "Notion-Version": NOTION_VERSION,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            parent: { database_id: DATABASE_ID },
            properties,
            children,
          }),
        });

        const result = await notionRes.json();

        if (!notionRes.ok) {
          return new Response(JSON.stringify({ error: result.message || "Notion API error" }), {
            status: notionRes.status,
            headers: { ...cors, "Content-Type": "application/json" },
          });
        }

        return new Response(JSON.stringify({
          success: true,
          pageId: result.id,
          pageUrl: result.url,
        }), {
          headers: { ...cors, "Content-Type": "application/json" },
        });
      }

      return new Response(JSON.stringify({ error: "Not found" }), {
        status: 404,
        headers: { ...cors, "Content-Type": "application/json" },
      });

    } catch (e) {
      return new Response(JSON.stringify({ error: e.message }), {
        status: 500,
        headers: { ...cors, "Content-Type": "application/json" },
      });
    }
  },
};
