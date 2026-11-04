import sqlite3
import feedparser
import requests
from bs4 import BeautifulSoup
import os
import re
import json
import html as html_mod
import math
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

# ==========================================================================
# KONFIGURASI PATH & URL (PORTABEL & OTOMATIS)
# ==========================================================================
BASE_DIR = Path(__file__).parent.resolve()
DB_PATH = str(BASE_DIR / "database_hasil5" / "database.sqlite")
RSS_FILE = str(BASE_DIR / "rss_list.txt")
OUTPUT_DIR = BASE_DIR 

# ✅ DETEKSI OTOMATIS NAMA REPO GITHUB
REPO_FULL_NAME = os.environ.get("GITHUB_REPOSITORY", "railfansday/tekno")
USERNAME = REPO_FULL_NAME.split("/")[0]
REPO_NAME = REPO_FULL_NAME.split("/")[1]
BASE_URL = f"https://{USERNAME}.github.io/{REPO_NAME}"

ARTICLES_PER_PAGE = 100 
SITE_NAME = "Portal Berita Indonesia"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

ADSTERRA_CODE = """
<div style="text-align:center; margin:20px 0;">
    <script type="text/javascript">
        atOptions = {'key' : 'f9e40eb3b3cd05f1038aac7b36b33ad2','format' : 'iframe','height' : 90,'width' : 728,'params' : {}};
    </script>
    <script type="text/javascript" src="https://upholdbrilliancerecast.com/f9e40eb3b3cd05f1038aac7b36b33ad2/invoke.js"></script>
</div>
"""

# ==========================================================================
# BAGIAN 1: FUNGSI IMPORT (SCRAPING)
# ==========================================================================
def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS artikel (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            link TEXT UNIQUE, judul TEXT, konten TEXT, gambar TEXT,
            tanggal_publish TEXT, tanggal_update TEXT
        )
    """)
    cursor.execute("PRAGMA table_info(artikel)")
    columns = [col[1] for col in cursor.fetchall()]
    if 'gambar' not in columns:
        cursor.execute("ALTER TABLE artikel ADD COLUMN gambar TEXT")
    if 'tanggal_publish' not in columns:
        cursor.execute("ALTER TABLE artikel ADD COLUMN tanggal_publish TEXT")
    if 'tanggal_update' not in columns:
        cursor.execute("ALTER TABLE artikel ADD COLUMN tanggal_update TEXT")
    conn.commit()
    conn.close()

def extract_full_content(url):
    try:
        response = requests.get(url, headers=HEADERS, timeout=8)
        if response.status_code != 200: return None, None, None
        soup = BeautifulSoup(response.text, 'html.parser')

        image_url = None
        og_image = soup.find('meta', property='og:image') or soup.find('meta', attrs={'name': 'og:image'})
        if og_image and og_image.get('content'): image_url = og_image['content']

        tanggal = None
        for meta_prop in ['article:published_time', 'article:modified_time', 'datePublished']:
            meta = soup.find('meta', property=meta_prop) or soup.find('meta', attrs={'name': meta_prop})
            if meta and meta.get('content'):
                tanggal = meta['content']
                break

        paragraphs = soup.find_all('p')
        full_text = []
        for p in paragraphs:
            text = p.get_text().strip()
            if len(text) > 40 and not any(x in text.lower() for x in ['cookie', 'privacy policy', 'subscribe', 'all rights reserved', 'terms of use']):
                full_text.append(f"<p>{text}</p>")

        return "\n".join(full_text) if full_text else None, image_url, tanggal
    except Exception:
        return None, None, None

def process_single_feed(feed_url):
    feed_url = feed_url.strip()
    if not feed_url or feed_url.startswith("#"): return 0, 0

    baru = 0
    duplikat = 0

    try:
        feed = feedparser.parse(feed_url)
        conn = sqlite3.connect(DB_PATH, timeout=15)
        cursor = conn.cursor()

        # Batasi 5 artikel terbaru per feed agar tidak timeout
        for entry in feed.entries[:5]:
            link = entry.get('link', '')
            judul = entry.get('title', 'Tanpa Judul')
            if not link: continue

            cursor.execute("SELECT id FROM artikel WHERE link = ?", (link,))
            if cursor.fetchone():
                duplikat += 1
                continue

            full_text_html, image_url, tanggal = extract_full_content(link)
            if not full_text_html:
                full_text_html = f"<p>{entry.get('summary', 'Konten tidak tersedia.')}</p>"

            if not tanggal:
                tanggal = datetime.now().isoformat()

            tanggal_update = datetime.now().isoformat()
            
            img_tag = f'<div style="text-align:center; margin-bottom:20px;"><img src="{image_url}" alt="{judul}" style="max-width:100%; height:auto; border-radius:8px;" loading="lazy"></div>' if image_url else ""
            
            konten_lengkap = f"""
            {img_tag}
            {ADSTERRA_CODE}
            <div class="article-body" style="font-size: 16px; line-height: 1.8; color: #333;">
                {full_text_html}
                <p style="margin-top: 25px;">
                    <a href="{link}" target="_blank" rel="nofollow noopener" style="color: #0d6efd; text-decoration: none; font-weight: bold;">
                        &rarr; Baca artikel asli
                    </a>
                </p>
            </div>
            {ADSTERRA_CODE}
            """

            try:
                cursor.execute(
                    "INSERT INTO artikel (link, judul, konten, gambar, tanggal_publish, tanggal_update) VALUES (?, ?, ?, ?, ?, ?)",
                    (link, judul, konten_lengkap, image_url, tanggal, tanggal_update)
                )
                conn.commit()
                baru += 1
            except sqlite3.IntegrityError:
                duplikat += 1
            except Exception:
                pass
        conn.close()
    except Exception:
        pass
    return baru, duplikat

def jalankan_scraping():
    if not os.path.exists(RSS_FILE):
        print(f"[ERROR] File {RSS_FILE} tidak ditemukan!")
        return
    init_db()
    with open(RSS_FILE, "r", encoding="utf-8") as f:
        rss_urls = [line.strip() for line in f if line.strip() and not line.startswith("#")]
    
    print(f"[*] Memulai scraping dari {len(rss_urls)} RSS...")
    total_baru, total_duplikat = 0, 0
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(process_single_feed, url): url for url in rss_urls}
        for future in as_completed(futures):
            b, d = future.result()
            total_baru += b
            total_duplikat += d

    print(f"[SELESAI SCRAPING] Baru: {total_baru} | Duplikat: {total_duplikat}\n")


# ==========================================================================
# BAGIAN 2: FUNGSI EXPORT (GENERATE HTML - 100 ARTIKEL/HALAMAN)
# ==========================================================================
def esc(text):
    return html_mod.escape(str(text)) if text else ""

HTML_HEAD = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="robots" content="index, follow, max-image-preview:large">
<title>{title}</title>
<meta name="description" content="{desc}">
<link rel="canonical" href="{canonical}">
<style>
body{{font-family:'Segoe UI',system-ui,sans-serif;background:#f4f6f8;margin:0;padding:20px;color:#333;line-height:1.6}}
.container{{max-width:1000px;margin:0 auto}}
header{{background:#1e293b;color:#fff;padding:20px;border-radius:12px;margin-bottom:20px;text-align:center;box-shadow:0 4px 6px rgba(0,0,0,.1)}}
header h1{{margin:0;font-size:26px;letter-spacing:1px}}
nav{{background:#fff;padding:10px;border-radius:12px;margin-bottom:24px;display:flex;gap:8px;flex-wrap:wrap;justify-content:center;box-shadow:0 2px 4px rgba(0,0,0,.05)}}
nav a{{padding:8px 16px;border-radius:8px;color:#475569;text-decoration:none;font-weight:600;transition:all .2s;font-size:14px}}
nav a:hover,nav a.active{{background:#2563eb;color:#fff}}
.article-card{{background:#fff;padding:24px;margin-bottom:24px;border-radius:12px;box-shadow:0 2px 8px rgba(0,0,0,.06);border-left:4px solid #2563eb}}
.article-card h2{{margin:0 0 12px;font-size:22px;color:#1e293b}}
.article-meta{{font-size:13px;color:#64748b;margin-bottom:16px;padding-bottom:12px;border-bottom:1px solid #e2e8f0}}
.article-meta a{{color:#2563eb;text-decoration:none;font-weight:bold}}
.article-body{{font-size:16px;line-height:1.8;color:#334155}}
.article-body img{{max-width:100%;height:auto;border-radius:8px;margin:16px 0}}
.pagination{{text-align:center;margin:40px 0;display:flex;flex-wrap:wrap;justify-content:center;gap:8px}}
.pagination a{{display:inline-block;padding:10px 16px;border-radius:8px;text-decoration:none;color:#334155;font-weight:bold;background:#fff;box-shadow:0 2px 4px rgba(0,0,0,.05);transition:all .2s}}
.pagination a:hover{{background:#f1f5f9}}
.pagination a.active{{background:#2563eb;color:#fff}}
footer{{text-align:center;color:#64748b;font-size:14px;padding:30px 0;border-top:1px solid #e2e8f0;margin-top:20px}}
</style>
</head>
<body>
<div class="container">
<header><h1>{site}</h1></header>
{nav}
"""

HTML_FOOT = """<footer>&copy; {year} {site}. All rights reserved.</footer>
</div></body></html>"""

def render_page(title, desc, canonical, rows, page_nav="", filename="index.html"):
    cards = ""
    for row in rows:
        judul = row["judul"]
        konten = row["konten"]
        gambar = row["gambar"]
        link = row["link"]
        tanggal = row["tanggal_publish"]
        aid = row["id"]

        img = f'<img src="{esc(gambar)}" alt="{esc(judul)}" loading="lazy">' if gambar else ""
        
        # Schema per artikel agar Google membaca tiap berita secara mandiri
        schema_json = json.dumps({
            "@context": "https://schema.org",
            "@type": "NewsArticle",
            "headline": judul,
            "image": [gambar] if gambar else [],
            "datePublished": tanggal or datetime.now().isoformat(),
            "dateModified": tanggal or datetime.now().isoformat(),
            "mainEntityOfPage": f"{BASE_URL}/{filename}"
        }, ensure_ascii=False)

        cards += f"""
<article class="article-card">
    <script type="application/ld+json">{schema_json}</script>
    <h2>{esc(judul)}</h2>
    <div class="article-meta">
        📅 {tanggal[:10] if tanggal else '-'} &nbsp;|&nbsp; 
        <a href="{esc(link)}" target="_blank" rel="nofollow">Read Source &rarr;</a>
    </div>
    <div class="article-body">
        {img}
        {konten or ""}
        {ADSTERRA_CODE}
    </div>
</article>"""

    head = HTML_HEAD.format(title=esc(title), desc=esc(desc), canonical=esc(canonical), site=esc(SITE_NAME), nav='') # Nav disederhanakan
    body = f'<h1 style="color:#1e293b;border-left:5px solid #2563eb;padding-left:15px;margin-bottom:30px;">{esc(title)}</h1>'
    body += cards + page_nav
    return head + body + HTML_FOOT.format(year=datetime.now().year, site=esc(SITE_NAME))

def jalankan_export():
    print(f"[*] Exporting HTML ({ARTICLES_PER_PAGE} artikel per halaman)...")
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    c.execute("SELECT * FROM artikel ORDER BY tanggal_publish DESC, id DESC")
    all_rows = c.fetchall()
    total = len(all_rows)
    print(f"   Total articles: {total}")

    if total > 0:
        pages = [all_rows[i:i+ARTICLES_PER_PAGE] for i in range(0, total, ARTICLES_PER_PAGE)]
        
        for i, page_rows in enumerate(pages, start=1):
            if i == 1:
                fn, canon, title = "index.html", f"{BASE_URL}/", f"{SITE_NAME} - Latest News"
            else:
                fn, canon, title = f"page{i}.html", f"{BASE_URL}/{fn}", f"{SITE_NAME} - Page {i}"

            nav = '<div class="pagination">'
            for j in range(1, len(pages)+1):
                href = "index.html" if j == 1 else f"page{j}.html"
                a = ' class="active"' if j == i else ''
                nav += f'<a href="{href}"{a}>{j}</a>'
            nav += '</div>'

            html = render_page(title, f"Page {i}", canon, page_rows, nav, filename=fn)
            with open(os.path.join(OUTPUT_DIR, fn), "w", encoding="utf-8") as f:
                f.write(html)

    # SITEMAP & ROBOTS (Bersih & Sesuai Standar Google)
    today = datetime.now().strftime('%Y-%m-%d')
    xml = ['<?xml version="1.0" encoding="UTF-8"?>', '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
    pages_count = max(1, (total + ARTICLES_PER_PAGE - 1) // ARTICLES_PER_PAGE)
    
    for i in range(1, pages_count+1):
        url = f"{BASE_URL}/" if i == 1 else f"{BASE_URL}/page{i}.html"
        xml.append(f'<url><loc>{url}</loc><lastmod>{today}</lastmod><priority>1.0</priority></url>')
    
    xml.append('</urlset>')
    
    with open(os.path.join(OUTPUT_DIR, "sitemap.xml"), "w", encoding="utf-8") as f:
        f.write("\n".join(xml))
    with open(os.path.join(OUTPUT_DIR, "robots.txt"), "w", encoding="utf-8") as f:
        f.write(f"User-agent: *\nAllow: /\n\nSitemap: {BASE_URL}/sitemap.xml\n")
    print(f"   ✅ sitemap.xml & robots.txt")
    conn.close()

# ==========================================================================
# BAGIAN 3: JALANKAN SEMUANYA
# ==========================================================================
if __name__ == "__main__":
    print("=" * 55)
    print("MULAI PROSES IMPORT & EXPORT (100 ARTIKEL/HALAMAN)")
    print("=" * 55)
    
    # 1. Jalankan Scraping
    jalankan_scraping()
    
    # 2. Jalankan Export
    print("\n" + "=" * 55)
    print("MULAI EXPORT HTML")
    print("=" * 55)
    jalankan_export()
    
    print("\n[PROSES SELESAI]")
