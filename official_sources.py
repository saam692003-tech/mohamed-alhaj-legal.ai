"""Import official Yemeni legislation text and index court/source catalogs.
The importer does not bulk-copy third-party editorial libraries.
"""
import os, re, time, hashlib, urllib.parse, urllib.robotparser
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup
import psycopg
from psycopg.rows import dict_row

DATABASE_URL = os.getenv("DATABASE_URL", "")
AG_ROOT = "https://www.agoye.gov.ye"
COURT_ROOT = "https://www.sc-yemen.com"
YEMENILAW = "https://www.yemenilaw.com"

def db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is missing")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row, connect_timeout=15)

def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def robots_for(session, root, agent):
    rp = urllib.robotparser.RobotFileParser()
    response = session.get(root + "/robots.txt", timeout=20)
    response.raise_for_status()
    rp.parse(response.text.splitlines())
    return rp

def clean_text(soup):
    for node in soup(["script","style","noscript","svg","header","footer","nav","form"]):
        node.decompose()
    main = soup.find("main") or soup.find("article") or soup.find(id=re.compile("content|main", re.I)) or soup.body or soup
    return "\n".join(re.sub(r"\\s+", " ", x).strip() for x in main.stripped_strings if x.strip())

def upsert(cur, title, body, url, kind, source, status):
    digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
    cur.execute("SELECT id,content_hash FROM documents WHERE source_url=%s", (url,))
    old = cur.fetchone()
    if old:
        cur.execute("""UPDATE documents SET title=%s,text=%s,url=%s,kind=%s,source_name=%s,
                       content_hash=%s,fetched_at=%s,verification_status=%s,is_active=1 WHERE id=%s""",
                    (title,body,url,kind,source,digest,now(),status,old["id"]))
        return old["id"], old["content_hash"] != digest
    cur.execute("""INSERT INTO documents(title,text,url,kind,source_name,source_url,content_hash,
                   fetched_at,verification_status,is_active)
                   VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,1) RETURNING id""",
                (title,body,url,kind,source,url,digest,now(),status))
    return cur.fetchone()["id"], True

def import_official_legislation(max_pages=180, delay=1.3):
    agent = "MohamedAlhajjLegalResearch/1.0 (official-public-legal-text-index)"
    session = requests.Session()
    session.headers.update({"User-Agent": agent, "Accept":"text/html,application/pdf"})
    roots = [AG_ROOT]
    robots = {root: robots_for(session, root, agent) for root in roots}
    seeds = [
        AG_ROOT + "/page.php?cid=11&lng=arabic",
        AG_ROOT + "/page.php?cid=44&lng=arabic",
        AG_ROOT + "/page.php?cid=45&lng=arabic",
        AG_ROOT + "/page.php?cid=46&lng=arabic",
        AG_ROOT + "/page.php?cid=61",
    ]
    queue = list(dict.fromkeys(seeds))
    seen, saved, errors = set(), 0, 0
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""CREATE TABLE IF NOT EXISTS crawl_runs(
                id BIGSERIAL PRIMARY KEY, started_at TEXT NOT NULL, finished_at TEXT,
                status TEXT NOT NULL, pages_seen INTEGER DEFAULT 0, pages_saved INTEGER DEFAULT 0,
                errors INTEGER DEFAULT 0, detail TEXT DEFAULT '')""")
            cur.execute("INSERT INTO crawl_runs(started_at,status) VALUES(%s,'running') RETURNING id",(now(),))
            run_id = cur.fetchone()["id"]
        conn.commit()
        while queue and len(seen) < max_pages:
            url = queue.pop(0)
            if url in seen: continue
            seen.add(url)
            if not robots[AG_ROOT].can_fetch(agent, url): continue
            try:
                response = session.get(url, timeout=30)
                response.raise_for_status()
                ctype = response.headers.get("Content-Type","").lower()
                if "pdf" in ctype or url.lower().endswith(".pdf"):
                    title = urllib.parse.unquote(url.rsplit("/",1)[-1])
                    body = "ملف PDF رسمي متاح للتنزيل من المصدر الأصلي. لم يُستخرج النص من الملف."
                    with conn.cursor() as cur:
                        upsert(cur,title,body,url,"official_pdf","النيابة العامة اليمنية","رابط ملف رسمي؛ راجع المصدر")
                    conn.commit(); saved += 1
                    continue
                soup = BeautifulSoup(response.text,"html.parser")
                title_node = soup.find("h1") or soup.find("h2") or soup.title
                title = title_node.get_text(" ",strip=True) if title_node else "صفحة تشريعية"
                body = clean_text(soup)
                parsed = urllib.parse.urlsplit(url)
                if parsed.path.endswith("page.php") and (parsed.query.find("id=") >= 0):
                    kind = "official_law"
                    status = "نص منشور على موقع النيابة العامة؛ يلزم مطابقة الجريدة الرسمية والتعديلات"
                    if len(body) > 350:
                        with conn.cursor() as cur:
                            upsert(cur,title,body,url,kind,"النيابة العامة اليمنية",status)
                        conn.commit(); saved += 1
                # Discover only internal legislative pages and direct PDF links.
                for a in soup.find_all("a", href=True):
                    nxt = urllib.parse.urljoin(url,a["href"])
                    p = urllib.parse.urlsplit(nxt)
                    if p.scheme not in ("http","https") or p.netloc.lower() not in ("www.agoye.gov.ye","agoye.gov.ye"):
                        continue
                    if p.path.endswith(".pdf") or (p.path.endswith("page.php") and ("id=" in p.query or "cid=" in p.query)):
                        if nxt not in seen and nxt not in queue: queue.append(nxt)
                time.sleep(max(1.0,delay))
            except Exception:
                errors += 1
        with conn.cursor() as cur:
            status = "complete" if not queue and errors == 0 else "partial"
            cur.execute("""UPDATE crawl_runs SET finished_at=%s,status=%s,pages_seen=%s,
                           pages_saved=%s,errors=%s,detail=%s WHERE id=%s""",
                        (now(),status,len(seen),saved,errors,
                         "فهرسة التشريعات المنشورة في موقع النيابة العامة مع احترام robots.txt",run_id))
        conn.commit()
    return {"run_id":run_id,"status":status,"pages_seen":len(seen),"pages_saved":saved,"errors":errors}

def index_catalogs():
    """Index source links and titles only; do not copy bulk editorial content."""
    session = requests.Session()
    session.headers.update({"User-Agent":"MohamedAlhajjLegalResearch/1.0"})
    catalog_pages = [
      (COURT_ROOT,"/library","المحكمة العليا اليمنية","court_catalog"),
      (YEMENILAW,"/library/yemeni-laws/","الموقع القانوني اليمني","external_law_catalog"),
      (YEMENILAW,"/library/judicial-precedents/","الموقع القانوني اليمني","external_precedent_catalog"),
      (YEMENILAW,"/contracts/","الموقع القانوني اليمني","external_contract_catalog"),
    ]
    count = 0
    for root,path,source,kind in catalog_pages:
        try:
            rp = robots_for(session,root,"MohamedAlhajjLegalResearch/1.0")
            url = root + path
            if not rp.can_fetch("MohamedAlhajjLegalResearch/1.0",url): continue
            response = session.get(url,timeout=25); response.raise_for_status()
            soup = BeautifulSoup(response.text,"html.parser")
            with db() as conn:
                with conn.cursor() as cur:
                    title_node = soup.find("h1") or soup.title
                    title = title_node.get_text(" ",strip=True) if title_node else path
                    body = "فهرس وروابط للمحتوى الأصلي. افتح رابط المصدر للاطلاع والتنزيل وفق شروطه."
                    upsert(cur,title,body,url,kind,source,"فهرس روابط فقط؛ المحتوى يبقى لدى مصدره")
                    for a in soup.find_all("a",href=True):
                        href = urllib.parse.urljoin(url,a["href"])
                        text = a.get_text(" ",strip=True) or a.find("img").get("alt","") if a.find("img") else a.get_text(" ",strip=True)
                        if not text or len(text) < 3: continue
                        if urllib.parse.urlsplit(href).netloc.lower() != urllib.parse.urlsplit(root).netloc.lower(): continue
                        if href.rstrip("/") == url.rstrip("/"): continue
                        upsert(cur,text,"رابط إلى مادة منشورة لدى المصدر الأصلي.",href,kind,source,
                               "رابط فهرسي؛ لم يتم نسخ المحتوى")
                        count += 1
                conn.commit()
        except Exception:
            continue
    return {"catalog_links_indexed":count}
