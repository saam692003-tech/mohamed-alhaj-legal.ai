"""Crawler for publicly accessible Yemeni legal texts and downloadable PDFs."""
import os,re,time,hashlib,urllib.parse,urllib.robotparser,io
from datetime import datetime,timezone
import requests
from bs4 import BeautifulSoup
import psycopg
from psycopg.rows import dict_row
from pypdf import PdfReader

DATABASE_URL=os.getenv("DATABASE_URL","")
SOURCES=[
 ("https://www.agoye.gov.ye","النيابة العامة اليمنية","official_law"),
 ("https://www.sc-yemen.com","المحكمة العليا اليمنية","precedent"),
 ("https://www.yemenilaw.com","الموقع القانوني اليمني","legal_library"),
]
UA="MohamedAlhajjLegalResearch/1.0 (+legal research portal)"
def db():
 if not DATABASE_URL: raise RuntimeError("DATABASE_URL missing")
 return psycopg.connect(DATABASE_URL,row_factory=dict_row,connect_timeout=15)
def now(): return datetime.now(timezone.utc).isoformat(timespec="seconds")
def robots(session,root):
 rp=urllib.robotparser.RobotFileParser()
 try:
  r=session.get(root+"/robots.txt",timeout=15)
  if r.status_code==404: rp.parse([])
  else: r.raise_for_status(); rp.parse(r.text.splitlines())
 except requests.RequestException:
  rp.parse([])
 return rp
def text_of(soup):
 for x in soup(["script","style","noscript","svg","header","footer","nav","form"]): x.decompose()
 main=soup.find("main") or soup.find("article") or soup.find(id=re.compile("content|main",re.I)) or soup.body or soup
 return "\n".join(re.sub(r"\s+"," ",s).strip() for s in main.stripped_strings if s.strip())
def classify(url,title):
 u=(url+" "+title).lower()
 if ".pdf" in url.lower(): return "pdf"
 if any(x in u for x in ["precedent","judgment","court","قض","حكم","مبدأ"]): return "precedent"
 if any(x in u for x in ["contract","agreement","عقد","اتفاق","صيغة","نموذج"]): return "contract"
 return "official_law"
def save(cur,title,body,url,kind,source,pdf=None,content_type="text/html"):
 digest=hashlib.sha256((body or "").encode()).hexdigest()
 cur.execute("SELECT id FROM documents WHERE source_url=%s",(url,))
 old=cur.fetchone()
 if old:
  cur.execute("""UPDATE documents SET title=%s,text=%s,url=%s,kind=%s,source_name=%s,
    content_hash=%s,fetched_at=%s,verification_status=%s,is_active=1,
    pdf_bytes=COALESCE(%s,pdf_bytes),content_type=%s WHERE id=%s""",
   (title,body,url,kind,source,digest,now(),"مستورد من المصدر الأصلي؛ راجع تاريخ النفاذ والتعديلات",pdf,content_type,old["id"]))
  return old["id"]
 cur.execute("""INSERT INTO documents(title,text,url,kind,source_name,source_url,content_hash,
  fetched_at,verification_status,is_active,pdf_bytes,content_type)
  VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,1,%s,%s) RETURNING id""",
  (title,body,url,kind,source,url,digest,now(),"مستورد من المصدر الأصلي؛ راجع تاريخ النفاذ والتعديلات",pdf,content_type))
 return cur.fetchone()["id"]
def crawl_all(max_pages=1200,delay=1.0):
 s=requests.Session(); s.headers.update({"User-Agent":UA,"Accept":"text/html,application/pdf,*/*"})
 total_saved=0; pages=0; errors=0; runs=[]
 for root,source,default_kind in SOURCES:
  rp=robots(s,root); queue=[root+"/"]
  # Known landing pages often expose document catalogs.
  queue += [root+p for p in ["/library","/laws","/legislation","/library/yemeni-laws/","/library/judicial-precedents/","/contracts/"]]
  seen=set(); saved=0; err=0
  while queue and pages<max_pages:
   url=queue.pop(0)
   if url in seen: continue
   seen.add(url)
   p=urllib.parse.urlsplit(url)
   if p.scheme not in ("http","https") or p.netloc.lower() not in (urllib.parse.urlsplit(root).netloc.lower(),): continue
   if not rp.can_fetch(UA,url): continue
   try:
    r=s.get(url,timeout=35); 
    if r.status_code in (403,404,410): continue
    r.raise_for_status()
    ct=r.headers.get("Content-Type","").lower()
    if "pdf" in ct or p.path.lower().endswith(".pdf"):
     title=urllib.parse.unquote(p.path.rsplit("/",1)[-1]) or "وثيقة قانونية PDF"
     raw=r.content
     try:
      reader=PdfReader(io.BytesIO(raw))
      body="\n".join(page.extract_text() or "" for page in reader.pages).strip()
     except Exception: body=""
     with db() as conn:
      with conn.cursor() as cur: save(cur,title,body or "ملف PDF محفوظ من المصدر الأصلي.",url,"pdf",source,raw,"application/pdf")
      conn.commit()
     saved+=1; total_saved+=1; pages+=1; time.sleep(delay); continue
    if "html" not in ct and not r.text.lstrip().startswith("<"): continue
    soup=BeautifulSoup(r.text,"html.parser")
    title_node=soup.find("h1") or soup.find("h2") or soup.title
    title=title_node.get_text(" ",strip=True) if title_node else url
    body=text_of(soup)
    if len(body)>250 and not any(url==root+x for x in ["/","/library","/laws"]):
     kind=classify(url,title)
     with db() as conn:
      with conn.cursor() as cur: save(cur,title,body,url,kind,source)
      conn.commit()
     saved+=1; total_saved+=1
    for a in soup.find_all("a",href=True):
     href=urllib.parse.urldefrag(urllib.parse.urljoin(url,a["href"]))[0]
     hp=urllib.parse.urlsplit(href)
     if hp.scheme in ("http","https") and hp.netloc.lower()==p.netloc.lower():
      if hp.path.lower().endswith((".pdf",".html",".htm")) or hp.path in ("/",) or hp.path.lower().endswith((".php",".aspx")) or any(k in (hp.path+hp.query).lower() for k in ["law","leg","court","judg","preced","contract","library","قانون","قض","حكم","عقد"]):
       if href not in seen and href not in queue: queue.append(href)
    pages+=1; time.sleep(delay)
   except Exception as e:
    err+=1; errors+=1
  runs.append({"source":source,"pages":len(seen),"saved":saved,"errors":err})
 return {"status":"complete" if errors==0 else "partial","pages_seen":pages,"documents_saved":total_saved,"errors":errors,"sources":runs}
def index_catalogs():
 return {"note":"All accessible same-domain pages and PDF files are crawled by crawl_all."}
if __name__=="__main__": print(crawl_all(int(os.getenv("CRAWL_MAX_PAGES","1200")),float(os.getenv("CRAWL_DELAY","1.0"))))
