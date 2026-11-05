"""Respectful Yemenilaw public-page crawler with historical snapshots and change detection."""
import os, re, time, hashlib, sqlite3, argparse, urllib.parse, urllib.robotparser, json, difflib
from datetime import datetime, timezone
from collections import deque
from pathlib import Path
import requests
from bs4 import BeautifulSoup

ROOT = 'https://www.yemenilaw.com'
SECTIONS = {
    'laws': ROOT + '/library/yemeni-laws/',
    'precedents': ROOT + '/library/judicial-precedents/',
    'contracts': ROOT + '/contracts/',
}
PREFIXES = tuple(urllib.parse.urlparse(x).path for x in SECTIONS.values())
AGENT = 'YemenLegalResearchIndexer/2.0 (public legal index; respectful crawl)'
DB = os.getenv('DATABASE_PATH', str(Path(__file__).with_name('legal_library.db')))

def now(): return datetime.now(timezone.utc).isoformat(timespec='seconds')
def norm_url(url):
    p=urllib.parse.urlsplit(url)
    path=re.sub(r'/+', '/', p.path or '/')
    if path != '/' and not path.endswith('/') and '.' not in path.rsplit('/',1)[-1]: path += '/'
    return urllib.parse.urlunsplit((p.scheme.lower(),p.netloc.lower(),path,p.query,''))
def allowed(url):
    p=urllib.parse.urlsplit(url)
    return p.scheme in ('http','https') and p.netloc.lower() in ('www.yemenilaw.com','yemenilaw.com') and any(p.path.startswith(x) for x in PREFIXES)
def connect(path=DB):
    c=sqlite3.connect(path); c.row_factory=sqlite3.Row; return c
def setup(c):
    c.executescript('''
    CREATE TABLE IF NOT EXISTS documents(
      id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL, text TEXT NOT NULL,
      url TEXT DEFAULT '', kind TEXT DEFAULT 'legal_source', source_name TEXT DEFAULT '',
      created_at TEXT DEFAULT CURRENT_TIMESTAMP, source_url TEXT DEFAULT '', content_hash TEXT DEFAULT '',
      fetched_at TEXT DEFAULT '', verification_status TEXT DEFAULT 'غير متحقق', is_active INTEGER DEFAULT 1);
    CREATE TABLE IF NOT EXISTS crawl_runs(
      id INTEGER PRIMARY KEY AUTOINCREMENT, started_at TEXT NOT NULL, finished_at TEXT,
      status TEXT NOT NULL, pages_seen INTEGER DEFAULT 0, pages_saved INTEGER DEFAULT 0,
      errors INTEGER DEFAULT 0, detail TEXT DEFAULT '');
    CREATE TABLE IF NOT EXISTS document_snapshots(
      id INTEGER PRIMARY KEY AUTOINCREMENT, document_id INTEGER NOT NULL, run_id INTEGER NOT NULL,
      title TEXT NOT NULL, content TEXT NOT NULL, content_hash TEXT NOT NULL, captured_at TEXT NOT NULL,
      FOREIGN KEY(document_id) REFERENCES documents(id), FOREIGN KEY(run_id) REFERENCES crawl_runs(id));
    CREATE INDEX IF NOT EXISTS ix_snapshots_document ON document_snapshots(document_id,captured_at);
    CREATE TABLE IF NOT EXISTS change_events(
      id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER NOT NULL, document_id INTEGER,
      change_type TEXT NOT NULL, title TEXT NOT NULL, url TEXT NOT NULL, detected_at TEXT NOT NULL,
      old_hash TEXT DEFAULT '', new_hash TEXT DEFAULT '', summary TEXT DEFAULT '',
      FOREIGN KEY(run_id) REFERENCES crawl_runs(id));
    CREATE INDEX IF NOT EXISTS ix_changes_run ON change_events(run_id);
    CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(title,text,content='documents',content_rowid='id',tokenize='unicode61');
    CREATE TRIGGER IF NOT EXISTS documents_ai AFTER INSERT ON documents BEGIN INSERT INTO documents_fts(rowid,title,text) VALUES(new.id,new.title,new.text); END;
    CREATE TRIGGER IF NOT EXISTS documents_ad AFTER DELETE ON documents BEGIN INSERT INTO documents_fts(documents_fts,rowid,title,text) VALUES('delete',old.id,old.title,old.text); END;
    CREATE TRIGGER IF NOT EXISTS documents_au AFTER UPDATE ON documents BEGIN
      INSERT INTO documents_fts(documents_fts,rowid,title,text) VALUES('delete',old.id,old.title,old.text);
      INSERT INTO documents_fts(rowid,title,text) VALUES(new.id,new.title,new.text); END;
    ''')
    cols={r[1] for r in c.execute('PRAGMA table_info(documents)')}
    if 'is_active' not in cols: c.execute('ALTER TABLE documents ADD COLUMN is_active INTEGER DEFAULT 1')
    c.execute('CREATE INDEX IF NOT EXISTS ix_documents_source_url ON documents(source_url)')
    # Repair an FTS index that may have been created before its source table was populated.
    c.execute("INSERT INTO documents_fts(documents_fts) VALUES('rebuild')")
    c.commit()
def extract(soup):
    for t in soup(['script','style','noscript','svg','header','footer','nav','form']): t.decompose()
    main=soup.find('main') or soup.find('article') or soup.body or soup
    text='\n'.join(re.sub(r'\s+',' ',x).strip() for x in main.stripped_strings if x.strip())
    h=main.find(['h1','h2']) or soup.find(['h1','h2'])
    title=h.get_text(' ',strip=True) if h else (soup.title.get_text(' ',strip=True) if soup.title else 'صفحة قانونية')
    return title,text
def section_for(url):
    p=urllib.parse.urlsplit(url).path
    if p.startswith(PREFIXES[0]): return 'law_page'
    if p.startswith(PREFIXES[1]): return 'precedent_page'
    return 'contract_page'
def crawl(db_path=DB, starts=None, max_pages=700, delay=1.2, timeout=25):
    starts=starts or list(SECTIONS.values()); c=connect(db_path); setup(c)
    started=now(); cur=c.execute("INSERT INTO crawl_runs(started_at,status) VALUES(?, 'running')",(started,)); run_id=cur.lastrowid; c.commit()
    session=requests.Session(); session.headers.update({'User-Agent':AGENT,'Accept':'text/html,application/xhtml+xml'})
    try:
        rr=session.get(ROOT+'/robots.txt',timeout=timeout); rr.raise_for_status()
        rp=urllib.robotparser.RobotFileParser(); rp.parse(rr.text.splitlines())
    except Exception as e:
        c.execute("UPDATE crawl_runs SET status='blocked',finished_at=?,detail=? WHERE id=?",(now(),f'تعذر قراءة robots.txt: {e}',run_id)); c.commit(); c.close()
        return {'run_id':run_id,'status':'blocked','error':str(e),'pages_seen':0,'pages_saved':0,'changes':0}
    q=deque((norm_url(x), urllib.parse.urlsplit(x).path) for x in starts if allowed(x))
    seen=set(); successful=set(); errors=0; robots_skipped=0; saved=0; changes=0
    while q and len(seen)<max_pages:
        url, seed_path=q.popleft()
        if url in seen or not allowed(url): continue
        seen.add(url)
        if not rp.can_fetch(AGENT,url):
            robots_skipped+=1
            continue
        try:
            r=session.get(url,timeout=timeout); r.raise_for_status()
            if 'text/html' not in r.headers.get('Content-Type','').lower(): continue
            soup=BeautifulSoup(r.text,'html.parser'); title,body=extract(soup)
            if len(body)<100: continue
            digest=hashlib.sha256(body.encode('utf-8')).hexdigest(); fetched=now(); kind=section_for(url)
            old=c.execute('SELECT id,title,text,content_hash,is_active FROM documents WHERE source_url=?',(url,)).fetchone()
            if old is None:
                cur=c.execute('''INSERT INTO documents(title,text,url,kind,source_name,source_url,content_hash,fetched_at,verification_status,is_active)
                                 VALUES(?,?,?,?,?,?,?,?,?,1)''',(title,body,url,kind,'Yemenilaw',url,digest,fetched,'محتوى منشور بالمصدر؛ لم يُتحقق من رسميته'))
                doc_id=cur.lastrowid; c.execute('INSERT INTO document_snapshots(document_id,run_id,title,content,content_hash,captured_at) VALUES(?,?,?,?,?,?)',(doc_id,run_id,title,body,digest,fetched))
                c.execute('INSERT INTO change_events(run_id,document_id,change_type,title,url,detected_at,new_hash,summary) VALUES(?,?,?,?,?,?,?,?)',(run_id,doc_id,'new',title,url,fetched,digest,'صفحة جديدة مرصودة في المصدر'))
                changes+=1; saved+=1
            else:
                doc_id=old['id']
                if old['content_hash'] != digest or old['title'] != title or not old['is_active']:
                    c.execute('UPDATE documents SET title=?,text=?,url=?,kind=?,content_hash=?,fetched_at=?,verification_status=?,is_active=1 WHERE id=?',(title,body,url,kind,digest,fetched,'محتوى منشور بالمصدر؛ لم يُتحقق من رسميته',doc_id))
                    diff='\n'.join(list(difflib.unified_diff(old['text'].splitlines(),body.splitlines(),fromfile='النسخة السابقة',tofile='النسخة الحالية',n=1))[:14])
                    summary=('تغير العنوان أو المحتوى. مقتطف المقارنة: '+diff[:1800]) if diff else 'تغيرت بصمة المحتوى أو بيانات الصفحة.'
                    c.execute('INSERT INTO change_events(run_id,document_id,change_type,title,url,detected_at,old_hash,new_hash,summary) VALUES(?,?,?,?,?,?,?,?,?)',(run_id,doc_id,'updated',title,url,fetched,old['content_hash'],digest,summary))
                    changes+=1; saved+=1
                c.execute('INSERT INTO document_snapshots(document_id,run_id,title,content,content_hash,captured_at) VALUES(?,?,?,?,?,?)',(doc_id,run_id,title,body,digest,fetched))
            successful.add(url); c.commit()
            for a in soup.find_all('a',href=True):
                nxt=norm_url(urllib.parse.urljoin(url,a['href']))
                if allowed(nxt) and nxt not in seen: q.append((nxt,seed_path))
            time.sleep(max(1.0,delay))
        except Exception as e:
            errors+=1; print(f'[تعذر التحقق] {url}: {e}')
    # Only mark removals when the section's crawl completed without errors and its queue was exhausted.
    complete=(not q and errors==0 and robots_skipped==0)
    if complete:
        crawled_prefixes={urllib.parse.urlsplit(x).path for x in starts}
        for prefix in crawled_prefixes:
            rows=c.execute('SELECT id,title,url,source_url,content_hash FROM documents WHERE source_url LIKE ? AND is_active=1',(prefix+'%',)).fetchall()
            for row in rows:
                if row['source_url'] not in successful:
                    c.execute('UPDATE documents SET fetched_at=? WHERE id=?',(now(),row['id']))
                    c.execute('INSERT INTO change_events(run_id,document_id,change_type,title,url,detected_at,old_hash,summary) VALUES(?,?,?,?,?,?,?,?)',(run_id,row['id'],'possibly_removed',row['title'],row['url'],now(),row['content_hash'],'لم تظهر الصفحة في جولة مكتملة؛ لم تُحذف من الفهرس وتحتاج إلى تحقق يدوي'))
                    changes+=1
    status='complete' if complete else 'partial'
    detail='اكتملت الجولة دون أخطاء أو صفحات محجوبة' if complete else ('جولة جزئية: بقيت روابط أو ظهرت أخطاء أو روابط يمنعها robots.txt' if errors or q or robots_skipped else '')
    c.execute('UPDATE crawl_runs SET finished_at=?,status=?,pages_seen=?,pages_saved=?,errors=?,detail=? WHERE id=?',(now(),status,len(seen),saved,errors,detail,run_id)); c.commit()
    c.close()
    return {'run_id':run_id,'status':status,'pages_seen':len(seen),'pages_saved':saved,'errors':errors,'robots_skipped':robots_skipped,'changes':changes,'detail':detail}
def report(db_path=DB, run_id=None):
    c=connect(db_path)
    run=c.execute('SELECT * FROM crawl_runs WHERE id=?',(run_id,)).fetchone() if run_id else c.execute('SELECT * FROM crawl_runs ORDER BY id DESC LIMIT 1').fetchone()
    if not run: c.close(); return {'status':'no_runs','message':'لا توجد جولات فهرسة مسجلة'}
    rows=c.execute('SELECT change_type,title,url,detected_at,summary FROM change_events WHERE run_id=? ORDER BY id',(run['id'],)).fetchall()
    out={'run':dict(run),'changes':[dict(x) for x in rows]}; c.close(); return out
if __name__=='__main__':
    p=argparse.ArgumentParser(); p.add_argument('--max-pages',type=int,default=700); p.add_argument('--delay',type=float,default=1.2); p.add_argument('--db',default=DB); p.add_argument('--section',choices=['laws','precedents','contracts','all'],default='all'); a=p.parse_args()
    starts=list(SECTIONS.values()) if a.section=='all' else [SECTIONS[a.section]]
    result=crawl(a.db,starts,a.max_pages,a.delay); print(json.dumps(result,ensure_ascii=False,indent=2)); print(json.dumps(report(a.db,result['run_id']),ensure_ascii=False,indent=2))
