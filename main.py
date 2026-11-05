import os, re, sqlite3, math
from contextlib import closing
from typing import Optional
from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from dotenv import load_dotenv

load_dotenv()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.getenv("DATABASE_PATH", os.path.join(BASE_DIR, "legal_library.db"))
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "change-this-admin-token")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
CHAT_MODEL = os.getenv("OPENAI_CHAT_MODEL", "gpt-4.1-mini")
EMBED_MODEL = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")

app = FastAPI(title="Mohamed Al-Hajj Legal AI API", version="0.4.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=False,
                   allow_methods=["*"], allow_headers=["*"])

def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with closing(db()) as c:
        c.execute("""CREATE TABLE IF NOT EXISTS documents(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL, text TEXT NOT NULL, url TEXT DEFAULT '',
            kind TEXT DEFAULT 'legal_source', source_name TEXT DEFAULT '',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP, source_url TEXT DEFAULT '', content_hash TEXT DEFAULT '', fetched_at TEXT DEFAULT '', verification_status TEXT DEFAULT 'غير متحقق')""")
        cols={r[1] for r in c.execute("PRAGMA table_info(documents)").fetchall()}
        for name, typ in [("source_url","TEXT DEFAULT ''"),("content_hash","TEXT DEFAULT ''"),("fetched_at","TEXT DEFAULT ''"),("verification_status","TEXT DEFAULT 'غير متحقق'")]:
            if name not in cols: c.execute(f"ALTER TABLE documents ADD COLUMN {name} {typ}")
        c.execute("ALTER TABLE documents ADD COLUMN is_active INTEGER DEFAULT 1") if "is_active" not in cols else None
        c.execute("""CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts
                     USING fts5(title, text, content='documents', content_rowid='id', tokenize='unicode61')""")
        c.executescript("""
        CREATE TRIGGER IF NOT EXISTS documents_ai AFTER INSERT ON documents BEGIN
          INSERT INTO documents_fts(rowid,title,text) VALUES(new.id,new.title,new.text);
        END;
        CREATE TRIGGER IF NOT EXISTS documents_ad AFTER DELETE ON documents BEGIN
          INSERT INTO documents_fts(documents_fts,rowid,title,text) VALUES('delete',old.id,old.title,old.text);
        END;
        CREATE TRIGGER IF NOT EXISTS documents_au AFTER UPDATE ON documents BEGIN
          INSERT INTO documents_fts(documents_fts,rowid,title,text) VALUES('delete',old.id,old.title,old.text);
          INSERT INTO documents_fts(rowid,title,text) VALUES(new.id,new.title,new.text);
        END;
        """)
        c.commit()

class AskRequest(BaseModel):
    question: str = Field(min_length=3, max_length=8000)

class IngestRequest(BaseModel):
    title: str = Field(min_length=2, max_length=500)
    text: str = Field(min_length=20)
    url: Optional[str] = ""
    kind: Optional[str] = "legal_source"
    source_name: Optional[str] = ""

def search_docs(query: str, limit: int = 8):
    terms = re.findall(r"[\w\u0600-\u06FF]+", query)
    if not terms: return []
    # FTS5 requires valid tokens; OR improves recall for natural-language Arabic queries.
    match = " OR ".join('"' + t.replace('"','') + '"' for t in terms[:12])
    with closing(db()) as c:
        rows = c.execute("""SELECT d.id,d.title,d.text,d.url,d.kind,d.source_name,
                            bm25(documents_fts) AS rank
                            FROM documents_fts JOIN documents d ON d.id=documents_fts.rowid
                            WHERE documents_fts MATCH ? AND COALESCE(d.is_active,1)=1 ORDER BY rank LIMIT ?""",
                         (match, limit)).fetchall()
    out=[]
    for r in rows:
        txt=r["text"]
        out.append({"id":r["id"],"title":r["title"],"excerpt":txt[:900],
                    "url":r["url"],"kind":r["kind"],"source_name":r["source_name"]})
    return out

@app.on_event("startup")
def startup():
    init_db()
    seed_sources()

def seed_sources():
    seeds = [
      ("فهرس القوانين اليمنية — Yemenilaw","فهرس يتضمن روابط صفحات القوانين اليمنية؛ يجب فتح كل قانون والتحقق من نصه وتعديلاته وحالة نفاذه.","https://www.yemenilaw.com/library/yemeni-laws/","index"),
      ("فهرس السوابق القضائية اليمنية — Yemenilaw","فهرس للسوابق القضائية مرتبة حسب الإصدارات والتصنيفات الموضوعية. لا يعد عنوان السابقة وحده بديلاً عن نص الحكم أو مرجعه الأصلي.","https://www.yemenilaw.com/library/judicial-precedents/","index"),
      ("السوابق المتعلقة بالأحكام القضائية — Yemenilaw","فهرس موضوعي للسوابق المتعلقة بالأحكام والبطلان والتسبيب.","https://www.yemenilaw.com/library/judicial-precedents/judicial-rulings/","index"),
      ("السوابق المتعلقة بقواعد الإثبات — Yemenilaw","فهرس موضوعي لقواعد الإثبات والمستندات والشهادة واليمين.","https://www.yemenilaw.com/library/judicial-precedents/evidence-rules/","index"),
      ("السوابق المتعلقة بإجراءات التقاضي — Yemenilaw","فهرس موضوعي لإجراءات الخصومة والتقاضي والحضور والغياب وسقوط الخصومة.","https://www.yemenilaw.com/library/judicial-precedents/litigation-procedures/","index"),
      ("فهرس العقود والصيغ — Yemenilaw","فهرس نماذج العقود والصيغ القانونية؛ يلزم التحقق من شروط إعادة الاستخدام لكل مادة.","https://www.yemenilaw.com/contracts/","index")
    ]
    with closing(db()) as c:
        for title, body, url, kind in seeds:
            exists=c.execute("SELECT id FROM documents WHERE url=? AND title=?",(url,title)).fetchone()
            if not exists:
                c.execute("INSERT INTO documents(title,text,url,kind,source_name) VALUES(?,?,?,?,?)",
                          (title,body,url,kind,"Yemenilaw"))
        c.commit()

@app.get("/api/health")
def health():
    with closing(db()) as c:
        count=c.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
    return {"status":"ok","service":"Mohamed Al-Hajj Legal AI","documents":count,
            "openai_configured":bool(OPENAI_API_KEY)}

@app.get("/api/search")
def search(q: str, limit: int = 8):
    if len(q.strip()) < 2: return {"query":q,"results":[]}
    return {"query":q,"results":search_docs(q, max(1,min(limit,20)))}

@app.post("/api/ingest/text")
def ingest_text(payload: IngestRequest, x_admin_token: str = Header(default="")):
    if not ADMIN_TOKEN or x_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="رمز الإدارة غير صحيح")
    with closing(db()) as c:
        cur=c.execute("""INSERT INTO documents(title,text,url,kind,source_name)
                         VALUES(?,?,?,?,?)""",
                      (payload.title,payload.text,payload.url or "",payload.kind or "legal_source",
                       payload.source_name or ""))
        c.commit()
        doc_id=cur.lastrowid
    return {"status":"stored","document_id":doc_id,"title":payload.title}

@app.get("/api/sync/status")
def sync_status():
    with closing(db()) as c:
        total=c.execute("SELECT COUNT(*) FROM documents WHERE source_url<>'' AND is_active=1").fetchone()[0]
        runs=c.execute("SELECT id,started_at,finished_at,status,pages_seen,pages_saved,errors,detail FROM crawl_runs ORDER BY id DESC LIMIT 5").fetchall() if c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='crawl_runs'").fetchone() else []
    return {"active_indexed_pages":total,"recent_runs":[dict(x) for x in runs]}

@app.get("/api/sync/changes")
def sync_changes(limit: int = 100):
    with closing(db()) as c:
        exists=c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='change_events'").fetchone()
        if not exists: return {"changes":[]}
        rows=c.execute("SELECT change_type,title,url,detected_at,summary FROM change_events ORDER BY id DESC LIMIT ?",(max(1,min(limit,500)),)).fetchall()
    return {"changes":[dict(x) for x in rows]}

@app.post("/api/admin/sync")
def run_sync(x_admin_token: str = Header(default="")):
    if not ADMIN_TOKEN or x_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="رمز الإدارة غير صحيح")
    try:
        from .sync_yemenilaw import crawl, report, DB
        result=crawl(DB, max_pages=int(os.getenv("CRAWL_MAX_PAGES","700")), delay=float(os.getenv("CRAWL_DELAY","1.2")))
        return {"result":result,"report":report(DB,result.get("run_id"))}
    except Exception as e:
        raise HTTPException(status_code=502,detail=f"تعذرت المزامنة: {str(e)[:300]}")

def build_prompt(question, sources):
    context = "\n\n".join(
        f"[المصدر {i+1}] العنوان: {s['title']}\nالرابط: {s['url']}\nالمقتطف: {s['excerpt']}"
        for i,s in enumerate(sources)
    )
    return f"""أنت مساعد قانوني بحثي متخصص في القانون اليمني ضمن منصة محمد الحاج.
أجب بالعربية القانونية الواضحة. استخدم المواد الموجودة في السياق فقط بوصفها مصادر متاحة.
لا تخترع أرقام مواد أو أحكام أو تواريخ أو اقتباسات. إذا لم يتضمن السياق النص القانوني المطلوب، صرّح بأن قاعدة البيانات لا تحتوي حالياً على النص الكافي، واطلب نسخة التشريع أو وجّه إلى المصدر.
ميّز بين النص التشريعي الرسمي والفهرس أو الشرح أو عنوان السابقة. لا تصف القانون بأنه نافذ إلا إذا توفر مصدر رسمي يثبت ذلك.
اذكر المصادر التي استندت إليها في نهاية الإجابة بأسمائها وروابطها.
هذه مساعدة بحثية ولا تستبدل مراجعة المحامي للنصوص الرسمية والملف الأصلي.

المصادر المسترجعة:
{context if context else "لا توجد نتائج مسترجعة من قاعدة البيانات."}

سؤال المستخدم:
{question}
"""

@app.post("/api/ask")
def ask(payload: AskRequest):
    sources=search_docs(payload.question, 8)
    if not OPENAI_API_KEY:
        return {"answer":"محرك البحث المحلي متصل، لكن مفتاح OpenAI API غير مضبوط بعد. أضف OPENAI_API_KEY في ملف .env لتفعيل الإجابة الذكية. نتائج البحث الأولية متاحة في واجهة البحث.","sources":[{"title":s["title"],"url":s["url"]} for s in sources]}
    try:
        from openai import OpenAI
        client=OpenAI(api_key=OPENAI_API_KEY)
        response=client.responses.create(
            model=CHAT_MODEL,
            instructions="أنت مساعد قانوني بحثي متخصص في القانون اليمني. لا تختلق المراجع، وصرح بوضوح عند نقص المصدر.",
            input=build_prompt(payload.question,sources)
        )
        answer=response.output_text or "لم يتم إنشاء إجابة."
        return {"answer":answer,"sources":[{"title":s["title"],"url":s["url"]} for s in sources]}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"تعذر الاتصال بمحرك الذكاء الاصطناعي: {str(e)[:300]}")

@app.get("/")
def home():
    return FileResponse(os.path.join(BASE_DIR,"..","frontend","index.html"))

app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR,"..","frontend")), name="static")
