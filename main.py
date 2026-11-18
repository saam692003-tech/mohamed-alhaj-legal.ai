import os, re, math, threading, io
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from contextlib import closing
import psycopg
from psycopg.rows import dict_row
from typing import Optional
from fastapi import FastAPI, HTTPException, Header, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, HTMLResponse, Response
from html import escape
from pydantic import BaseModel, Field
from dotenv import load_dotenv

load_dotenv()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_URL = os.getenv("DATABASE_URL", "")
DB_PATH = os.getenv("DATABASE_PATH", os.path.join(BASE_DIR, "legal_library.db"))
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
CHAT_MODEL = os.getenv("OPENAI_CHAT_MODEL", "gpt-4.1-mini")
EMBED_MODEL = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")

app = FastAPI(title="Mohamed Al-Hajj Legal AI API", version="0.4.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=False,
                   allow_methods=["*"], allow_headers=["*"])

def db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL غير مضبوط في إعدادات الخدمة")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row, connect_timeout=10)

def init_db():
    with closing(db()) as c:
        with c.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS documents(
                id BIGSERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                text TEXT NOT NULL,
                url TEXT DEFAULT '',
                kind TEXT DEFAULT 'legal_source',
                source_name TEXT DEFAULT '',
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                source_url TEXT DEFAULT '',
                content_hash TEXT DEFAULT '',
                fetched_at TEXT DEFAULT '',
                verification_status TEXT DEFAULT 'غير متحقق',
                is_active INTEGER DEFAULT 1
            )
            """)
            cur.execute("ALTER TABLE documents ADD COLUMN IF NOT EXISTS pdf_bytes BYTEA")
            cur.execute("ALTER TABLE documents ADD COLUMN IF NOT EXISTS content_type TEXT DEFAULT 'text/html'")
            cur.execute("CREATE INDEX IF NOT EXISTS ix_documents_source_url ON documents(source_url)")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS case_files(
                id BIGSERIAL PRIMARY KEY, filename TEXT NOT NULL,
                content_type TEXT DEFAULT '', extracted_text TEXT DEFAULT '',
                analysis TEXT DEFAULT '', created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            )
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS crawl_runs(
                id BIGSERIAL PRIMARY KEY, started_at TEXT NOT NULL, finished_at TEXT,
                status TEXT NOT NULL, pages_seen INTEGER DEFAULT 0, pages_saved INTEGER DEFAULT 0,
                errors INTEGER DEFAULT 0, detail TEXT DEFAULT ''
            )
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS change_events(
                id BIGSERIAL PRIMARY KEY, run_id BIGINT, document_id BIGINT,
                change_type TEXT NOT NULL, title TEXT NOT NULL, url TEXT NOT NULL,
                detected_at TEXT NOT NULL, old_hash TEXT DEFAULT '', new_hash TEXT DEFAULT '',
                summary TEXT DEFAULT ''
            )
            """)
        c.commit()

class AskRequest(BaseModel):
    question: str = Field(min_length=3, max_length=8000)

class DraftRequest(BaseModel):
    document_type: str = Field(min_length=2, max_length=100)
    court: Optional[str] = ""
    applicant: Optional[str] = ""
    opponent: Optional[str] = ""
    case_number: Optional[str] = ""
    facts: str = Field(min_length=20, max_length=20000)
    requests: Optional[str] = ""
    defenses: Optional[str] = ""

class IngestRequest(BaseModel):
    title: str = Field(min_length=2, max_length=500)
    text: str = Field(min_length=20)
    url: Optional[str] = ""
    kind: Optional[str] = "legal_source"
    source_name: Optional[str] = ""

def search_docs(query: str, limit: int = 8):
    terms = re.findall(r"[\w\u0600-\u06FF]+", query)
    if not terms:
        return []
    clauses = " OR ".join(["(title ILIKE %s OR text ILIKE %s)" for _ in terms[:12]])
    params=[]
    for term in terms[:12]:
        pattern=f"%{term}%"
        params.extend([pattern,pattern])
    params.append(limit)
    with closing(db()) as c:
        with c.cursor() as cur:
            cur.execute(f"""SELECT id,title,text,url,kind,source_name
                            FROM documents WHERE COALESCE(is_active,1)=1 AND ({clauses})
                            ORDER BY id DESC LIMIT %s""", params)
            rows=cur.fetchall()
    return [{"id":r["id"],"title":r["title"],"excerpt":r["text"][:900],
             "url":r["url"],"kind":r["kind"],"source_name":r["source_name"]} for r in rows]

@app.on_event("startup")
def startup():
    init_db()
    seed_sources()
    scheduler = BackgroundScheduler(timezone=os.getenv("SCHEDULE_TIMEZONE","Asia/Aden"))
    scheduler.add_job(run_weekly_import, CronTrigger(day_of_week="mon",hour=10,minute=0),
                      id="yemen-legal-weekly", replace_existing=True, max_instances=1)
    scheduler.start()
    app.state.legal_scheduler = scheduler
    threading.Thread(target=initial_source_import, daemon=True).start()

def run_weekly_import():
    try:
        from official_sources import crawl_all
        result = crawl_all(max_pages=int(os.getenv("CRAWL_MAX_PAGES","1200")),
                           delay=float(os.getenv("CRAWL_DELAY","1.0")))
        print(f"Weekly legal source refresh: {result}")
    except Exception as exc:
        print(f"Weekly legal source refresh failed: {exc}")

def initial_source_import():
    try:
        with closing(db()) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS n FROM documents WHERE source_url<>'' AND is_active=1")
                done=cur.fetchone()["n"]
        if done:
            return
        from official_sources import crawl_all
        crawl_all(max_pages=int(os.getenv("CRAWL_MAX_PAGES","1200")), delay=float(os.getenv("CRAWL_DELAY","1.0")))
    except Exception as exc:
        print(f"Initial legal source import failed: {exc}")

def seed_sources():
    seeds = [
      ("فهرس القوانين اليمنية — Yemenilaw","فهرس يتضمن روابط صفحات القوانين اليمنية؛ يجب فتح كل قانون والتحقق من نصه وتعديلاته وحالة نفاذه.","https://www.yemenilaw.com/library/yemeni-laws/","index"),
      ("فهرس السوابق القضائية اليمنية — Yemenilaw","فهرس للسوابق القضائية مرتبة حسب الإصدارات والتصنيفات الموضوعية.","https://www.yemenilaw.com/library/judicial-precedents/","index"),
      ("فهرس العقود والصيغ — Yemenilaw","فهرس نماذج العقود والصيغ القانونية.","https://www.yemenilaw.com/contracts/","index")
    ]
    with closing(db()) as c:
        with c.cursor() as cur:
            for title, body, url, kind in seeds:
                cur.execute("SELECT id FROM documents WHERE url=%s AND title=%s",(url,title))
                if not cur.fetchone():
                    cur.execute("INSERT INTO documents(title,text,url,kind,source_name) VALUES(%s,%s,%s,%s,%s)",
                                (title,body,url,kind,"Yemenilaw"))
        c.commit()

@app.get("/api/health")
def health():
    with closing(db()) as c:
        with c.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM documents")
            count=cur.fetchone()["n"]
            cur.execute("SELECT current_database() AS db")
            dbname=cur.fetchone()["db"]
    return {"status":"ok","service":"Mohamed Al-Hajj Legal AI","documents":count,
            "database":"postgresql","database_name":dbname,"openai_configured":bool(OPENAI_API_KEY)}

@app.get("/api/search")
def search(q: str, limit: int = 8):
    if len(q.strip()) < 2: return {"query":q,"results":[]}
    return {"query":q,"results":search_docs(q, max(1,min(limit,20)))}


@app.post("/api/cases/analyze")
async def analyze_case_files(files: list[UploadFile] = File(...)):
    if not files:
        raise HTTPException(status_code=400, detail="اختر ملفاً واحداً على الأقل")
    extracted=[]
    for upload in files[:10]:
        name=(upload.filename or "document").lower()
        raw=await upload.read()
        if len(raw)>15*1024*1024:
            raise HTTPException(status_code=413, detail=f"حجم الملف {upload.filename} يتجاوز 15 ميغابايت")
        try:
            if name.endswith(".pdf") or upload.content_type=="application/pdf":
                from pypdf import PdfReader
                reader=PdfReader(io.BytesIO(raw))
                text="\n".join(page.extract_text() or "" for page in reader.pages).strip()
            elif name.endswith(".docx"):
                from docx import Document
                doc=Document(io.BytesIO(raw))
                text="\n".join(p.text for p in doc.paragraphs).strip()
                for table in doc.tables:
                    text += "\n" + "\n".join(" | ".join(cell.text for cell in row.cells) for row in table.rows)
            elif name.endswith(".txt") or (upload.content_type or "").startswith("text/"):
                text=raw.decode("utf-8-sig",errors="replace")
            else:
                raise HTTPException(status_code=415,detail=f"صيغة غير مدعومة: {upload.filename}. الصيغ المدعومة PDF وDOCX وTXT")
            if not text.strip():
                raise HTTPException(status_code=422,detail=f"تعذر استخراج النص من {upload.filename}. قد يكون PDF ممسوحاً ضوئياً ويحتاج OCR.")
            extracted.append({"filename":upload.filename or "document","text":text[:60000]})
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=422,detail=f"تعذر قراءة {upload.filename}: {str(exc)[:180]}")
    combined="\n\n".join(f"المستند: {x['filename']}\n{x['text']}" for x in extracted)
    analysis="تم استخراج النصوص وحفظها. يلزم ضبط مفتاح OpenAI لتفعيل التحليل الآلي."
    if OPENAI_API_KEY:
        try:
            from openai import OpenAI
            refs=search_docs(combined[:6000],8)
            context="\n\n".join(f"{x['title']} | {x['url']}\n{x['excerpt']}" for x in refs)
            client=OpenAI(api_key=OPENAI_API_KEY)
            response=client.responses.create(
                model=CHAT_MODEL,
                instructions="أنت محلل قانوني يمني. حلل المستندات المقدمة فقط، افصل الوقائع والطلبات والأدلة والتواريخ والتناقضات والدفوع المحتملة. لا تختلق مواد قانونية أو وقائع. حدد ما يحتاج إلى مراجعة.",
                input=f"نصوص المستندات:\n{combined[:90000]}\n\nمراجع المكتبة:\n{context}\n\nأعد تحليلاً منظماً مع بيان النواقص."
            )
            analysis=response.output_text or analysis
        except Exception as exc:
            analysis=f"تم استخراج النصوص وحفظها، لكن تعذر تشغيل التحليل الذكي: {str(exc)[:250]}"
    ids=[]
    with closing(db()) as conn:
        with conn.cursor() as cur:
            for item in extracted:
                cur.execute("INSERT INTO case_files(filename,extracted_text,analysis) VALUES(%s,%s,%s) RETURNING id",
                    (item["filename"],item["text"],analysis))
                ids.append(cur.fetchone()["id"])
        conn.commit()
    return {"status":"processed","files":[{"id":i,"filename":x["filename"]} for i,x in zip(ids,extracted)],"analysis":analysis}

@app.get("/api/cases")
def list_case_files():
    with closing(db()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id,filename,created_at FROM case_files ORDER BY id DESC LIMIT 100")
            rows=cur.fetchall()
    return {"items":rows,"total":len(rows)}

@app.post("/api/ingest/text")
def ingest_text(payload: IngestRequest, x_admin_token: str = Header(default="")):
    if not ADMIN_TOKEN or x_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="رمز الإدارة غير صحيح")
    with closing(db()) as c:
        with c.cursor() as cur:
            cur.execute("""INSERT INTO documents(title,text,url,kind,source_name)
                           VALUES(%s,%s,%s,%s,%s) RETURNING id""",
                        (payload.title,payload.text,payload.url or "",payload.kind or "legal_source",payload.source_name or ""))
            doc_id=cur.fetchone()["id"]
        c.commit()
    return {"status":"stored","document_id":doc_id,"title":payload.title}

@app.get("/api/sync/status")
def sync_status():
    with closing(db()) as c:
        with c.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM documents WHERE source_url<>'' AND is_active=1")
            total=cur.fetchone()["n"]
            cur.execute("SELECT id,started_at,finished_at,status,pages_seen,pages_saved,errors,detail FROM crawl_runs ORDER BY id DESC LIMIT 5")
            runs=cur.fetchall()
    return {"active_indexed_pages":total,"recent_runs":runs}

@app.get("/api/sync/changes")
def sync_changes(limit: int = 100):
    with closing(db()) as c:
        with c.cursor() as cur:
            cur.execute("SELECT change_type,title,url,detected_at,summary FROM change_events ORDER BY id DESC LIMIT %s",(max(1,min(limit,500)),))
            rows=cur.fetchall()
    return {"changes":rows}

@app.post("/api/admin/sync")
def run_sync(x_admin_token: str = Header(default="")):
    if not ADMIN_TOKEN or x_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="رمز الإدارة غير صحيح")
    try:
        from official_sources import crawl_all
        return crawl_all(max_pages=int(os.getenv("CRAWL_MAX_PAGES","1200")), delay=float(os.getenv("CRAWL_DELAY","1.0")))
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


@app.post("/api/draft")
def create_legal_draft(payload: DraftRequest):
    if not OPENAI_API_KEY:
        raise HTTPException(status_code=503, detail="محرك الصياغة لم يُفعّل بعد: يلزم ضبط OPENAI_API_KEY في إعدادات خدمة الخادم.")
    query = " ".join([payload.document_type, payload.facts[:3000], payload.defenses or ""])
    sources = search_docs(query, 10)
    source_context = "\n\n".join(
        f"[{i+1}] {s['title']} | {s['url']}\n{s['excerpt']}" for i,s in enumerate(sources)
    )
    prompt = f"""أعد مسودة {payload.document_type} وفق القانون اليمني وبصياغة قانونية عربية مهنية.
بيانات المستند:
المحكمة أو الجهة: {payload.court or 'غير محددة'}
مقدم الطلب: {payload.applicant or 'غير محدد'}
الخصم: {payload.opponent or 'غير محدد'}
رقم القضية: {payload.case_number or 'غير محدد'}
الوقائع: {payload.facts}
الطلبات: {payload.requests or 'تُستخلص من الوقائع عند إمكان ذلك، وإلا اذكر أنها تحتاج استكمالاً'}
الدفوع المطلوب بحثها: {payload.defenses or 'استخرج الدفوع المحتملة من الوقائع دون افتراض وقائع غير مذكورة'}
المراجع المسترجعة من مكتبة المنصة:
{source_context or 'لا توجد مراجع مسترجعة.'}
التزم بما يلي:
1. لا تختلق أرقام مواد أو نصوص قوانين أو أحكاماً قضائية أو تواريخ.
2. لا تنسب إلى مصدر معلومة لا يتضمنها.
3. إذا لم تتوفر مادة قانونية محددة، ضع عبارة [يلزم التحقق من النص ورقم المادة] بدلاً من اختلاقها.
4. افصل الوقائع عن التكييف القانوني والدفوع والطلبات.
5. أبرز أوجه النقص أو التناقض في الوقائع في فقرة مستقلة.
6. أخرج مسودة كاملة قابلة للتحرير، مع ترك البيانات الناقصة بين أقواس مربعة.
7. اختم بقائمة المرفقات والمصادر المتاحة.
"""
    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)
        response = client.responses.create(
            model=CHAT_MODEL,
            instructions="أنت محامٍ وباحث قانوني متخصص في التشريعات اليمنية. الدقة مقدمة على الإطالة، ولا تختلق أي مرجع.",
            input=prompt
        )
        return {"draft": response.output_text or "لم يتم إنشاء مسودة.", "sources":[{"title":s["title"],"url":s["url"]} for s in sources]}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"تعذر تشغيل محرك الصياغة: {str(e)[:300]}")

@app.get("/api/library")
def library_list(q: str = "", kind: str = "", limit: int = 100, offset: int = 0):
    limit=max(1,min(limit,200)); offset=max(0,offset)
    filters=["COALESCE(is_active,1)=1"]
    params=[]
    if kind:
        filters.append("kind=%s"); params.append(kind)
    if q.strip():
        filters.append("(title ILIKE %s OR text ILIKE %s)")
        params.extend([f"%{q.strip()}%",f"%{q.strip()}%"])
    where=" AND ".join(filters)
    with closing(db()) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS n FROM documents WHERE {where}",params)
            total=cur.fetchone()["n"]
            cur.execute(f"""SELECT id,title,url,kind,source_name,verification_status,
                            source_url,fetched_at,LEFT(text,500) AS excerpt,(pdf_bytes IS NOT NULL) AS has_pdf
                            FROM documents WHERE {where}
                            ORDER BY id DESC LIMIT %s OFFSET %s""",params+[limit,offset])
            rows=cur.fetchall()
    return {"total":total,"limit":limit,"offset":offset,"items":rows}


@app.get("/api/library/{document_id}/download")
def download_document(document_id: int):
    with closing(db()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT title,pdf_bytes FROM documents WHERE id=%s AND COALESCE(is_active,1)=1",(document_id,))
            row=cur.fetchone()
    if not row: raise HTTPException(status_code=404,detail="المستند غير موجود")
    if not row["pdf_bytes"]: raise HTTPException(status_code=404,detail="لا توجد نسخة PDF محفوظة لهذا المستند")
    filename=re.sub(r"[^\\w\\u0600-\\u06FF.-]+","_",row["title"] or "legal-document")[:100]+".pdf"
    return Response(content=bytes(row["pdf_bytes"]),media_type="application/pdf",headers={"Content-Disposition":f'inline; filename="{filename}"'})

@app.get("/api/library/stats")
def library_stats():
    with closing(db()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT kind,COUNT(*) AS n FROM documents WHERE COALESCE(is_active,1)=1 GROUP BY kind ORDER BY kind")
            kinds=cur.fetchall()
            cur.execute("SELECT COUNT(*) AS n FROM documents WHERE COALESCE(is_active,1)=1")
            total=cur.fetchone()["n"]
    return {"total":total,"by_kind":kinds}

@app.get("/library/{document_id}/print", response_class=HTMLResponse)
def print_document(document_id: int):
    with closing(db()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM documents WHERE id=%s AND COALESCE(is_active,1)=1",(document_id,))
            row=cur.fetchone()
    if not row: raise HTTPException(status_code=404,detail="المستند غير موجود")
    title=escape(row["title"] or "")
    body=escape(row["text"] or "").replace("\n","<br>")
    source=escape(row["source_name"] or "")
    url=escape(row["source_url"] or row["url"] or "", quote=True)
    status=escape(row["verification_status"] or "")
    html=f"""<!doctype html><html lang="ar" dir="rtl"><meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <title>{title}</title><style>
    body{{font-family:Tahoma,Arial,sans-serif;direction:rtl;line-height:2;color:#17212b;margin:32px}}
    header{{border-bottom:2px solid #9b7b43;padding-bottom:14px;margin-bottom:24px}}
    h1{{font-size:23px}}.meta{{color:#555;font-size:13px}}main{{white-space:normal;font-size:16px}}
    a{{color:#174a72;word-break:break-all}}button{{padding:10px 18px;background:#174a72;color:#fff;border:0;border-radius:6px}}
    @media print{{button{{display:none}}body{{margin:16mm}}}}
    </style><header><button onclick="window.print()">طباعة / حفظ بصيغة PDF</button>
    <h1>{title}</h1><div class="meta">المصدر: {source} | تصنيف السجل: {escape(row["kind"] or "")}</div>
    <div class="meta">حالة التحقق: {status}</div></header><main>{body}</main>
    <hr><p class="meta">رابط المصدر الأصلي: <a href="{url}">{url}</a></p>
    <p class="meta">منصة محمد الحاج القانونية — نسخة قراءة مولدة من البيانات المفهرسة، وليست بديلاً عن مراجعة النسخة الرسمية.</p>
    </html>"""
    return HTMLResponse(html)

@app.get("/")
def home():
    return FileResponse(os.path.join(BASE_DIR,"index.html"))


