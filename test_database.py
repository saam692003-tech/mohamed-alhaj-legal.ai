import os, sqlite3, tempfile, unittest
from backend.sync_yemenilaw import setup

class DatabaseEngineTest(unittest.TestCase):
    def test_schema_and_full_text_index(self):
        fd, path = tempfile.mkstemp(suffix='.db'); os.close(fd)
        try:
            c=sqlite3.connect(path)
            setup(c)
            c.execute("INSERT INTO documents(title,text,url,kind,source_name,source_url,content_hash,fetched_at,verification_status) VALUES(?,?,?,?,?,?,?,?,?)",
                      ('قانون تجريبي','أحكام الالتزامات المدنية والتجارية','https://www.yemenilaw.com/library/yemeni-laws/test/','law_page','Yemenilaw','https://www.yemenilaw.com/library/yemeni-laws/test/','hash-1','2026-11-05T00:00:00+00:00','غير متحقق'))
            c.commit()
            self.assertEqual(c.execute("SELECT COUNT(*) FROM documents_fts WHERE documents_fts MATCH 'الالتزامات'").fetchone()[0],1)
            for table in ('crawl_runs','document_snapshots','change_events'):
                self.assertEqual(c.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",(table,)).fetchone()[0],1)
            c.close()
        finally:
            if os.path.exists(path): os.remove(path)

if __name__=='__main__': unittest.main()
