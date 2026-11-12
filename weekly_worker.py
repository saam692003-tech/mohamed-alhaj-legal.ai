import os, logging
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from official_sources import import_official_legislation, index_catalogs

logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')

def run_weekly():
    logging.info('بدء تحديث التشريعات الرسمية وفهارس المصادر القضائية')
    try:
        legislation=import_official_legislation(
            max_pages=int(os.getenv('CRAWL_MAX_PAGES','180')),
            delay=float(os.getenv('CRAWL_DELAY','1.3'))
        )
        catalogs=index_catalogs()
        logging.info('نتيجة تحديث التشريعات: %s',legislation)
        logging.info('نتيجة تحديث الفهارس: %s',catalogs)
    except Exception:
        logging.exception('تعذر إكمال التحديث الأسبوعي')

if __name__=='__main__':
    tz=os.getenv('SCHEDULE_TIMEZONE','Asia/Aden')
    scheduler=BlockingScheduler(timezone=tz)
    scheduler.add_job(run_weekly,CronTrigger(day_of_week='mon',hour=10,minute=0,timezone=tz),id='yemen-legal-weekly',coalesce=True,max_instances=1,misfire_grace_time=3600)
    logging.info('المتابعة الأسبوعية مفعلة: كل اثنين 10:00 (%s)',tz)
    scheduler.start()
