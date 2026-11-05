import os, logging
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from sync_yemenilaw import crawl, report, DB
logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')

def run_weekly():
    logging.info('بدء الفهرسة والمقارنة الأسبوعية لموقع Yemenilaw')
    result=crawl(DB, max_pages=int(os.getenv('CRAWL_MAX_PAGES','700')), delay=float(os.getenv('CRAWL_DELAY','1.2')))
    logging.info('نتيجة الجولة: %s',result)
    logging.info('تقرير التغييرات: %s',report(DB,result.get('run_id')))

if __name__=='__main__':
    tz=os.getenv('SCHEDULE_TIMEZONE','Asia/Aden')
    scheduler=BlockingScheduler(timezone=tz)
    scheduler.add_job(run_weekly,CronTrigger(day_of_week='mon',hour=10,minute=0,timezone=tz),id='yemenilaw-weekly',coalesce=True,max_instances=1,misfire_grace_time=3600)
    logging.info('المتابعة الأسبوعية مفعلة: كل اثنين 10:00 (%s)',tz)
    scheduler.start()
