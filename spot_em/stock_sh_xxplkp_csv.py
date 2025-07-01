# 把获取的沪市考评文件 读取到csv后 这里读取并 保存倒数数据库

import pandas as pd
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects.mysql import insert
from zvtm import init_log, zvt_config
# Initialize logging
logger = logging.getLogger(__name__)
sched = BackgroundScheduler()

# Database configuration - 替换为您的实际配置
DATABASE_URI = (
    f"mysql+pymysql://{zvt_config['mysql_user']}:{zvt_config['mysql_password']}"
    f"@{zvt_config['mysql_host']}:{zvt_config['mysql_port']}/finance"
)
# Define SQLAlchemy Base
Base = declarative_base()


class CompanyEvaluation(Base):
    __tablename__ = 'shse_company_evaluation'  # 修改表名为沪市公司评价

    id = Column(String(128), primary_key=True)  # Unique ID: company_code + evaluation_year
    gsdm = Column(String(20))  # 公司代码
    gsjc = Column(String(50))  # 公司简称
    kpjg = Column(String(50))  # 考评结果
    kpnd = Column(Integer)  # 考评年度

    def __repr__(self):
        return f"<CompanyEvaluation(id={self.id}, gsdm='{self.gsdm}', gsjc='{self.gsjc}')>"


@sched.scheduled_job('cron', day_of_week='mon-fri', hour=20, minute=0)
def record_shse_evaluation_data():
    """Fetch and save SHSE evaluation data to database, only inserting new records."""
    engine = create_engine(DATABASE_URI, echo=True)  # 开启echo便于调试
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # 设置评价年度为2023（根据您的PDF标题）
    evaluation_year = 2023

    try:
        # 读取CSV文件
        file_path = "disclosure_evaluation_results.csv"  # 替换为实际文件路径
        df = pd.read_csv(file_path)
        logger.info(f"成功读取 {len(df)} 条记录")

        # 重命名列以匹配数据库模型
        df = df.rename(columns={
            '证券代码': 'gsdm',
            '证券简称': 'gsjc',
            '评价结果': 'kpjg'
        })

        # 添加评价年度列
        df['kpnd'] = evaluation_year

        # 生成唯一ID（公司代码 + 评价年度）
        df['id'] = df['gsdm'].astype(str) + '_' + df['kpnd'].astype(str)

        # 获取现有ID
        existing_ids = pd.read_sql("SELECT id FROM shse_company_evaluation", engine)['id'].tolist()
        new_records = df[~df['id'].isin(existing_ids)]

        if new_records.empty:
            logger.info("没有新记录需要插入")
            return True

        # 准备插入的数据
        records_to_insert = new_records[['id', 'gsdm', 'gsjc', 'kpjg', 'kpnd']].to_dict('records')
        logger.info(f"准备插入 {len(records_to_insert)} 条新记录")

        # 批量插入（使用ON DUPLICATE KEY UPDATE处理重复）
        stmt = insert(CompanyEvaluation).values(records_to_insert)
        stmt = stmt.on_duplicate_key_update(
            gsdm=stmt.inserted.gsdm,
            gsjc=stmt.inserted.gsjc,
            kpjg=stmt.inserted.kpjg,
            kpnd=stmt.inserted.kpnd
        )

        # 执行插入
        session.execute(stmt)
        session.commit()

        logger.info(f"成功插入/更新 {len(records_to_insert)} 条记录")
        return True

    except Exception as e:
        logger.exception(f"处理SHSE评价数据时出错: {str(e)}")
        session.rollback()
        return False
    finally:
        session.close()


if __name__ == "__main__":
    # 初始化日志
    init_log("shse_evaluation.log")

    # 启动调度器
    sched.start()

    # 立即执行一次（用于测试）
    record_shse_evaluation_data()

    # 保持程序运行（在生产环境中使用）
    try:
        while True:
            pass
    except (KeyboardInterrupt, SystemExit):
        sched.shutdown()