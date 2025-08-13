from sqlalchemy import Column, Integer, String, DECIMAL, TEXT, BigInteger, DateTime
from sqlalchemy.sql import func
from .base import Base

class StagingJobDetails(Base):
    __tablename__ = "lk_staging_job_details"

    job_id = Column("job_id", BigInteger, primary_key = True)
    job_name = Column("job_name", String(1000))
    standardized_name = Column("standardized_name", String(255))
    job_url = Column("job_url", String(1000))
    job_description = Column("job_description", TEXT)
    job_type = Column("job_type", String(255))
    job_min_salary = Column("job_min_salary", DECIMAL(10,2))
    job_max_salary = Column("job_max_salary", DECIMAL(10,2))
    job_pay_period = Column("job_pay_period", String(255))
    job_functions = Column("job_functions", TEXT)
    job_experience_level = Column("job_experience_level", String(255))
    job_views = Column("job_views",Integer)
    company_id = Column("company_id", Integer)
    