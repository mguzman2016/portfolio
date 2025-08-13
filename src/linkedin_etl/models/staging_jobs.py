from sqlalchemy import Column, BigInteger, Integer, PrimaryKeyConstraint
from .base import Base

class StagingJobs(Base):
    __tablename__ = "lk_staging_jobs"

    job_id = Column(BigInteger)
    etl_id = Column(Integer)
    
    __table_args__ = (
        PrimaryKeyConstraint('job_id', 'etl_id'),
    )
