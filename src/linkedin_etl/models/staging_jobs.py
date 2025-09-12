from sqlalchemy import Column, BigInteger, Integer, PrimaryKeyConstraint
from .base import Base

class StagingJobs(Base):
    __tablename__ = "lk_staging_jobs"

    etl_id = Column(BigInteger)
    source_id = Column(Integer)
    
    __table_args__ = (
        PrimaryKeyConstraint('etl_id', 'source_id'),
    )
