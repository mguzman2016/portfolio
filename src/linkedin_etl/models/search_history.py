from sqlalchemy import Column, Integer, Date, PrimaryKeyConstraint
from .base import Base

class SearchHistory(Base):
    __tablename__ = "lk_search_history"

    etl_search_id = Column("etl_search_id", Integer)
    search_date = Column("search_date", Date)
    total_jobs = Column("total_jobs", Integer)

    __table_args__ = (
        PrimaryKeyConstraint('etl_search_id', 'search_date'),
    )
