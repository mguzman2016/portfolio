from sqlalchemy import Column, Integer, String, Boolean, Date
from .base import Base

class EtlStatus(Base):
    __tablename__ = "lk_etl_status"

    etl_id = Column("etl_id", Integer, primary_key = True)
    etl_search= Column("etl_search", String(255))
    etl_url = Column("etl_url", String(255))
    is_running = Column("is_running", Boolean)
    last_updated = Column("last_updated", Date)
    country = Column("country", String(255))
    city = Column("city", String(255)) 

    def __str__(self):
        return '-'.join([
            "<ETLStatusClass>",
            str(self.etl_id), 
            self.etl_search, 
            self.etl_url, 
            str(self.is_running), 
            str(self.last_updated),
            "</ETLStatusClass>"
            ])