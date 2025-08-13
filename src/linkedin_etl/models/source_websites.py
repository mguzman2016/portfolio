from sqlalchemy import Column, Integer, String
from .base import Base

class SourceWebsites(Base):
    __tablename__ = "lk_source_websites"

    source_website_id = Column(Integer, primary_key=True)
    source_website_name = Column(String)
