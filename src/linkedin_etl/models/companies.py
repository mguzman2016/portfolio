from sqlalchemy import Column, Integer, String, BigInteger, TEXT
from .base import Base

class Companies(Base):
    __tablename__ = "lk_companies"

    company_id = Column("company_id", BigInteger, primary_key = True)
    company_name = Column("company_name", String(255))
    company_image_url = Column("company_image_url", TEXT)
    company_description = Column("company_description", TEXT)
    company_staff_count = Column("company_staff_count", Integer)
    company_url = Column("company_url", String(1000))
    company_follower_count = Column("company_follower_count", String(255))
    company_industries = Column("company_industries",TEXT)