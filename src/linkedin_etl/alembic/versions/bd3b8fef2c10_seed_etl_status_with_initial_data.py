"""seed etl status with initial data

Revision ID: bd3b8fef2c10
Revises: 7cd9ccf59c1f
Create Date: 2025-09-09 22:16:37.133677

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'bd3b8fef2c10'
down_revision: Union[str, Sequence[str], None] = '7cd9ccf59c1f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.bulk_insert(
        sa.table(
            'lk_etl_status',
            sa.column('etl_id', sa.Integer),
            sa.column('etl_url', sa.String),
            sa.column('etl_search', sa.String),
            sa.column('is_running', sa.Boolean),
            sa.column('last_updated', sa.Date)
        ),
        [
            {'etl_id': 1, 'etl_search': 'Data engineer - Zurich', 'etl_url': 'https://www.linkedin.com/voyager/api/voyagerJobsDashJobCards?decorationId=com.linkedin.voyager.dash.deco.jobs.search.JobSearchCardsCollection-220&count=50&q=jobSearch&query=(currentJobId:4301047908,origin:JOB_SEARCH_PAGE_LOCATION_AUTOCOMPLETE,keywords:Data%20Engineer,locationUnion:(geoId:102436504),spellCorrectionEnabled:true)&start=0', 'is_running': False, 'last_updated': '2020-01-01'},
            {'etl_id': 2, 'etl_search': 'Data engineer - Basel',  'etl_url': 'https://www.linkedin.com/voyager/api/voyagerJobsDashJobCards?decorationId=com.linkedin.voyager.dash.deco.jobs.search.JobSearchCardsCollection-220&count=50&q=jobSearch&query=(origin:JOB_SEARCH_PAGE_LOCATION_AUTOCOMPLETE,keywords:Data%20Engineer,locationUnion:(geoId:100100349),spellCorrectionEnabled:true)&start=0', 'is_running': False, 'last_updated': '2020-01-01'},
            {'etl_id': 3, 'etl_search': 'Data engineer - Geneva', 'etl_url': 'https://www.linkedin.com/voyager/api/voyagerJobsDashJobCards?decorationId=com.linkedin.voyager.dash.deco.jobs.search.JobSearchCardsCollection-220&count=50&q=jobSearch&query=(origin:JOB_SEARCH_PAGE_LOCATION_AUTOCOMPLETE,keywords:Data%20Engineer,locationUnion:(geoId:104406358),spellCorrectionEnabled:true)&start=0', 'is_running': False, 'last_updated': '2020-01-01'},
            {'etl_id': 4, 'etl_search': 'Data engineer - Bern',   'etl_url': 'https://www.linkedin.com/voyager/api/voyagerJobsDashJobCards?decorationId=com.linkedin.voyager.dash.deco.jobs.search.JobSearchCardsCollection-220&count=50&q=jobSearch&query=(origin:JOB_SEARCH_PAGE_LOCATION_AUTOCOMPLETE,keywords:Data%20Engineer,locationUnion:(geoId:105825954),spellCorrectionEnabled:true)&start=0', 'is_running': False, 'last_updated': '2020-01-01'}
        ]
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.execute(
        "DELETE FROM lk_etl_status WHERE etl_id IN (1, 2, 3,)"
    )
