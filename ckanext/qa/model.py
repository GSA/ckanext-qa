import logging
from sqlalchemy import Table, Column, types
import ckan.model as model
import ckan.model.meta  as meta
import ckan.model.domain_object as domain_object

log = logging.getLogger(__name__)

qa_ids_table = None
resource_domain_blacklist_table = None
qa_system_info_table = None

class QaIdsException(Exception):
    pass

class QaIds(domain_object.DomainObject):
    pass

class QaRDB(domain_object.DomainObject):
    pass

class QaRDBException(Exception):
    pass

class QaSystemInfo(domain_object.DomainObject):
    pass

class QaSystemInfoException(Exception):
    pass

def setup():

    # qa_ids table
    if qa_ids_table is None:
        define_apps_tables()
        log.debug('qa_ids table defined in memory')

    if model.repo.are_tables_created():
        if not qa_ids_table.exists():
            qa_ids_table.create()
            log.debug('qa_ids table created')
        else:
            log.debug('qa_ids table already exists')
    else:
        log.debug('qa_ids table creation deferred')

    # resource_domain_blacklist table
    if resource_domain_blacklist_table is None:
        define_apps_tables()
        log.debug('resource_domain_blacklist table defined in memory')

    if model.repo.are_tables_created():
        if not resource_domain_blacklist_table.exists():
            resource_domain_blacklist_table.create()
            log.debug('resource_domain_blacklist table created')
        else:
            log.debug('resource_domain_blacklist table already exists')
    else:
        log.debug('resource_domain_blacklist table creation deferred')

    # qa_system_info table
    if qa_system_info_table is None:
        define_apps_tables()
        log.debug('qa_system_info table defined in memory')

    if model.repo.are_tables_created():
        if not qa_system_info_table.exists():
            qa_system_info_table.create()
            log.debug('qa_system_info table created')
        else:
            log.debug('qa_system_info table already exists')
    else:
        log.debug('qa_system_info table creation deferred')

# qa_ids model definition
qa_ids_table = Table('qa_ids', meta.metadata,
    Column('id', types.Integer(),  primary_key=True),
    Column('pkg_id', types.UnicodeText, nullable=False, default=u''),
    Column('status', types.UnicodeText, nullable=False, default=u'New'),
)

# resource_domain_blacklist model definition
resource_domain_blacklist_table = Table('resource_domain_blacklist', meta.metadata,
    Column('path', types.UnicodeText,  primary_key=True),
    Column('count', types.Integer(), nullable=False, default=u''),
    Column('errno', types.Integer(), nullable=False, default=u''),
)

# qa_system_info model definition
qa_system_info_table = Table('qa_system_info', meta.metadata,
    Column('id', types.Integer(),  primary_key=True),
    Column('key', types.UnicodeText, nullable=False, default=u''),
    Column('value', types.UnicodeText, nullable=False, default=u''),
)

meta.mapper(QaIds, qa_ids_table)
meta.mapper(QaRDB, resource_domain_blacklist_table)
meta.mapper(QaSystemInfo, qa_system_info_table)

