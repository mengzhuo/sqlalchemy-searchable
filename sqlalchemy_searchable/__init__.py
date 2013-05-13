import re

from sqlalchemy import event
from sqlalchemy.schema import DDL
from sqlalchemy.orm.mapper import Mapper


def safe_search_terms(query, wildcard=':*'):
    # Remove all illegal characters from the search query. Also remove multiple
    # spaces.
    query = re.sub(r'[():|&!*@#\s]+', ' ', query).strip()
    if not query:
        return []

    # Split the search query into terms.
    terms = query.split(' ')

    # Search for words starting with the given search terms.
    return map(lambda a: a + wildcard, terms)


class SearchQueryMixin(object):
    def search_filter(self, term, tablename=None, language=None):
        return search_filter(self, term, tablename, language)

    def search(self, search_query, tablename=None, language=None):
        """
        Search given query with full text search.

        :param search_query: the search query
        :param tablename: custom tablename
        :param language: language to be passed to to_tsquery
        """
        if not search_query:
            return self

        terms = safe_search_terms(search_query)
        if not terms:
            return self

        return (
            self.filter(
                self.search_filter(search_query, tablename, language)
            )
            .params(term=u' & '.join(terms))
        )


def search_filter(query, term, tablename=None, language=None):
    if not tablename:
        mapper = query._entities[0].entity_zero
        entity = mapper.class_

        try:
            tablename = entity.__search_options__['tablename']
        except AttributeError:
            tablename = entity._inspect_searchable_tablename()
        except KeyError:
            tablename = entity._inspect_searchable_tablename()

    if not language:
        return '%s.search_vector @@ to_tsquery(:term)' % (
            quote_identifier(tablename)
        )
    else:
        return "%s.search_vector @@ to_tsquery('%s', :term)" % (
            quote_identifier(tablename), language
        )


def search(query, search_query, tablename=None, language=None):
    """
    Search given query with full text search.

    :param search_query: the search query
    :param tablename: custom tablename
    :param language: language to be passed to to_tsquery
    """
    if not search_query:
        return query

    terms = safe_search_terms(search_query)
    if not terms:
        return query

    if hasattr(query, 'search_filter'):
        query = query.filter(
            query.search_filter(search_query, tablename, language)
        )
    else:
        query = query.filter(
            search_filter(query, search_query, tablename, language)
        )
    return query.params(term=' & '.join(terms))


def quote_identifier(identifier):
    """Adds double quotes to given identifier. Since PostgreSQL is the only
    supported dialect we don't need dialect specific stuff here"""
    return '"%s"' % identifier


def attach_search_indexes(mapper, class_):
    if issubclass(class_, Searchable):
        class_.__search_args_init__()
        class_.define_search_vector()


# attach to all mappers
event.listen(Mapper, 'instrument_class', attach_search_indexes)


class Searchable(object):

    __searchable_columns__ = []

    __search_vector_name__ = 'search_vector'   # pg
    __search_catalog__ = 'pg_catalog.english'  # pg
    __search_modifier__ = 'IN BOOLEAN MODE'    # mysql
    
    @classmethod
    def __search_args_init__(cls):
        if not hasattr(cls, '__search_trigger_name__'):
            cls.__search_trigger_name__ = '{.__tablename__}_search_update'.format(cls)

        if not hasattr(cls, '__search_index_name__'):
            cls.__search_index_name__ = '{.__tablename__}_search_index'.format(cls)

        return cls

    @classmethod
    def _inspect_searchable_tablename(cls):
        """
        Recursive method that returns the name of the searchable table. This is
        method is needed for the inspection of tablenames in certain
        inheritance scenarios such as joined table inheritance where only
        parent is defined is searchable.
        """
        if Searchable in cls.__bases__:
            return cls.__tablename__

        for class_ in cls.__bases__:
            return class_._inspect_searchable_tablename()

    @classmethod
    def __make_ddls(cls):
        return [
            # PostgreSQL
            DDL("""
                ALTER TABLE {0.__tablename__}
                ADD COLUMN {0.__search_vector_name__} tsvector
                """
                .format(cls)).execute_if(dialect='postgresql'),

            DDL("""
                CREATE INDEX {0.__search_index_name__} 
                ON {0.__tablename__}
                USING gin({0.__search_vector_name__})
                """
                .format(cls)).execute_if(dialect='postgresql'),
            DDL("""
                CREATE TRIGGER {0.__search_trigger_name__}
                BEFORE UPDATE OR INSERT ON {0.__tablename__}
                FOR EACH ROW EXECUTE PROCEDURE
                tsvector_update_trigger({1})
                """
                .format(cls,
                        ', '.join([cls.__search_vector_name__,
                               "'%s'" % cls.__search_catalog__] + 
                                      cls.__searchable_columns__ ))
                ).execute_if(dialect='postgresql'),
            # MySQL
            DDL("""
                ALTER TABLE {0.__tablename__}
                ADD NEW FULLTEXT({1})
                """
                .format(cls,
                        ", ".join(cls.__searchable_columns__))
                ).execute_if(dialect='mysql')
        ]

    @classmethod
    def define_search_vector(cls):
        # In order to support joined table inheritance we need to ensure that
        # this class directly inherits Searchable.
        if Searchable not in cls.__bases__:
            return

        if not cls.__searchable_columns__:
            raise Exception(
                "No searchable columns defined for model {.__name__}".format(cls))

        # add DDL list into database
        for ddl in cls.__make_ddls():
            event.listen(cls.__table__,
                         'after_create',
                         ddl)
