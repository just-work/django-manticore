from django.db.models import query


class SearchQuerySet(query.QuerySet):

    def _filter_or_exclude(self, negate, *args, **kwargs):
        # some sql expressions in where clause are not supported correcly

        return super()._filter_or_exclude(negate, *args, **kwargs)