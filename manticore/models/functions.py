# noinspection PyAbstractClass
from django.db.models import Func


class Weight(Func):
    """
    Ranking weight function

    >>> qs.order_by(OrderBy(Weight(), descending=True),)
    """
    function = 'weight'
    arity = 0


# noinspection PyAbstractClass
class Expr(Func):
    """
    Function defining expr ranker

    >>> qs.options(ranker=Expr(Value("sum(hit_count * user_weight)")))
    """
    function = 'expr'
    arity = 1


# noinspection PyAbstractClass
class Export(Func):
    """
    Function defining export ranker

    >>> qs.options(ranker=Export(Value("sum(hit_count * user_weight)")))
    """
    function = 'export'
    arity = 1
