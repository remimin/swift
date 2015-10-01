NONE = 0
FIRST = 1
SHORT_CIRCUIT = 2
PRE_AUTH = 3
AUTH = 4
POST_AUTH = 5
LAST = 6
APP = 7


class BaseMiddleware(object):
    requires = []
    before = []
    after = []
    group = NONE
