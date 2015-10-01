# Middleware Groups:
# NONE
#   default, even for a middleware that doesn't inherit BaseMiddleware.
#   Middleware in this group will be left alone.
# FIRST
#   Middleware what should appear first (e.g gatekeeper and catch_errors)
# SHORT_CIRCUIT
#   Middleware that respond to the user without hitting the app, and we want
#   minimal processing when it does.
# PRE_AUTH
#   Middleware what should appear before auth (e.g tempurl, formpost)
# AUTH
#   Authentication middleware.
# POST_AUTH
#   Middle that should sit post AUTH.
# LAST
#   Middleware that should appear at the end of the pipeline.
# APP
#   Application gets detected and marked, no middleware should get this
#   group.
NONE = 0
FIRST = 1
SHORT_CIRCUIT = 2
PRE_AUTH = 3
AUTH = 4
POST_AUTH = 5
LAST = 6
APP = 7


class BaseMiddleware(object):
    # List of middleware class names that are required
    requires = []

    # List of middleware class names that this middleware should be before
    # if it appears in this pipeline.
    before = []

    # List of middleware class names that this middleware should be after
    # if it appears in this pipeline.
    after = []

    # Group this middleware is apart of (see comment at top of file)
    group = NONE
