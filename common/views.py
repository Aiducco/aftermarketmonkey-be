import typing

import simplejson
from django import http, views


class Handler404View(views.View):
    def get(
        self, request: http.HttpRequest, *args: typing.Any, **kwargs: typing.Any
    ) -> http.HttpResponse:
        return http.HttpResponse(
            headers={"Content-Type": "application/json"},
            content=simplejson.dumps(
                {
                    "error": {
                        "title": "The endpoint {} you are trying to access does not exist.".format(
                            request.path
                        )
                    }
                }
            ),
            status=404,
        )
