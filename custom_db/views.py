from rest_framework.views import APIView
import logging

logger = logging.getLogger(__name__)

import logging
from typing import Optional

# Third Party Stuff
from rest_framework.response import Response



def status_200(message="", data=None):
    # Explicit None check is required, because data can contain empty list - []
    if data is not None:
        return Response({"status": 200, "message": message, "data": data})
    return Response({"status": 200, "message": message})


class BaseAPIView(APIView):
    def log_error(self, error):
        logger.error(error, exc_info=True)

    def log_exception(self, exception):
        logger.exception(exception)


class TryApi(BaseAPIView):
    def post(self, request):
        # res = CustomUser.objects.all()[:10]
        from django.db import connection
        #
        print(f"Database Engine: {connection.vendor}")
        # logger.info(f"[DEBUG] :: {res=}")
        from django.db import connection
        cursor = connection.cursor()
        cursor.execute("SELECT 1")

        return status_200("Ok", data={})