"""Expose prometheus_client metrics on the Faust worker web server."""

from prometheus_client import generate_latest

from faust import web

# aiohttp rejects charset in content_type; Prometheus expects this MIME type.
PROMETHEUS_CONTENT_TYPE = 'text/plain; version=0.0.4'

prometheus_blueprint = web.Blueprint('prometheus')


@prometheus_blueprint.route('/metrics', name='metrics')
class PrometheusMetrics(web.View):
    """Prometheus scrape endpoint (consumer:6066/metrics)."""

    async def get(self, request: web.Request) -> web.Response:
        return self.bytes(
            generate_latest(),
            content_type=PROMETHEUS_CONTENT_TYPE,
        )
