#!/usr/bin/python
#
# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import random
import time
from concurrent import futures
from uuid import uuid4

# @TODO: Temporarily removed in https://github.com/GoogleCloudPlatform/microservices-demo/pull/3196
# import googlecloudprofiler

import grpc

import demo_pb2
import demo_pb2_grpc
from grpc_health.v1 import health_pb2
from grpc_health.v1 import health_pb2_grpc

from logger import getJSONLogger
logger = getJSONLogger('recommendationservice-server')

def initStackdriverProfiling():
  project_id = None
  try:
    project_id = os.environ["GCP_PROJECT_ID"]
  except KeyError:
    # Environment variable not set
    pass

  # @TODO: Temporarily removed in https://github.com/GoogleCloudPlatform/microservices-demo/pull/3196
  # for retry in range(1,4):
  #   try:
  #     if project_id:
  #       googlecloudprofiler.start(service='recommendation_server', service_version='1.0.0', verbose=0, project_id=project_id)
  #     else:
  #       googlecloudprofiler.start(service='recommendation_server', service_version='1.0.0', verbose=0)
  #     logger.info("Successfully started Stackdriver Profiler.")
  #     return
  #   except (BaseException) as exc:
  #     logger.info("Unable to start Stackdriver Profiler Python agent. " + str(exc))
  #     if (retry < 4):
  #       logger.info("Sleeping %d seconds to retry Stackdriver Profiler agent initialization"%(retry*10))
  #       time.sleep (1)
  #     else:
  #       logger.warning("Could not initialize Stackdriver Profiler after retrying, giving up")
  return

class RecommendationService(demo_pb2_grpc.RecommendationServiceServicer):
    def _log_event(self, event, fields, level='info', **details):
        log_fields = {**fields, **details, 'event': event}
        getattr(logger, level)(
            '[RecommendationRetry] event=%s request_id=%s attempt_count=%s retry_count=%s stop_reason=%s',
            event, log_fields['request_id'], log_fields['attempt_count'], log_fields['retry_count'],
            log_fields['stop_reason'], extra=log_fields,
        )

    def _retry_timeout(self, context, deadline, fields):
        timeout = min(1.0, deadline - time.monotonic())
        remaining = context.time_remaining()
        if remaining is not None:
            timeout = min(timeout, remaining)
        active = context.is_active()
        if active and timeout > 0:
            return timeout
        reason = 'client_cancelled' if not active else 'request_budget_exhausted'
        if active and remaining is not None and remaining <= 0:
            reason = 'incoming_deadline_exceeded'
        fields.update(stop_reason=reason, fallback_used=fields['attempt_count'] > 0)
        self._log_event('catalog_retry_stopped', fields, level='warning', retry_reason=reason)
        if fields['attempt_count'] == 0:
            code = grpc.StatusCode.CANCELLED if not active else grpc.StatusCode.DEADLINE_EXCEEDED
            context.abort(code, 'recommendation request no longer active')
        return None

    def _fetch_candidates(self, excluded_ids, fields, timeout=None, before_call=None):
        attempt = fields['attempt_count'] + 1
        self._log_event('catalog_attempt_started', fields, attempt=attempt,
                        rpc_timeout_ms=None if timeout is None else round(timeout * 1000, 3))
        if before_call is not None:
            timeout = before_call()
            if timeout is None:
                self._log_event('catalog_attempt_skipped', fields, level='warning', attempt=attempt)
                return None
        fields['attempt_count'] = attempt
        fields['retry_count'] = attempt - 1
        started = time.perf_counter()
        try:
            if timeout is None:
                catalog = product_catalog_stub.ListProducts(demo_pb2.Empty())
            else:
                catalog = product_catalog_stub.ListProducts(demo_pb2.Empty(), timeout=timeout)
        except grpc.RpcError as error:
            status = error.code() if callable(getattr(error, 'code', None)) else grpc.StatusCode.UNKNOWN
            fields['last_rpc_status'] = status.name if isinstance(status, grpc.StatusCode) else 'UNKNOWN'
            if fields['attempt_count'] == 1:
                fields['stop_reason'] = 'initial_catalog_rpc_failed'
            self._log_event('catalog_attempt_failed', fields, level='error', attempt=attempt,
                            rpc_timeout_ms=None if timeout is None else round(timeout * 1000, 3),
                            rpc_status=fields['last_rpc_status'], error_type=type(error).__name__,
                            rpc_duration_ms=round((time.perf_counter() - started) * 1000, 3))
            if attempt == 1:
                self._log_event('catalog_retry_stopped', fields, level='error', retry_reason=fields['stop_reason'])
            raise
        duration_ms = round((time.perf_counter() - started) * 1000, 3)
        catalog_ids = {product.id for product in catalog.products}
        candidates = list(catalog_ids - excluded_ids)
        fields.update(catalog_count=len(catalog_ids), effective_excluded_count=len(catalog_ids & excluded_ids),
                      candidate_count=len(candidates), last_rpc_status='OK')
        self._log_event('catalog_attempt_succeeded', fields, attempt=attempt, rpc_status='OK',
                        rpc_timeout_ms=None if timeout is None else round(timeout * 1000, 3), rpc_duration_ms=duration_ms)
        return candidates

    def _get_candidates(self, request, context, fields):
        """Opt-in fault: mistake valid short candidates for an incomplete catalog."""
        excluded_ids = set(request.product_ids)
        if not fields['retry_bug_enabled']:
            candidates = self._fetch_candidates(excluded_ids, fields) or []
            fields['stop_reason'] = 'retry_disabled'
            self._log_event('catalog_retry_stopped', fields, retry_reason=fields['stop_reason'])
            return candidates

        deadline = time.monotonic() + 3.0
        candidates = []
        for attempt in range(1, fields['max_attempts'] + 1):
            timeout = self._retry_timeout(context, deadline, fields)
            if timeout is None:
                return candidates
            try:
                fetched = self._fetch_candidates(excluded_ids, fields, timeout,
                                                 before_call=lambda: self._retry_timeout(context, deadline, fields))
                if fetched is None:
                    return candidates
                candidates = fetched
            except grpc.RpcError:
                if attempt == 1:
                    raise
                fields.update(stop_reason='catalog_rpc_failed', fallback_used=True)
                break

            if not 0 < len(candidates) < 3:
                fields['stop_reason'] = 'empty_candidates' if not candidates else 'candidates_sufficient'
                break
            fields['retry_condition_met'] = True
            if attempt == fields['max_attempts']:
                fields.update(stop_reason='attempts_exhausted', fallback_used=True)
                break
            self._log_event('catalog_retry_triggered', fields, level='warning', retry_reason='insufficient_candidates',
                            next_attempt=attempt + 1, candidate_threshold=3)
        self._log_event('catalog_retry_stopped', fields, level='warning' if fields['fallback_used'] else 'info',
                        retry_reason=fields['stop_reason'])
        return candidates

    def ListRecommendations(self, request, context):
        started = time.perf_counter()
        enabled = os.environ.get('RECOMMENDATION_RETRY_BUG_ENABLED') == '1'
        fields = {
            'service': 'recommendationservice', 'request_id': uuid4().hex,
            'method': '/hipstershop.RecommendationService/ListRecommendations',
            'downstream_method': '/hipstershop.ProductCatalogService/ListProducts',
            'retry_bug_enabled': enabled, 'max_attempts': 5 if enabled else 1,
            'excluded_count': len(set(request.product_ids)), 'attempt_count': 0, 'retry_count': 0,
            'candidate_count': 0, 'returned_count': 0, 'retry_condition_met': False,
            'fallback_used': False, 'stop_reason': '', 'request_status': 'IN_PROGRESS', 'elapsed_ms': 0.0,
        }
        self._log_event('recommendation_request_started', fields)
        try:
            candidates = self._get_candidates(request, context, fields)
            prod_list = random.sample(candidates, min(5, len(candidates)))
            response = demo_pb2.ListRecommendationsResponse(product_ids=prod_list)
            fields.update(returned_count=len(prod_list), request_status='SUCCESS')
            return response
        except Exception as error:
            fields['request_status'] = 'ERROR'
            if not fields['stop_reason']:
                fields['stop_reason'] = 'unexpected_error'
            self._log_event('recommendation_request_failed', fields, level='error', error_type=type(error).__name__)
            raise
        finally:
            fields['elapsed_ms'] = round((time.perf_counter() - started) * 1000, 3)
            self._log_event('recommendation_request_completed', fields,
                            level='error' if fields['request_status'] == 'ERROR' else 'info')

    def Check(self, request, context):
        return health_pb2.HealthCheckResponse(
            status=health_pb2.HealthCheckResponse.SERVING)

    def Watch(self, request, context):
        return health_pb2.HealthCheckResponse(
            status=health_pb2.HealthCheckResponse.UNIMPLEMENTED)


if __name__ == "__main__":
    logger.info("initializing recommendationservice")

    try:
      if "DISABLE_PROFILER" in os.environ:
        raise KeyError()
      else:
        logger.info("Profiler enabled.")
        initStackdriverProfiling()
    except KeyError:
        logger.info("Profiler disabled.")

    port = os.environ.get('PORT', "8080")
    catalog_addr = os.environ.get('PRODUCT_CATALOG_SERVICE_ADDR', '')
    if catalog_addr == "":
        raise Exception('PRODUCT_CATALOG_SERVICE_ADDR environment variable not set')
    logger.info("product catalog address: " + catalog_addr)
    channel = grpc.insecure_channel(catalog_addr)
    product_catalog_stub = demo_pb2_grpc.ProductCatalogServiceStub(channel)

    # create gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    # add class to gRPC server
    service = RecommendationService()
    demo_pb2_grpc.add_RecommendationServiceServicer_to_server(service, server)
    health_pb2_grpc.add_HealthServicer_to_server(service, server)

    # start server
    logger.info("listening on port: " + port)
    server.add_insecure_port('[::]:'+port)
    server.start()

    # keep alive
    try:
         while True:
            time.sleep(10000)
    except KeyboardInterrupt:
            server.stop(0)
