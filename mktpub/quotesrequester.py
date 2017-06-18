"""Quotes request abstraction"""
import time
import requests

from saifu.core.models import Quote
from saifu.core import utils


def _extract_pairs(timestamp, data):
    """Extract the currency pairs from a response"""
    for source, targets in data.iteritems():
        for target, price in targets.iteritems():
            ticker = "{}{}".format(source, target)
            yield Quote(ticker, price, timestamp)

def _build_uri(base_uri, sources, targets):
    """Builds resource url from settings"""
    store = {
        "sources": ",".join(sources),
        "targets": ",".join(targets)
    }
    return base_uri.format(**store)


def _is_error_response(response):
    """Returns true if the response returned by the provider is an error"""
    return "Response" in response and str(response["Response"]) == "Error"


def _get_message_from_response(response):
    """Extracts the message from the provider response"""
    if "Message" not in response:
        return "unknown"
    return str(response["Message"])

def _extract_sources_targets(pairs):
    sources = set()
    targets = set()
    for source, target in pairs:
        sources.add(source)
        targets.add(target)
    return sources, targets

class RequesterException(Exception):
    """Thrown when an error occurs in quotes requester"""
    def __init__(self, message):
        super(RequesterException, self).__init__(message)


class Requester(object):
    """Requests quotes for a given list of source and target currencies"""

    _HTTP_STATUS_CODE_OK = 200

    def __init__(self, logger, resource, pairs):
        self.resource = resource
        self.pairs = pairs
        self.logger = logger

    def get(self):
        """Gets the quotes for the sources and targets currency pairs"""
        sources, targets = _extract_sources_targets(self.pairs)
        resource = _build_uri(self.resource, sources, targets)
        self.logger.debug("Will fetch quotes from {}".format(resource))
        try:
            response = requests.post(resource)
            if response.status_code != Requester._HTTP_STATUS_CODE_OK:
                raise RequesterException(
                    "Service responded with unexpected http code ({})".format(
                        response.status_code))

            data = response.json()

            if _is_error_response(response):
                raise RequesterException(
                    "Service responded with an error ({})".format(
                        _get_message_from_response(data)))

            for pair in _extract_pairs(utils.utc_time(), data):
                yield pair

        except requests.exceptions.RequestException as error:
            raise RequesterException(
                "Unable to send request to service ({})".format(
                    str(error)))
