import json
from typing import Optional, Dict

from fastapi import Query


def json_parser(q: Query, expected_type=str, return_type=Optional[Dict], default=None):
    def parse_json(names: expected_type = q) -> return_type:
        if names is None:
            return default

        # we already have a list, we can return
        if isinstance(names, list) and len(names) > 0:
            return names
        elif isinstance(names, dict) and len(names.keys()) > 0:
            return names

        # noinspection PyTypeChecker
        ret = json.loads(names)
        if isinstance(ret, list):
            return ret if len(ret) > 0 else default
        elif isinstance(ret, dict):
            return ret if len(ret.keys()) > 0 else default
        return ret

    return parse_json
