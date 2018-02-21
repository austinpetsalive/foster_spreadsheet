import re
import json
import os
import itertools
import urllib.parse
from typing import (Iterator, Dict, List, Sequence, Any, Optional,
                    Tuple, Callable, DefaultDict, Mapping, Union)

import requests


Animal = Dict[str, Any]
Person = Dict[str, Any]
Events = Dict[str, Any]
Scores = Dict[str, Union[str, int]]


class APIError(Exception):
    pass

def do_request(
        key: str,
        url: str,
        params: Optional[Dict] = None,
        field: Optional[str] = None) -> Iterator[Dict[str, Any]]:
    offset = 0
    concat_params = dict(params) if params is not None else {}
    while True:
        concat_params['offset'] = offset
        params_for_url = urllib.parse.urlencode(concat_params)
        print(f'Requesting offset = {offset}, params = {params_for_url}')
        r = requests.get(
            f'https://www.shelterluv.com/api/v1/'
            f'{url}?{params_for_url}',
            headers={'X-Api-Key': key})
        r.raise_for_status()
        j = r.json()
        if field is None:
            yield j
            return
        if j['success'] != 1:
            raise APIError(j['error_message'])
        yield from j[field]
        if not j['has_more']:
            return
        offset += 100

def get_from_shelterluv(
        key: str,
        url: str,
        field: Optional[str] = None) -> Iterator[Dict[str, Any]]:
    return do_request(key, url, params={'status_type': 'in custody'}, field=field)

def get_all_animals(key: str) -> Iterator[Animal]:
    return get_from_shelterluv(key, 'animals', 'animals')

def get_all_dogs(key: str) -> Iterator[Animal]:
    for a in get_all_animals(key):
        if a['Type'] != 'Dog':
            continue
        yield a

def get_shelter_dogs(key: str, include_not_available=False) -> Iterator[Animal]:
    for a in get_all_animals(key):
        if a['Type'] == 'Dog':
            if a['Name'] == 'Kobu':
                a['CurrentLocation'] = {
                    'Tier3': '',
                    'Tier4': ''
                }
                yield a
                continue
            cur_loc = a['CurrentLocation']
            if cur_loc and cur_loc.get('Tier2') == 'Off Site':
                a['CurrentLocation'] = {
                    'Tier3': 'Off Site',
                    'Tier4': ''
                }
                yield a
                continue
            if ((include_not_available or a['Status'] == 'Available In Shelter')
                and a.get('CurrentLocation')
                and a['CurrentLocation'].get('Tier2') == 'TLAC'
                and a['CurrentLocation'].get('Tier3') != 'Barn'
                and not a['CurrentLocation'].get('Tier3').startswith('Medical')
                and not a['CurrentLocation'].get('Tier3').startswith('Parvo')
                and not a['CurrentLocation'].get('Tier3').startswith('Maternity')):
                yield a

def get_shelter_dogs_test(key: str) -> Iterator[Animal]:
    yield from [
        {'Name': 'Foo', 'ID': 3},
        {'Name': 'Bar', 'ID': 12},
        {'Name': 'Baz', 'ID': 23312}
    ]

def get_id(a: Animal) -> str:
    return a['Internal-ID']

def get_animal(key: str, animal_id: str) -> Animal:
    return next(get_from_shelterluv(key, f'animals/{animal_id}'))

def get_events(animal_id: str) -> Iterator[Events]:
    return get_from_shelterluv(f'animals/{animal_id}/events', 'events')

def get_people(key: str, person_id: str) -> Person:
    return next(do_request(key, f'people/{person_id}'))

def get_all_people(key: str) -> Iterator[Person]:
    return do_request(key, f'people', field='people')

def json_source(key: str):
    with open('src.json') as f:
        yield from json.load(f)

def get_location(dog: Animal) -> Tuple[str, str]:
    d = dog.get('CurrentLocation', {}) or {}
    return d.get('Tier3', 'Unknown'), d.get('Tier4', 'Unknown')

def limited_source(
        src: Callable[[str], Iterator[Animal]],
        limit: int = 3) -> Callable[[str], Iterator[Animal]]:
    def _f(key):
        return itertools.islice(src(key), limit)
    return _f

class Shelterluv(object):

    def __init__(self, key: str, source: Callable[[str], Iterator[Animal]]) -> None:
        self.key = key
        self.source = source
        self.refresh()

    def refresh(self):
        full = list(self.source(self.key))
        self.by_name = {dog['Name']: dog for dog in full}

        self.by_location = {
            get_location(dog): dog
            for dog in full
        }
        self.by_apa_id = {
            dog['ID']: dog for dog in full
        }

    def scores(self, apa_id: str) -> Scores:
        dog = self.by_apa_id[apa_id]
        attributes = dog.get('Attributes', []) or []
        scores: Scores = {
            'Dog': 0,
            'Child': 0,
            'Cat': 0,
            'Home': 0,
            'Energy': 'Unknown'
        }
        for attr in attributes:
            m = re.match('SCORE - (\w+) \((\d) out of 5\)', attr['AttributeName'])
            if m is not None:
                category, score = m.groups()
                scores[category] = int(score)
            m = re.match('ENERGY - (\w+)', attr['AttributeName'])
            if m is not None:
                energy = m.group(1)
                scores['Energy'] = energy
        return scores
