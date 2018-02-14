from typing import (Dict, List, Tuple, Optional, Any)
import datetime

import requests
from bs4 import BeautifulSoup
import pygsheets
import phonenumbers

import shelterluv

class ExistingDogException(Exception):
    pass

def get_internal_id(page: BeautifulSoup) -> str:
    link = page.select_one('link[rel="shortlink"]').attrs['href']
    return link.split('/')[-1]

def get_person_apa_id(page: BeautifulSoup) -> str:
    link = page.select_one('#animal_loc_foster_link')
    if link is None:
        return ''
    href = link.attrs['href']
    return href.split('-')[-1]

def get_bc_mc(dog: Dict[str, Any]) -> str:
    def _():
        for x in dog['Attributes']:
            attr_name = x['AttributeName']
            if attr_name in ('Behavior Consult', 'Medical Consult'):
                yield ''.join(w[0] for w in attr_name.split())
    return '/'.join(_())

def get_dob(dog: Dict[str, Any]) -> str:
    return datetime.datetime.fromtimestamp(dog['DOBUnixTime']).strftime('%m/%d/%Y') 

def get_fee(dog: Dict[str, Any]) -> str:
    return dog['AdoptionFeeGroup']['Price']

def get_phone(person: Dict[str, Any]) -> str:
    ph = phonenumbers.parse(person['Phone'])
    return phonenumbers.format_number(ph, phonenumbers.PhoneNumberFormat.NATIONAL)

def fix_formulas(ws):
    rows = ws.jsonSheet['properties']['gridProperties']['rowCount']
    values = [
        [f'=IF(trim(E{i})="","--",(TODAY()-E{i})/7)']
        for i in range(3, rows)
    ]
    ws.update_cells('F3:F', values)


class Foster(object):

    def __init__(self,
                 api_key: str, sl_username: str, sl_password: str,
                 sheet_key: str, service_file: str) -> None:
        self.api_key = api_key
        self.username = sl_username
        self.password = sl_password
        self.sheet_key = sheet_key
        self.service_file = service_file
        self.session: Optional[requests.sessions.Session] = None
        self.sheet: Optional[pygsheets.spreadsheet.Spreadsheet] = None

    def login(self) -> None:
        self.session = requests.Session()
        r = self.session.post(
            'https://www.shelterluv.com/user/login',
            {
                'name': self.username,
                'pass': self.password,
                'op': 'Log in',
                'form_id': 'user_login'
            }
        )
        r.raise_for_status()

    def logout(self) -> None:
        r = self.session.get('https://www.shelterluv.com/user/logout')
        r.raise_for_status()

    def open_sheet(self) -> None:
        client = pygsheets.authorize(service_file=self.service_file)
        self.sheet = client.open_by_key(self.sheet_key)

    def get_internal_ids(self, apa_id: str) -> Tuple[str, str]:
        r = self.session.get(f'https://www.shelterluv.com/APA-A-{apa_id}')
        r.raise_for_status()
        page = BeautifulSoup(r.text, 'html5lib')
        dog_internal_id = get_internal_id(page)
        person_apa_id = get_person_apa_id(page)
        if person_apa_id == '':
            return dog_internal_id, ''

        r = self.session.get(f'https://www.shelterluv.com/APA-P-{person_apa_id}')
        r.raise_for_status()
        page = BeautifulSoup(r.text, 'html5lib')
        person_internal_id = get_internal_id(page)

        return dog_internal_id, person_internal_id

    def dog_info(self, internal_id: str) -> Dict[str, Any]:
        return shelterluv.get_animal(self.api_key, internal_id)

    def person_info(self, internal_id: str) -> Dict[str, Any]:
        if internal_id == '':
            return {
                'Firstname': '',
                'Lastname': '',
                'Email': '',
                'Phone': '+18888888888'
            }
        return shelterluv.get_people(self.api_key, internal_id)

    def info(self, apa_id: str) -> Dict[str, Any]:
        dog_id, person_id = self.get_internal_ids(apa_id)
        dog = self.dog_info(dog_id)
        person = self.person_info(person_id)
        return {
            'Name': dog['Name'],
            'ID Number': dog['ID'],
            'BC/MC': get_bc_mc(dog),
            'Breed': dog['Breed'],
            'DOB': get_dob(dog),
            'Sex': dog['Sex'],
            'Spay / Neuter': dog['Altered'],
            'Foster Full Name': ' '.join([person['Firstname'], person['Lastname']]),
            'Email': person['Email'],
            'Cell Phone': get_phone(person),
            'Adoption Fee': get_fee(dog),
            'Status': dog['Status'],
            'dog_internal_id': dog_id,
            'person_internal_id': person_id
        }

    def append_dog(self, apa_id: str) -> None:
        ws = self.sheet.worksheet_by_title('Tracking')
        info = self.info(apa_id)
        ids = ws.get_col(2)
        if apa_id in ids:
            raise ExistingDogException(apa_id)
        ws.insert_rows(
            len(ids),
            values=[[
                info['Name'],
                apa_id,
                info['BC/MC'],
                info['Breed'],
                info['DOB'],
                '=IF(trim(E)="","--",(TODAY()-E)/7)',
                info['Adoption Fee'],
                info['Sex'],
                info['Spay / Neuter'],
                '', '', '', '',
                info['Foster Full Name'],
                info['Email'],
                '',
                info['Cell Phone'],
                '', '', '', '', '', '',
                info['Status'],
                '', '', '', '', '', '', '',
                info['dog_internal_id'],
                info['person_internal_id']
            ]]
        )
        fix_formulas(ws)
