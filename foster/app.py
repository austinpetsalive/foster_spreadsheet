from typing import (Dict, List, Tuple, Optional, Any)
import datetime
import json
import os
import base64
import functools

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
    try:
        ph = phonenumbers.parse(person['Phone'])
        return phonenumbers.format_number(ph, phonenumbers.PhoneNumberFormat.NATIONAL)
    except phonenumbers.NumberParseException:
        return ''

def fix_formulas(ws):
    rows = ws.jsonSheet['properties']['gridProperties']['rowCount']
    values = [
        [f'=IF(trim(E{i})="","--",(TODAY()-E{i})/7)']
        for i in range(3, rows)
    ]
    ws.update_cells('F3:F', values)
    values = [
        [f'=IF(K{i}, DAYS360(K{i}, TODAY()), "--")']
        for i in range(3, rows)
    ]
    ws.update_cells('AB3:AB', values)

def new_row(old_row: List, dog: Dict[str, Any], person: Dict[str, Any],
            apa_id: str, dog_internal_id: str, person_internal_id: str) -> List:
    old_row[0] = dog['Name']
    old_row[1] = apa_id
    old_row[2] = get_bc_mc(dog)
    old_row[3] = dog['Breed']
    old_row[4] = get_dob(dog)
    old_row[6] = dog['Sex']
    old_row[7] = dog['Altered']
    old_row[12] = ' '.join([person['Firstname'], person['Lastname']])
    old_row[13] = person['Email']
    old_row[15] = get_phone(person)
    old_row[22] = dog['Status']
    old_row[23] = get_fee(dog)
    old_row[31] = dog_internal_id
    old_row[32] = person_internal_id
    return old_row


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

    def refresh_all(self):
        ws = self.sheet.worksheet_by_title('Tracking')
        all_values = ws.get_all_values()
        new_values = []
        for row in all_values[2:]:
            apa_id = row[1]
            if not apa_id:
                break
            print(f'Processing {apa_id}')
            dog_internal_id = row[31]
            person_internal_id = row[32]
            if not (dog_internal_id and person_internal_id):
                print('.. missing some ids, will get them')
                dog_internal_id, person_internal_id = self.get_internal_ids(apa_id)
            dog = self.dog_info(dog_internal_id)
            person = self.person_info(person_internal_id)
            new_values.append(
                new_row(row, dog, person, apa_id, dog_internal_id, person_internal_id))
        print('Updating spreadsheet')
        ws.update_cells('A3', values=new_values)
        fix_formulas(ws)
        timestamp = datetime.datetime.now().strftime('%m/%d/%Y %I:%M %p')
        ws.update_cell(
            'A1',
            f'Last Full Update: {timestamp}'
        )
        # get all values
        # if value has both internals
        #    self.dog_info, self.person_info
        # else
        #    self.info(apa_id)
        # update row
        # update_values
        # update timestamp

    def append_dog(self, apa_id: str) -> None:
        ws = self.sheet.worksheet_by_title('Tracking')
        ids = ws.get_col(2)
        values = ['']*32
        if apa_id in ids:
            print(f'Updating dog {apa_id}')
            row_number = ids.index(apa_id) + 1
            values = ws.get_row(row_number)
            values = values + (['']*32)[:32 - len(values)]
            fun = functools.partial(ws.update_row, row_number)
        else:
            print(f'Creating new row for {apa_id}')
            fun = functools.partial(ws.insert_rows, len(ids))
        try:
            dog_internal_id, person_internal_id = self.get_internal_ids(apa_id)
            dog = self.dog_info(dog_internal_id)
            person = self.person_info(person_internal_id)
            fun(
                values=[new_row(values, dog, person, apa_id, dog_internal_id, person_internal_id)]
            )
        except Exception as exc:
            ws.insert_rows(
                len(ids),
                values=[[
                    f'Error (most likely {apa_id} was not found)',
                    apa_id
                ]]
            )
        fix_formulas(ws)


def get_service_file(b64_string: str) -> None:
    if os.path.exists('secret.json'):
        return
    b = base64.b64decode(b64_string)
    s = b.decode('ascii')
    with open('secret.json', 'w') as f:
        f.write(s)

def handle(req: str):
    api_key = os.environ['API_KEY']
    sl_username = os.environ['SL_USERNAME']
    sl_password = os.environ['SL_PASSWORD']
    sheet_key = os.environ['SHEET_KEY']
    get_service_file(os.environ['SERVICE_FILE_B64'])
    f = Foster(
        api_key, sl_username, sl_password, sheet_key, 'secret.json')
    f.login()
    f.open_sheet()

    r = json.loads(req)
    if r.get('refresh'):
        f.refresh_all()
    else:
        dog = r['apa_id']
        f.append_dog(dog)
