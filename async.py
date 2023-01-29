import aiohttp
import asyncio
import datetime
import os

import requests
from dotenv import load_dotenv
from more_itertools import chunked
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import declarative_base, sessionmaker

URL = 'https://swapi.dev/'
MAX_CHUNK = 10


def get_len():
    response = requests.get(f'{URL}api/people/')
    json_data = response.json()
    return json_data['count']

unit_count = get_len() + 1
print('unit count: ', unit_count)


async def check_health(session: aiohttp.ClientSession):
    while True:
        async with session.get(URL) as response:
            print(response.status)


async def get_unit(session, unit_id):
    async with session.get(f'{URL}api/people/{unit_id}') as response:
        json_data = await response.json()
        return json_data


async def get_unit_data():
    unit_list = []
    async with aiohttp.ClientSession() as session:
        coroutines = (get_unit(session, i) for i in range(1, unit_count+1))
        for coroutines_chunk in chunked(coroutines, MAX_CHUNK):
            result = await asyncio.gather(*coroutines_chunk)
            for item in result:
                print(item)
                if 'name' in item:
                    unit_list.append(item)
        return unit_list

load_dotenv()

db_name = os.environ.get('POSTGRES_DB')
db_pass = os.environ.get('POSTGRES_PASSWORD')
db_user = os.environ.get('POSTGRES_USER')
print('db name: ', db_name)


PG_DSN = f'postgresql://postgres:postgres@127.0.0.1:5432/{db_name}'
PG_DSN_ALC = f'postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/{db_name}'
engine = create_async_engine(PG_DSN_ALC, echo=False)
Base = declarative_base()
Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def create_db_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
        print('db has been created')


class Unit(Base):
    __tablename__ = 'unit_db'

    id = Column(Integer, primary_key=True)
    name = Column(String, index=True)
    birth_year = Column(String, index=True)
    eye_color = Column(String, index=True)
    films = Column(String, index=True)
    gender = Column(String, index=True)
    hair_color = Column(String, index=True)
    height = Column(String, index=True)
    homeworld = Column(String, index=True)
    mass = Column(String, index=True)
    skin_color = Column(String, index=True)
    species = Column(String, index=True)
    starships = Column(String, index=True)
    vehicles = Column(String, index=True)

async def save_unit_in_db(unit_data):
    async with Session() as session:
        async with session.begin():
            for item in unit_data[0]:
                name = item['name']
                birth_year = item['birth_year']
                eye_color = item['eye_color']
                films = ','.join(item['films'])
                gender = item['gender']
                hair_color = item['hair_color']
                height = item['height']
                homeworld = item['homeworld']
                mass = item['mass']
                skin_color = item['skin_color']
                species = ','.join(item['species'])
                starships = ','.join(item['starships'])
                vehicles = ','.join(item['vehicles'])
                unit = Unit(name=name, birth_year=birth_year, eye_color=eye_color, films=films,
                            gender=gender, hair_color=hair_color, height=height, homeworld=homeworld,
                            mass=mass, skin_color=skin_color, species=species, starships=starships,
                            vehicles=vehicles)
                session.add(unit)


async def async_main():
    await create_db_tables()
    unit_data = await asyncio.gather(get_unit_data())
    await save_unit_in_db(unit_data)


async def main():
    await create_db_tables()
    unit_data = await asyncio.gather(get_unit_data())
    await save_unit_in_db(unit_data)


start = datetime.datetime.now()
if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())
print('saved at database ', datetime.datetime.now()-start)