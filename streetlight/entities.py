#!/usr/bin/env python3
"""
Entities generator for streetlights

Reads a .CSV files with streetlight properties,
and generates a configuration file for
https://github.com/telefonicasc/urbo-cli
"""

from typing import Optional, Any, Mapping, Sequence, Tuple, cast
import asyncio
import aiohttp
import pandas as pd
import jinja2


class FetchError(Exception):
    """Exception raised when Fetch fails"""

    # pylint: disable=super-init-not-called
    def __init__(self,
                 url: str,
                 resp: aiohttp.ClientResponse,
                 meta: Optional[Any] = None):
        """Build error info from Fetch request"""
        self.url = url
        self.status = resp.status
        self.meta = meta

    def __str__(self) -> str:
        """Format exception"""
        return f'URL[{self.url}]: {self.status}'


async def login(session: aiohttp.ClientSession, user: str, password: str,
                domain: str) -> str:
    """Get auth token"""
    data = {
        "auth": {
            "identity": {
                "methods": ["password"],
                "password": {
                    "user": {
                        "domain": {
                            "name": domain
                        },
                        "name": user,
                        "password": password
                    }
                }
            }
        }
    }
    url = 'https://iot.demo.urbo2.es/idm/auth/tokens'
    async with session.post(url, json=data) as reply:
        if reply.status != 201:
            raise FetchError(url, reply, meta=data)
        return reply.headers['x-subject-token']


async def entities(session: aiohttp.ClientSession, token: str, service: str,
                   subservice: str) -> Mapping[str, pd.DataFrame]:
    """ Collects entities of all types, indexed by entity id"""
    url = 'http://iot.demo.urbo2.es:1026/v2/entities'
    headers = {
        "fiware-service": service,
        "fiware-servicepath": subservice,
        "x-auth-token": token,
    }
    async with session.get(url, headers=headers) as reply:
        if reply.status != 200:
            raise FetchError(url, reply, meta=headers)
        items = await reply.json()
    tnames = frozenset(item['type'] for item in items)

    def match(items: Sequence[Mapping[str, Any]],
              type_name: str) -> pd.DataFrame:
        """Return items that match the given type_name"""
        return pd.DataFrame((item for item in items
                             if item['type'] == type_name)).set_index('id')

    return dict((tname, match(items, tname)) for tname in tnames)


def point_to_tuple(points: Sequence[str]) -> Tuple[Tuple[float, float], ...]:
    """Transforms a 'POINT (xxxx yyyy)' TO (x, y)"""
    return tuple(
        cast(
            Tuple[float, float],
            tuple(
                float(x.strip())
                for x in point.split("(")[1].strip(")").split(" ")[:2]))
        for point in points)


def group(group_size: int):
    """Return a list of group ids, starting at 1 and padded to 4 digits"""
    return tuple(f'{x:04}' for x in range(1, group_size + 1))


async def read_lights(csvfile: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Read the list of lights from a csv export of data.

    Artificially creates a cabinet per each street.
    The cabinet dataframe has the following properties:

    - [axis]: name of the street ('calle_cal' field in streetlight CSV)
    - group: id, from '0001' to the number of streets ('9999' max)
    - WKT_X, WKT_Y: coordinates of the cabinet (copied fom the first lamp)
    - lamparas_p: combined power of all the lamps in the cabinet.

    The streetlight dataframe is enhanced with the following fields:

    - group: group of the cabinet to where it belongs
    - WKT_X, WKT_Y: coordinates of the lamp.
    """
    streetlights = pd.read_csv(csvfile).sort_values(
        by=['id_luminar', 'id_lampara', 'id_soporte'], axis=0)
    # Filter out empty points
    streetlights = streetlights[streetlights.WKT != 'POINT EMPTY']
    # And limit to 9999 entities
    streetlights = streetlights[:9999]
    # Turn coordinates into float
    streetlights['POINT'] = point_to_tuple(streetlights.WKT)
    streetlights['WKT_X'] = tuple(item[0] for item in streetlights.POINT)
    streetlights['WKT_Y'] = tuple(item[1] for item in streetlights.POINT)
    cabinets = streetlights.groupby(by='calles_cal').agg({
        'lamparas_p': sum,
        'WKT_X': 'first',
        'WKT_Y': 'first',
    })
    # Assign lights and group IDs
    cabinets = cabinets.assign(group=group(len(cabinets)))
    # Join cabinets to streetlights, so we get group ID
    streetlights = streetlights.join(cabinets,
                                     on='calles_cal',
                                     lsuffix='_cabinet')
    return (cabinets, streetlights)


async def read_entities(user: str, password: str, service: str,
                        subservice: str):
    """Read the list of entities from context broker"""
    async with aiohttp.ClientSession() as session:
        # Get token
        token = await login(session, user, password, service)
        # Enumerate entitites
        frames = await entities(session, token, service, subservice)
        # print out entities
        for type_name, frame in frames.items():
            print(f'**** TYPE: {type_name} ****')
            print(frame.head())


async def main():
    """Reads the CSV with light positions, and prints a sim config"""
    csvfile = 'exportado.csv'
    tmplfile = 'streetlight.tmpl'
    env = jinja2.Environment(loader=jinja2.FileSystemLoader('.'))

    cabinets, streetlights = await read_lights(csvfile)
    template = env.get_template(tmplfile)
    print(template.render(cabinets=cabinets, streetlights=streetlights))


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
