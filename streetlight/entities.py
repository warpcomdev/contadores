#!/usr/bin/env python3
"""
Entities generator for streetlights

Reads a .CSV files with streetlight properties,
and generates a configuration file for
https://github.com/telefonicasc/urbo-cli
"""

from typing import Sequence, Tuple, cast
import asyncio
import pandas as pd
import jinja2


def point_to_tuple(points: Sequence[str]) -> Tuple[Tuple[float, float], ...]:
    """
    Transforms a sequence of 'POINT (xxxx yyyy)' or 'MULTIPOINT ((xxx yyy))'
    to a tuple of (x, y) coordinate pairs.
    """
    return tuple(
        cast(
            Tuple[float, float],
            tuple(
                float(x.strip())
                for x in point.split("(")[-1].strip(")").split(" ")[:2]))
        for point in points)


def group(group_size: int):
    """Return a list of group ids, starting at 1 and padded to 4 digits"""
    return tuple(f'{x:04}' for x in range(1, group_size + 1))


def read_geom_csv(csvfile: str) -> pd.DataFrame:
    """
    Reads a CSV with WGS84 Point column 'WKT' and
    splits the point in columns WKT_X and WKT_Y.
    """
    data = pd.read_csv(csvfile)
    # Filter out empty points or multipoints
    data = data[data.WKT.str.contains("[0-9]+")]
    # And limit to 9999 entities
    data = data[:9999]
    # Turn coordinates into float
    data['POINT'] = point_to_tuple(data.WKT)
    data['WKT_X'] = tuple(item[0] for item in data.POINT)
    data['WKT_Y'] = tuple(item[1] for item in data.POINT)
    return data


async def read_lights(csvpuntosluz: str,
                      csvcm: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Read the list of lights and cabinets from csv exports of data.

    The cabinet dataframe is enhanced with the following properties:

    - group: id, from '0001' to the number of cabinets referenced
      by the streetlights df (other cabinets are dropped)
    - WKT_X, WKT_Y: coordinates of the cabinet.
    - lamparas_p: combined power of all the lamps in the cabinet.

    The streetlight dataframe is enhanced with the following fields:

    - group: group of the cabinet to where it belongs
    - WKT_X, WKT_Y: coordinates of the lamp.
    """
    streetlights = read_geom_csv(csvpuntosluz)
    cabinets = read_geom_csv(csvcm).set_index('id')
    # Filter out cabinets not referenced by streetligths
    referenced = streetlights.groupby('id_centro').agg({'lamparas_p': sum})
    cabinets = cabinets.join(referenced, how='inner')
    # Assign lights and group IDs
    cabinets = cabinets.assign(group=group(len(cabinets)))
    # Join cabinets to streetlights, so we get group ID
    streetlights = streetlights.join(cabinets,
                                     on='id_centro',
                                     lsuffix='_cabinet')
    return (cabinets, streetlights)


async def main():
    """Reads the CSV with light positions, and prints a sim config"""
    csvpuntosluz, csvcm = 'puntosluz.csv', 'cm.csv'
    tmplfile = 'streetlight.tmpl'
    env = jinja2.Environment(loader=jinja2.FileSystemLoader('.'))

    cabinets, streetlights = await read_lights(csvpuntosluz, csvcm)
    template = env.get_template(tmplfile)
    print(template.render(cabinets=cabinets, streetlights=streetlights))


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
