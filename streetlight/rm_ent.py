#!/usr/bin/env python3
"""
Remove all entities from a particular subservice.
Use with caution!
"""

from typing import Optional, Union, Any, Mapping, Sequence
import contextlib
import asyncio
import aiohttp
import pandas as pd
import click


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


class BrokerSession:
    """Encapsulates a session to the context broker"""

    # pylint: disable=too-many-arguments
    def __init__(self, auth_domain: str, cb_domain: str,
                 service: str, username: str, password: str):
        self._session: Union[None, aiohttp.ClientSession] = None
        self._headers = {'fiware-service': service}
        self._stack: Union[None, contextlib.AsyncExitStack] = None
        self._auth_domain = auth_domain
        self._cb_domain = cb_domain
        self._credentials = {
            'auth': {
                'identity': {
                    'methods': ['password'],
                    'password': {
                        'user': {
                            'domain': {
                                'name': service,
                            },
                            'name': username,
                            'password': password,
                        }
                    }
                }
            }
        }

    async def __aenter__(self):
        async with contextlib.AsyncExitStack() as stack:
            self._session = await stack.enter_async_context(aiohttp.ClientSession())
            self._headers['x-auth-token'] = await self._login()
            self._stack = stack.pop_all()
        return self

    # pylint: disable=invalid-name
    async def __aexit__(self, exc_type, exc, tb):
        if self._stack is not None:
            await self._stack.aclose()
        # Propagate the exception, if any, by returning None.

    async def _login(self) -> str:
        """Get auth token"""
        url = f'{self._auth_domain}/idm/auth/tokens'
        if self._session is None:
            return ''
        async with self._session.post(url, json=self._credentials) as reply:
            if reply.status != 201:
                raise FetchError(url, reply, meta=self._credentials)
            return reply.headers['x-subject-token']

    @staticmethod
    def _match(items: Sequence[Mapping[str, Any]],
               type_name: str) -> pd.DataFrame:
        """Return items that match the given type_name"""
        return pd.DataFrame((item for item in items
                             if item['type'] == type_name)).set_index('id')

    async def entities(self, subservice: str) -> Mapping[str, pd.DataFrame]:
        """ Collects entities of all types, indexed by entity id"""
        header = {'fiware-servicepath': subservice}
        header.update(self._headers)
        url = f'{self._cb_domain}/v2/entities'
        if self._session is None:
            return dict()
        async with self._session.get(url, headers=header) as reply:
            if reply.status != 200:
                raise FetchError(url, reply, meta=header)
            items = await reply.json()
        tnames = frozenset(item['type'] for item in items)
        return dict((tname, BrokerSession._match(items, tname)) for tname in tnames)

    async def _delete_entities(self, header: Mapping[str, str], entities: pd.DataFrame):
        if self._session is None:
            return 0
        totals = 0
        for entity in entities.index.to_list():
            url = f'{self._cb_domain}/v2/entities/{entity}'
            async with self._session.delete(url, headers=header) as reply:
                if reply.status != 204:
                    raise FetchError(url, reply, meta=header)
                totals += 1
        return totals

    async def delete(self, subservice: str, death_row: Sequence[str]):
        """Deletes a bunch of entities of the types set in death_row"""
        frames = await self.entities(subservice)
        purged = (frame for dtype, frame in frames.items() if dtype in death_row)
        header = {'fiware-servicepath': subservice}
        header.update(self._headers)
        return sum([await self._delete_entities(header, frame) for frame in purged])


# pylint: disable=too-many-arguments, line-too-long
@click.command()
@click.option('--auth-domain', default='https://iot.demo.urbo2.es', show_default=True, help='URL of the IdM')
@click.option('--cb-domain', default='http://iot.demo.urbo2.es:1026', show_default=True, help='URL of the CB')
@click.option('--service', default='murcia', show_default=True, help='service name')
@click.option('--subservice', default='/demo', show_default=True, help='service path')
@click.argument('user', required=True)
@click.option('--password', prompt=True, hide_input=True, help='user password')
def delete(auth_domain, cb_domain, service, subservice, user, password):
    """Delete entities from context broker"""
    death_row = ('Streetlight', 'StreetlightControlCabinet')
    async def do_delete():
        async with BrokerSession(auth_domain, cb_domain, service, user, password) as session:
            # Delete entities
            totals = 0
            while (purged := await session.delete(subservice, death_row)) > 0:
                click.echo(f'* Purged {purged} entities')
                totals += purged
            click.echo(f'Total {totals} entities purged')
    asyncio.get_event_loop().run_until_complete(do_delete())

if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter,unexpected-keyword-arg
    delete(auto_envvar_prefix='RM')
