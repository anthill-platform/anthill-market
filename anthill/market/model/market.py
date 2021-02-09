
from tornado.ioloop import PeriodicCallback

from anthill.common.model import Model
from anthill.common.database import DatabaseError, format_conditions_json
from anthill.common.validate import validate
from anthill.common import to_int


import hashlib
import logging
import ujson


class MarketAdapter(object):
    def __init__(self, data):
        self.market_id = str(data.get("market_id"))
        self.name = str(data.get("market_name"))
        self.settings = data.get("market_settings")


class MarketError(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return self.message


class NoMarketError(Exception):
    pass


class MarketModel(Model):

    def __init__(self, app, db):
        self.app = app
        self.db = db

    def get_setup_tables(self):
        return ["markets"]

    def get_setup_db(self):
        return self.db

    def has_delete_account_event(self):
        return False

    @validate(gamespace_id="int", market_name="str_name")
    async def find_market(self, gamespace_id, market_name, db=None):
        try:
            data = await (db or self.db).get(
                """
                    SELECT *
                    FROM `markets`
                    WHERE `market_name`=%s AND `gamespace_id`=%s;
                """, market_name, gamespace_id
            )
        except DatabaseError as e:
            raise MarketError(500, "Failed to gather order info: " + e.args[1])

        if not data:
            raise NoMarketError()

        return MarketAdapter(data)

    @validate(gamespace_id="int", market_id="int")
    async def get_market(self, gamespace_id, market_id, db=None):
        try:
            data = await (db or self.db).get(
                """
                    SELECT *
                    FROM `markets`
                    WHERE `market_id`=%s AND `gamespace_id`=%s;
                """, market_id, gamespace_id
            )
        except DatabaseError as e:
            raise MarketError(500, "Failed to gather order info: " + e.args[1])

        if not data:
            raise NoMarketError()

        return MarketAdapter(data)

    @validate(gamespace_id="int", market_name="str_name", market_settings="json_dict")
    async def new_market(self, gamespace_id, market_name, market_settings, db=None):
        try:
            market_id = await (db or self.db).insert(
                """
                    INSERT INTO `markets`
                    (gamespace_id, market_name, market_settings) 
                    VALUES (%s, %s, %s);
                """, gamespace_id, market_name, ujson.dumps(market_settings)
            )
        except DatabaseError as e:
            raise MarketError(500, "Failed to gather market info: " + e.args[1])

        return str(market_id)

    @validate(gamespace_id="int", market_id="int", market_name="str_name", market_settings="json_dict")
    async def update_market(self, gamespace_id, market_id, market_name, market_settings, db=None):

        try:
            await (db or self.db).execute(
                """
                    UPDATE `markets`
                    SET `market_name` = %s, `market_settings` = %s
                    WHERE `gamespace_id` = %s AND `market_id` = %s;
                """, market_name, ujson.dumps(market_settings), gamespace_id, market_id
            )
        except DatabaseError as e:
            raise MarketError(500, "Failed to gather market info: " + e.args[1])

    @validate(gamespace_id="int", market_id="int")
    async def delete_market(self, gamespace_id, market_id):

        async with self.db.acquire(auto_commit=False) as db:
            try:
                await db.execute(
                    """
                        DELETE FROM `markets`
                        WHERE `gamespace_id` = %s AND `market_id` = %s;
                    """, gamespace_id, market_id
                )
                await db.execute(
                    """
                        DELETE FROM `orders`
                        WHERE `gamespace_id` = %s AND `market_id` = %s;
                    """, gamespace_id, market_id
                )
                await db.execute(
                    """
                        DELETE FROM `items`
                        WHERE `gamespace_id` = %s AND `market_id` = %s;
                    """, gamespace_id, market_id
                )
            except DatabaseError as e:
                await db.rollback()
                raise MarketError(500, "Failed to delete market: " + e.args[1])
            else:
                await db.commit()

    @validate(gamespace_id="int")
    async def list_markets(self, gamespace_id, db=None):
        try:
            data = await (db or self.db).query(
                """
                    SELECT *
                    FROM `markets`
                    WHERE `gamespace_id`=%s;
                """, gamespace_id
            )
        except DatabaseError as e:
            raise MarketError(500, "Failed to gather order info: " + e.args[1])

        return map(MarketAdapter, data)
