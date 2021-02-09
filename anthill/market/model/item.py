
from tornado.ioloop import PeriodicCallback

from anthill.common.model import Model
from anthill.common.database import DatabaseError, format_conditions_json
from anthill.common.validate import validate, validate_value, ValidationError
from anthill.common import to_int


import hashlib
import logging
import ujson


class ItemAdapter(object):
    def __init__(self, data):
        self.item_id = str(data.get("item_id"))
        self.owner_id = str(data.get("owner_id"))
        self.market_id = str(data.get("market_id"))
        self.name = str(data.get("item_name"))
        self.amount = data.get("item_amount")
        self.payload = data.get("item_payload")
        self.hash = str(data.get("item_hash"))


class ItemFromUserAdapter(object):
    def __init__(self, data):
        try:
            self.name = validate_value(data["name"], "str_name")
            self.update_amount = validate_value(data["amount"], "int")
        except (ValidationError, KeyError):
            raise ItemError(400, "Item's field 'amount' or 'name' is missing or malformed")
        try:
            self.payload = validate_value(data.get("payload", {}), "json_dict")
        except ValidationError:
            raise ItemError(400, "Item {0}'s field 'payload' is malformed".format(self.name))


class ItemError(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return self.message


class NoItemError(Exception):
    pass


class ItemModel(Model):

    def __init__(self, app, db):
        self.app = app
        self.db = db

    def get_setup_tables(self):
        return ["items"]

    def get_setup_events(self):
        return ["delete_zero_items"]

    def get_setup_db(self):
        return self.db

    def has_delete_account_event(self):
        return True

    async def accounts_deleted(self, gamespace, accounts, gamespace_only):
        try:
            if gamespace_only:
                await self.db.execute(
                    """
                        DELETE FROM `items`
                        WHERE `gamespace_id`=%s AND `owner_id` IN %s;
                    """, gamespace, accounts)
            else:
                await self.db.execute(
                    """
                        DELETE FROM `items`
                        WHERE `owner_id` IN %s;
                    """, accounts)
        except DatabaseError as e:
            raise ItemError(500, "Failed to delete user orders: " + e.args[1])

    @validate(gamespace_id="int", item_id="int")
    async def get_item(self, gamespace_id, item_id, db=None):
        try:
            data = await (db or self.db).get(
                """
                    SELECT *
                    FROM `items`
                    WHERE `item_id`=%s AND `gamespace_id`=%s;
                """, item_id, gamespace_id
            )
        except DatabaseError as e:
            raise ItemError(500, "Failed to gather order info: " + e.args[1])

        if not data:
            raise NoItemError()

        return ItemAdapter(data)

    @validate(gamespace_id="int", owner_id="int", market_id="int")
    async def list_items(self, gamespace_id, owner_id, market_id, db=None):
        try:
            data = await (db or self.db).query(
                """
                    SELECT *
                    FROM `items`
                    WHERE `owner_id`=%s AND `gamespace_id`=%s AND `market_id`=%s AND `item_amount` != 0;
                """, owner_id, gamespace_id, market_id
            )
        except DatabaseError as e:
            raise ItemError(500, "Failed to gather order info: " + e.args[1])

        return map(ItemAdapter, data)

    @validate(gamespace_id="int", owner_id="int", market_id="int", item_name="str_name", item_payload="json_dict")
    async def find_item(self, gamespace_id, owner_id, market_id, item_name, item_payload, db=None):

        item_hash = ItemModel.item_hash(item_name, item_payload or {})

        try:
            data = await (db or self.db).get(
                """
                    SELECT *
                    FROM `items`
                    WHERE `owner_id`=%s AND `gamespace_id`=%s AND `market_id`=%s AND `item_hash`=%s
                    LIMIT 1;
                """, owner_id, gamespace_id, market_id, item_hash
            )
        except DatabaseError as e:
            raise ItemError(500, "Failed to gather order info: " + e.args[1])

        if not data:
            raise NoItemError()

        return ItemAdapter(data)

    @staticmethod
    def item_hash(name, payload):
        sha = hashlib.sha256(name.encode('utf8'))
        sha.update(ujson.dumps(payload, sort_keys=True).encode('utf8'))
        return sha.hexdigest()

    @validate(gamespace_id="int", owner_id="int", market_id="int", item_name="str_name",
              item_amount="int", item_payload="json")
    async def subtract_item(self, gamespace_id, owner_id, market_id, item_name, item_amount, item_payload, db=None):

        item_hash = ItemModel.item_hash(item_name, item_payload or {})

        try:
            updated = await(db or self.db).execute(
                """
                    UPDATE `items`
                    SET `item_amount` = `item_amount` - %s
                    WHERE `gamespace_id`=%s AND `owner_id`=%s AND `market_id`=%s AND `item_hash`=%s
                    AND `item_amount` >= %s;

                """, item_amount, gamespace_id, owner_id, market_id, item_hash, item_amount
            )
        except DatabaseError as e:
            raise ItemError(500, "Failed to decrease item amount: " + e.args[1])

        if updated:
            logging.info("User {0} gc {1} mk {2} subtracted {3} of {4}({5})".format(
                owner_id, gamespace_id, market_id, item_amount, item_name, ujson.dumps(item_payload)))
        else:
            logging.info("User {0} gc {1} mk {2} failed to subtract {3} of {4}({5})".format(
                owner_id, gamespace_id, market_id, item_amount, item_name, ujson.dumps(item_payload)))

        return updated

    @validate(gamespace_id="int", owner_id="int", market_id="int", item_name="str_name",
              item_amount="int", item_payload="json")
    async def update_item(self, gamespace_id, owner_id, market_id, item_name, item_amount, item_payload, db=None):

        item_hash = ItemModel.item_hash(item_name, item_payload or {})

        try:
            await(db or self.db).execute(
                """
                    INSERT INTO `items`
                    (`gamespace_id`, `owner_id`, `market_id`, `item_name`, `item_amount`, `item_payload`, `item_hash`) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE `item_amount` = `item_amount` + %s;
                    
                """, gamespace_id, owner_id, market_id, item_name, item_amount, ujson.dumps(item_payload or {}),
                item_hash, item_amount
            )
        except DatabaseError as e:
            raise ItemError(500, "Failed to update item amount: " + e.args[1])
        else:
            logging.info("User {0} gc {1} mk {2} updated {3} of {4}({5})".format(
                owner_id, gamespace_id, market_id, item_amount, item_name, ujson.dumps(item_payload)))

    @validate(gamespace_id="int", owner_id="int", market_id="int", items="json_list")
    async def update_items(self, gamespace_id, owner_id, market_id, items):
        async with self.db.acquire(auto_commit=False) as db:
            try:
                items = list(map(ItemFromUserAdapter, items))
                hashes = set()
                for item in items:
                    hashes.add(ItemModel.item_hash(item.name, item.payload or {}))

                existing_hashed_items = {}
                try:
                    hashed_items = await db.query(
                        """
                            SELECT *
                            FROM `items`
                            WHERE `owner_id`=%s AND `gamespace_id`=%s AND `market_id`=%s AND `item_hash` IN %s
                            FOR UPDATE;
                        """, owner_id, gamespace_id, market_id, list(hashes)
                    )
                except DatabaseError as e:
                    raise ItemError(500, "Failed to obtain items: " + e.args[1])

                for hh in hashed_items:
                    item_a = ItemAdapter(hh)
                    existing_hashed_items[item_a.hash] = item_a

                # check negative balances first
                for item in items:
                    if item.update_amount < 0:
                        item_hash = ItemModel.item_hash(item.name, item.payload or {})
                        if item_hash not in existing_hashed_items:
                            raise ItemError(409, "Not enough items '{0}".format(item.name))
                        if existing_hashed_items[item_hash].amount < -item.update_amount:
                            raise ItemError(409, "Not enough items '{0}".format(item.name))

                for item in items:
                    if item.update_amount < 0:
                        if not await self.subtract_item(
                                gamespace_id, owner_id, market_id, item.name,
                                -item.update_amount, item.payload, db=db):
                            raise ItemError(409, "Not enough items '{0}".format(item.name))
                    elif item.update_amount > 0:
                        await self.update_item(
                            gamespace_id, owner_id, market_id,
                            item.name, item.update_amount, item.payload, db=db)

            except Exception:
                await db.rollback()
                raise
            else:
                await db.commit()
