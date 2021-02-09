
from tornado.ioloop import PeriodicCallback

from anthill.common.model import Model
from anthill.common.database import DatabaseError, format_conditions_json
from anthill.common.validate import validate
from anthill.common import to_int

from .item import ItemModel

import hashlib
import logging
import ujson


class TransactionAdapter(object):
    def __init__(self, data):
        self.transaction_id = str(data.get("transaction_id"))
        self.market_id = str(data.get("market_id"))
        self.give_item = str(data.get("transaction_give_item"))
        self.give_payload = data.get("transaction_give_payload")
        self.give_hash = str(data.get("transaction_give_hash"))
        self.give_amount = str(data.get("transaction_give_amount"))
        self.give_owner = str(data.get("transaction_give_owner"))
        self.amount = str(data.get("transaction_amount"))
        self.take_item = str(data.get("transaction_take_item"))
        self.take_payload = data.get("transaction_take_payload")
        self.take_hash = str(data.get("transaction_take_hash"))
        self.take_amount = str(data.get("transaction_take_amount"))
        self.take_owner = str(data.get("transaction_take_owner"))
        self.date = data.get("transaction_date")


class TransactionError(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return self.message


class NoTransactionError(Exception):
    pass


class TransactionModel(Model):

    def __init__(self, app, db):
        self.app = app
        self.db = db

    def get_setup_tables(self):
        return ["transactions"]

    def get_setup_db(self):
        return self.db

    def has_delete_account_event(self):
        return False

    @validate(gamespace_id="int", market_id="int", transaction_id="int")
    async def get_transaction(self, gamespace_id, market_id, transaction_id, db=None):
        try:
            data = await (db or self.db).get(
                """
                    SELECT *
                    FROM `transactions`
                    WHERE `market_id`=%s AND `gamespace_id`=%s AND `transaction_id`=%s;
                """, market_id, gamespace_id, transaction_id
            )
        except DatabaseError as e:
            raise TransactionError(500, "Failed to gather order info: " + e.args[1])

        if not data:
            raise NoTransactionError()

        return TransactionAdapter(data)

    @validate(gamespace_id="int", market_id="int", give_item="str_name", give_payload="json_dict",
              give_amount="int", give_owner="int", take_item="str_name", take_payload="json_dict",
              take_amount="int", take_owner="int", amount="int")
    async def new_transaction(self, gamespace_id, market_id, give_item, give_payload, give_amount, give_owner,
                              take_item, take_payload, take_amount, take_owner, amount, db=None):

        give_hash = ItemModel.item_hash(give_item, give_payload or {})
        take_hash = ItemModel.item_hash(take_item, take_payload or {})

        give = (give_item, give_payload, give_hash, give_amount, give_owner)
        take = (take_item, take_payload, take_hash, take_amount, take_owner)

        if give_hash > take_hash:
            a = give
            b = take
        else:
            a = take
            b = give

        try:
            transaction_id = await (db or self.db).insert(
                """
                    INSERT INTO `transactions`
                    (gamespace_id, market_id, transaction_give_item, transaction_give_payload, transaction_give_hash,
                    transaction_give_amount, transaction_give_owner, transaction_amount,
                    transaction_take_item, transaction_take_payload, transaction_take_hash, 
                    transaction_take_amount, transaction_take_owner)                    
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, gamespace_id, market_id, a[0], ujson.dumps(a[1]), a[2], a[3],
                a[4], amount, b[0], ujson.dumps(b[1]), b[2], b[3], b[4]
            )
        except DatabaseError as e:
            raise TransactionError(500, "Failed to gather market info: " + e.args[1])

        return str(transaction_id)

    @validate(gamespace_id="int", market_id="int", give_item="str_name", give_payload="json_dict",
              take_item="str_name", take_payload="json_dict", limit="int")
    async def list_transaction(self, gamespace_id, market_id, give_item,
                               give_payload, take_item, take_payload, limit=100, db=None):

        give_hash = ItemModel.item_hash(give_item, give_payload or {})
        take_hash = ItemModel.item_hash(take_item, take_payload or {})

        if give_hash > take_hash:
            a = give_hash
            b = take_hash
        else:
            a = take_hash
            b = give_hash

        if limit <= 0 or limit > 100:
            raise TransactionError(400, "Bad limit")

        try:
            data = await (db or self.db).query(
                """
                    SELECT DATE(`transaction_date`) as date, AVG(`transaction_give_amount`) as give_amount, 
                    AVG(`transaction_take_amount`) as take_amount, SUM(`transaction_amount`) as amount
                    FROM `transactions`
                    WHERE `gamespace_id`=%s AND `market_id`=%s AND `transaction_give_hash`=%s AND
                    `transaction_take_hash`=%s
                    GROUP BY DATE(`transaction_date`)
                    ORDER BY `date` DESC
                    LIMIT %s;
                """, gamespace_id, market_id, a, b, limit
            )
        except DatabaseError as e:
            raise TransactionError(500, "Failed to gather transaction info: " + e.args[1])

        return map(TransactionAdapter, data)
