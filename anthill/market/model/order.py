from tornado.ioloop import PeriodicCallback, IOLoop

from anthill.common.model import Model
from anthill.common.database import DatabaseError, format_conditions_json
from anthill.common.validate import validate
from anthill.common.database import format_conditions_json
from anthill.common.internal import Internal, InternalError
from anthill.common import to_int

from .item import ItemFromUserAdapter, ItemError

from datetime import datetime
import logging
import ujson


class OrderAdapter(object):
    def __init__(self, data):
        self.order_id = str(data.get("order_id"))
        self.owner_id = str(data.get("owner_id"))
        self.market_id = str(data.get("market_id"))
        self.give_item = str(data.get("order_give_item"))
        self.give_payload = data.get("order_give_payload")
        self.give_amount = str(data.get("order_give_amount"))
        self.available = str(data.get("order_available"))
        self.take_item = str(data.get("order_take_item"))
        self.take_payload = data.get("order_take_payload")
        self.take_amount = str(data.get("order_take_amount"))
        self.payload = data.get("order_payload")
        self.time = data.get("order_time")
        self.deadline = data.get("order_deadline")


class OrderError(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return self.message


class NoOrderError(Exception):
    pass


class OrderQueryError(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return str(self.code) + ": " + self.message


class OrderQuery(object):
    COMP_MORE = '>'
    COMP_LESS = '<'
    COMP_EQUAL = '='
    COMP_LESS_OR_EQUAL = '<='
    COMP_MORE_OR_EQUAL = '>='

    COMPARISONS = [COMP_MORE, COMP_LESS, COMP_EQUAL, COMP_LESS_OR_EQUAL, COMP_MORE_OR_EQUAL]

    def __init__(self, gamespace_id, db, market_id=None):
        self.gamespace_id = gamespace_id
        self.market_id = market_id
        self.db = db

        self.owner = None
        self.give_item = None
        self.give_payload = None
        self.give_amount = None
        self.give_amount_comparison = None
        self.take_item = None
        self.take_payload = None
        self.take_amount = None
        self.take_amount_comparison = None

        self.sort_by = None
        self.sort_desc = True

        self.offset = 0
        self.limit = 0

    def __values__(self):
        conditions = [
            "`gamespace_id`=%s",
            "`market_id`=%s"
        ]

        data = [
            str(self.gamespace_id),
            str(self.market_id),
        ]

        if self.owner:
            conditions.append("`owner_id`=%s")
            data.append(str(self.owner))

        if self.give_item:
            conditions.append("`order_give_item`=%s")
            data.append(str(self.give_item))

        if self.give_payload:
            for condition, values in format_conditions_json('order_give_payload', self.give_payload):
                conditions.append(condition)
                data.extend(values)

        if self.give_amount and self.give_amount_comparison:
            if self.give_amount_comparison in OrderQuery.COMPARISONS:
                conditions.append("`order_give_amount`{0}%s".format(self.give_amount_comparison))
                data.append(str(self.give_amount))

        if self.take_item:
            conditions.append("`order_take_item`=%s")
            data.append(str(self.take_item))

        if self.take_payload:
            for condition, values in format_conditions_json('order_take_payload', self.give_payload):
                conditions.append(condition)
                data.extend(values)

        if self.take_amount and self.take_amount_comparison:
            if self.take_amount_comparison in OrderQuery.COMPARISONS:
                conditions.append("`order_take_amount`{0}%s".format(self.take_amount_comparison))
                data.append(str(self.take_amount))

        return conditions, data

    async def query(self, one=False, count=False):
        conditions, data = self.__values__()

        query = """
            SELECT {0} * FROM `orders`
            WHERE {1}
        """.format(
            "SQL_CALC_FOUND_ROWS" if count else "",
            " AND ".join(conditions))

        if self.sort_by in ["take_amount", "give_amount"]:
            sort = "`order_{0}` {1}, ".format(self.sort_by, "DESC" if self.sort_desc else "ASC")
        else:
            sort = ""

        query += """
            ORDER BY {0}`order_time` DESC
        """.format(sort)

        if self.limit:
            query += """
                LIMIT %s,%s
            """
            data.append(int(self.offset))
            data.append(int(self.limit))

        query += ";"

        if one:
            try:
                result = await self.db.get(query, *data)
            except DatabaseError as e:
                raise OrderQueryError(500, "Failed to get message: " + e.args[1])

            if not result:
                return None

            return OrderAdapter(result)
        else:
            try:
                result = await self.db.query(query, *data)
            except DatabaseError as e:
                raise OrderQueryError(500, "Failed to query messages: " + e.args[1])

            count_result = 0

            if count:
                count_result = await self.db.get(
                    """
                        SELECT FOUND_ROWS() AS count;
                    """)
                count_result = count_result["count"]

            items = map(OrderAdapter, result)

            if count:
                return (items, count_result)

            return items


class OrderModel(Model):

    ORDER_COMPLETED = "order_completed"
    ORDER_CANCELLED = "order_cancelled"

    def __init__(self, app, db):
        self.app = app
        self.db = db
        self.internal = Internal()
        self.check_cb = PeriodicCallback(self.__check_due_orders__, callback_time=60000)

    async def started(self, application):
        await super().started(application)
        self.check_cb.start()

    async def stopped(self):
        self.check_cb.stop()
        await super().stopped()

    def get_setup_tables(self):
        return ["orders"]

    def get_setup_db(self):
        return self.db

    def has_delete_account_event(self):
        return True

    async def __send_message__(self, gamespace_id, recipient_class, recipient_key,
                               account_id, message_type, payload):
        try:
            await self.internal.request(
                "message", "send_message",
                gamespace=gamespace_id, sender=account_id,
                recipient_class=recipient_class, recipient_key=recipient_key,
                message_type=message_type, payload=payload, flags=['remove_delivered'],
                authoritative=True)
        except InternalError:
            logging.exception("Could not deliver a message: {0}".format(message_type))

    async def accounts_deleted(self, gamespace, accounts, gamespace_only):
        try:
            if gamespace_only:
                await self.db.execute(
                    """
                        DELETE FROM `orders`
                        WHERE `gamespace_id`=%s AND `owner_id` IN %s;
                    """, gamespace, accounts)
            else:
                await self.db.execute(
                    """
                        DELETE FROM `orders`
                        WHERE `owner_id` IN %s;
                    """, accounts)
        except DatabaseError as e:
            raise OrderError(500, "Failed to delete user orders: " + e.args[1])

    @validate(gamespace_id="int", order_id="int")
    async def get_order(self, gamespace_id, order_id, db=None):
        try:
            data = await (db or self.db).get(
                """
                    SELECT *
                    FROM `orders`
                    WHERE `order_id`=%s AND `gamespace_id`=%s;
                """, order_id, gamespace_id
            )
        except DatabaseError as e:
            raise OrderError(500, "Failed to gather order info: " + e.args[1])

        if not data:
            raise NoOrderError()

        return OrderAdapter(data)

    def __check_due_orders__(self):
        IOLoop.current().add_callback(self.delete_due_orders)

    async def delete_due_orders(self):

        logging.info("Deleting due orders ...")

        try:
            orders = await self.db.query(
                """
                    SELECT `order_id`, `gamespace_id`
                    FROM `orders`
                    WHERE NOW()>`order_deadline`;
                """)
        except DatabaseError:
            logging.exception("Cannot delete due orders")
            return

        for order in orders:
            order_id = order["order_id"]
            gamespace_id = order["gamespace_id"]

            try:
                await self.delete_order(gamespace_id, order_id)
            except NoOrderError:
                pass
            except OrderError:
                logging.exception("Cannot delete due order {0}/{1}".format(gamespace_id, order_id))
            except ItemError:
                logging.exception("Cannot delete due order {0}/{1}".format(gamespace_id, order_id))
            else:
                logging.info("Deleted due order: {0}/{1}".format(gamespace_id, order_id))

        logging.info("Deleting done.")

    @validate(gamespace_id="int", order_id="int")
    async def delete_order(self, gamespace_id, order_id):
        try:
            async with self.db.acquire(auto_commit=False) as db:
                order = await db.get(
                    """
                        SELECT *
                        FROM `orders`
                        WHERE `order_id`=%s AND `gamespace_id`=%s
                        FOR UPDATE;
                    """, order_id, gamespace_id
                )

                if not order:
                    raise NoOrderError()

                order = OrderAdapter(order)

                await self.app.items.update_item(
                    gamespace_id, order.owner_id, order.market_id,
                    order.give_item, int(order.give_amount) * int(order.available), order.give_payload,
                    db=db)

                await db.execute(
                    """
                        DELETE
                        FROM `orders`
                        WHERE `order_id`=%s AND `gamespace_id`=%s;
                    """, order_id, gamespace_id)

                await db.commit()
        except DatabaseError as e:
            raise OrderError(500, "Failed to gather order info: " + e.args[1])
        else:
            await self.__order_cancelled__(gamespace_id, order.market_id, order)

    def orders_query(self, gamespace, marker_id=None):
        return OrderQuery(gamespace, self.db, marker_id)

    async def __order_completed__(self, gamespace_id, market_id, order,
                                  give_amount, complete_amount, left_amount):
        logging.info("Order completed {0} time(s): {1}".format(complete_amount, order.order_id))
        await self.__send_message__(
            gamespace_id, "user", str(order.owner_id), str(order.owner_id), OrderModel.ORDER_COMPLETED, {
                "order_id": order.order_id,
                "give_item": order.give_item,
                "give_amount": int(give_amount),
                "give_payload": order.give_payload,
                "take_item": order.take_item,
                "take_amount": int(order.take_amount),
                "take_payload": order.take_payload,
                "amount_completed": int(complete_amount),
                "amount_left": int(left_amount),
                "payload": order.payload
            })

    async def __order_cancelled__(self, gamespace_id, market_id, order):
        logging.info("Order cancelled: {0}".format(order.order_id))
        await self.__send_message__(
            gamespace_id, "user", str(order.owner_id), str(order.owner_id), OrderModel.ORDER_CANCELLED, {
                "order_id": order.order_id,
                "give_item": order.give_item,
                "give_amount": int(order.give_amount),
                "give_payload": order.give_payload,
                "take_item": order.take_item,
                "take_amount": int(order.take_amount),
                "take_payload": order.take_payload,
                "were_available": int(order.available),
                "payload": order.payload
            })

    @validate(order_id="int", gamespace_id="int", owner_id="int", market_id="int")
    async def fulfill_order(self, order_id, gamespace_id, owner_id, market_id):

        items = self.app.items
        transactions = self.app.transactions

        async with (self.db.acquire(auto_commit=False)) as db:
            item_to_fulfill_data = await db.get(
                """
                SELECT * FROM `orders`
                WHERE gamespace_id=%s AND market_id=%s AND owner_id=%s AND `order_available`!=0
                FOR UPDATE;
                """, gamespace_id, market_id, owner_id)

            if item_to_fulfill_data is None:
                return

            fulfill = OrderAdapter(item_to_fulfill_data)

            logging.info(
                "Matching orders: gc {0} ac {1} mk {2} give item {3} ({5}) give {7} "
                "take item {4} ({6}) take {8} amount of orders {9}".format(
                    gamespace_id, owner_id, market_id, fulfill.give_item, fulfill.take_item,
                    ujson.dumps(fulfill.give_payload), ujson.dumps(fulfill.take_payload),
                    fulfill.give_amount, fulfill.take_amount, fulfill.available))

            orders_to_fulfill = int(fulfill.available)
            backup = 0

            completed_orders = []

            matching_orders = await db.query(
                """
                SELECT * 
                FROM `orders`
                WHERE `gamespace_id`=%s AND `market_id`=%s 
                AND `order_take_item`=%s AND `order_give_item`=%s 
                AND JSON_CONTAINS(%s, `order_take_payload`) AND JSON_CONTAINS(`order_give_payload`, %s)
                AND %s>=`order_take_amount` AND `order_give_amount`>=%s AND `owner_id`!=%s
                ORDER BY `order_take_amount`, `order_give_amount`, `order_time` DESC
                FOR UPDATE;
                """, gamespace_id, market_id, fulfill.give_item, fulfill.take_item,
                ujson.dumps(fulfill.give_payload), ujson.dumps(fulfill.take_payload),
                fulfill.give_amount, fulfill.take_amount, owner_id)

            for matched in map(OrderAdapter, matching_orders):
                price_difference = int(fulfill.give_amount) - int(matched.take_amount)

                if int(matched.available) >= orders_to_fulfill:
                    fulfill_amount = orders_to_fulfill
                    updated_amount = int(matched.available) - orders_to_fulfill
                else:
                    fulfill_amount = int(matched.available)
                    updated_amount = 0

                backup += price_difference * fulfill_amount
                orders_to_fulfill -= fulfill_amount

                logging.info(
                    "Order matched: id {8} ac {0} give item {1} ({3}) give {5} take item {2} ({4}) take {6} "
                    "amount of orders {7}".format(
                        matched.owner_id, matched.give_item, matched.take_item,
                        matched.give_payload, matched.take_payload, matched.give_amount, matched.take_amount,
                        matched.available, matched.order_id))

                logging.info("Giving {1} items to the matched seller: {0}".format(
                    fulfill.give_item, int(fulfill_amount) * int(matched.take_amount)))

                completed_orders.append(
                    (matched, fulfill.take_amount, fulfill_amount,  int(matched.available) - fulfill_amount))

                await transactions.new_transaction(
                    gamespace_id, market_id, fulfill.give_item, fulfill.give_payload, int(matched.take_amount),
                    fulfill.owner_id, matched.give_item, matched.give_payload, int(fulfill.take_amount),
                    matched.owner_id, int(fulfill_amount), db=db)

                await items.update_item(
                    gamespace_id, matched.owner_id, market_id, fulfill.give_item,
                    fulfill_amount * int(matched.take_amount),
                    fulfill.give_payload, db=db)

                logging.info("Giving {1} items to the original seller: {0}".format(
                    matched.give_item, int(fulfill_amount) * int(fulfill.take_amount)))

                completed_orders.append(
                    (fulfill, matched.take_amount, fulfill_amount, int(fulfill.available) - fulfill_amount))

                await items.update_item(
                    gamespace_id, fulfill.owner_id, market_id, matched.give_item,
                    fulfill_amount * int(fulfill.take_amount),
                    matched.give_payload, db=db)

                matched_price_difference = int(matched.give_amount) - int(fulfill.take_amount)

                matched_backup = matched_price_difference * fulfill_amount

                if matched_backup > 0:
                    logging.info("Giving {1} items back to the original seller: {0}".format(
                        fulfill.take_item, matched_backup))

                    await items.update_item(
                        gamespace_id, matched.owner_id, market_id, matched.give_item,
                        matched_backup,
                        matched.give_payload, db=db)

                if updated_amount == 0:
                    logging.info("Deleted order: {0}".format(matched.order_id))
                    await db.execute(
                        """
                        DELETE FROM `orders`
                        WHERE `order_id`=%s;
                        """, matched.order_id)
                else:
                    logging.info("Updated order {0} availability to: {1}".format(matched.order_id, updated_amount))
                    await db.execute(
                        """
                        UPDATE `orders`
                        SET `order_available`=%s
                        WHERE `order_id`=%s;
                        """, updated_amount, matched.order_id)

                if orders_to_fulfill <= 0:
                    logging.info("Order has been fulfilled, skipping matching")
                    break

            if orders_to_fulfill == 0:
                logging.info("Deleted original order: {0}".format(order_id))
                await db.execute(
                    """
                    DELETE FROM `orders`
                    WHERE `order_id`=%s;
                    """, order_id)
            else:
                if orders_to_fulfill != int(fulfill.available):
                    logging.info("Updated original order {0} availability to: {1}".format(order_id, orders_to_fulfill))
                    await db.execute(
                        """
                        UPDATE `orders`
                        SET `order_available`=%s
                        WHERE `order_id`=%s;
                        """, orders_to_fulfill, order_id)

            if backup > 0:
                logging.info("Giving items back: {0} of {1} ({2})".format(
                    backup, fulfill.give_item, ujson.dumps(fulfill.give_payload)))

                await items.update_item(
                    gamespace_id, owner_id, market_id, fulfill.give_item,
                    backup, fulfill.give_payload, db=db)

            logging.info("Matching complete")
            await db.commit()

            for completed, g_amount, amount, left in completed_orders:
                await self.__order_completed__(gamespace_id, market_id, completed, g_amount, amount, left)

            return orders_to_fulfill == 0

    @validate(order_id="int", gamespace_id="int", fulfill_account="int", market_id="int", orders_amount="int")
    async def fulfill_order_with_account(self, order_id, gamespace_id, fulfill_account, market_id, orders_amount):
        items = self.app.items
        transactions = self.app.transactions

        async with (self.db.acquire(auto_commit=False)) as db:
            item_to_fulfill_data = await db.get(
                """
                SELECT * FROM `orders`
                WHERE gamespace_id=%s AND order_id=%s AND market_id=%s AND `order_available`>=%s AND `owner_id`!=%s
                FOR UPDATE;
                """, gamespace_id, order_id, market_id, orders_amount, fulfill_account)

            if item_to_fulfill_data is None:
                return None

            order = OrderAdapter(item_to_fulfill_data)

            logging.info(
                "Fulfilling an order: gc {0} for {1} mk {2} give item {3} ({5}) give {7} "
                "take item {4} ({6}) take {8} amount of orders {9}".format(
                    gamespace_id, fulfill_account, market_id, order.give_item, order.take_item,
                    ujson.dumps(order.give_payload), ujson.dumps(order.take_payload),
                    order.give_amount, order.take_amount, order.available))

            items_needed = int(order.take_amount) * int(orders_amount)
            items_given = int(order.give_amount) * int(orders_amount)

            logging.info("Taking {1} items from the fulfiller: {0}".format(
                order.take_item, items_needed))

            decreased = await items.subtract_item(
                gamespace_id, fulfill_account, market_id, order.take_item,
                items_needed, order.take_payload, db=db)

            if not decreased:
                logging.info("Not enough items, aborting")
                return None

            logging.info("Giving {1} items to the original seller: {0}".format(
                order.take_item, items_needed))

            await items.update_item(
                gamespace_id, order.owner_id, market_id, order.take_item,
                items_needed,
                order.take_payload, db=db)

            logging.info("Giving {1} items to the fulfiller: {0}".format(
                order.give_item, items_given))

            await items.update_item(
                gamespace_id, fulfill_account, market_id, order.give_item,
                items_given,
                order.give_payload, db=db)

            await transactions.new_transaction(
                gamespace_id, market_id, order.give_item, order.give_payload, int(order.give_amount),
                order.owner_id, order.take_item, order.take_payload, int(order.take_amount),
                fulfill_account, int(orders_amount), db=db)

            orders_left = int(order.available) - int(orders_amount)

            if orders_left > 0:
                logging.info("Updated original order {0} availability to: {1}".format(order_id, orders_left))
                await db.execute(
                    """
                    UPDATE `orders`
                    SET `order_available`=%s
                    WHERE `order_id`=%s;
                    """, orders_left, order_id)
            else:
                logging.info("Deleted original order: {0}".format(order_id))
                await db.execute(
                    """
                    DELETE FROM `orders`
                    WHERE `order_id`=%s;
                    """, order_id)

            await db.commit()
            logging.info("Fulfillment complete")

            await self.__order_completed__(
                gamespace_id, market_id, order, order.give_amount,
                int(orders_amount), orders_left)

            return orders_left <= 0

    @validate(gamespace_id="int", order_id="int", market_id="int", order_give_item="str_name",
              order_give_payload="json", order_give_amount="int", order_take_item="str_name", order_take_payload="json",
              order_take_amount="int", order_available="int", order_payload="json", order_deadline="datetime")
    async def new_order(self, gamespace_id, owner_id, market_id, order_give_item, order_give_payload, order_give_amount,
                        order_take_item, order_take_payload, order_take_amount,
                        order_available, order_payload, order_deadline, subtract_items=False):

        if order_deadline < datetime.utcnow():
            raise OrderError(400, "Order's deadline cannot be set for the past")

        if order_take_amount <= 0 or order_give_amount <= 0 or order_available <= 0:
            raise OrderError(400, "Bad order amounts")

        try:
            async with self.db.acquire(auto_commit=False) as db:
                if subtract_items:
                    if not await self.app.items.subtract_item(
                            gamespace_id, owner_id, market_id,
                            order_give_item, int(order_give_amount) * int(order_available),
                            order_give_payload, db=db):
                        raise OrderError(409, "Not enough items to generate an order")

                # only create order after all of the items have been successfully subtracted
                order_id = await db.insert(
                    """
                        INSERT INTO `orders` 
                        (gamespace_id, owner_id, market_id, order_give_item, order_give_payload, order_give_amount, 
                            order_take_item, order_take_payload, order_take_amount, order_available, order_payload, 
                            order_deadline)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """, gamespace_id, owner_id, market_id, order_give_item, ujson.dumps(order_give_payload),
                    order_give_amount, order_take_item, ujson.dumps(order_take_payload), order_take_amount,
                    order_available, ujson.dumps(order_payload), order_deadline)

                # commit both the subtraction and the order at the same time
                await db.commit()
        except DatabaseError as e:
            raise OrderError(500, "Failed to gather order info: " + e.args[1])

        logging.info(
            "User {0} gc {1} mk {2} created an {3} order(s) to sell {4} of {5}({6}) and "
            "buy {7} of {8}({9})".format(
                owner_id, gamespace_id, market_id, order_available,
                order_give_amount, order_give_item,
                ujson.dumps(order_give_payload), order_take_amount, order_take_item,
                ujson.dumps(order_take_payload)))

        return order_id

    @validate(gamespace_id="int", owner_id="int", market_id="int", order_id="int", order_give_item="str_name",
              order_give_payload="json", order_give_amount="int", order_take_item="str_name",
              order_take_payload="json", order_take_amount="int",
              order_available="int", order_payload="json", order_deadline="datetime")
    async def update_order(self, gamespace_id, owner_id, market_id, order_id,
                           order_give_item, order_give_payload, order_give_amount,
                           order_take_item, order_take_payload, order_take_amount,
                           order_available, order_payload, order_deadline, db=None):
        try:
            await (db or self.db).execute(
                """
                    UPDATE `orders` 
                    SET order_give_item=%s, order_give_payload=%s, order_give_amount=%s,
                    order_take_item=%s, order_take_payload=%s, order_take_amount=%s,
                    order_available=%s, order_payload=%s, order_deadline=%s
                    WHERE order_id=%s AND gamespace_id=%s AND `owner_id`=%s AND `market_id`=%s;
                """, order_give_item, ujson.dumps(order_give_payload), order_give_amount,
                order_take_item, ujson.dumps(order_take_payload), order_take_amount,
                order_available, ujson.dumps(order_payload), order_deadline, order_id,
                gamespace_id, owner_id, market_id
            )
        except DatabaseError as e:
            raise OrderError(500, "Failed to gather order info: " + e.args[1])
