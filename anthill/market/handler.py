
from tornado.web import HTTPError

from anthill.common.access import scoped, AccessToken, remote_ip
from anthill.common.handler import AuthenticatedHandler, AnthillRequestHandler
from anthill.common.validate import ValidationError, validate, validate_value

from . model.item import NoItemError, ItemError
from . model.market import NoMarketError, MarketError
from . model.order import NoOrderError, OrderError
import logging


class MarketHandler(AuthenticatedHandler):
    async def get_market(self, market_name):
        try:
            return await self.application.markets.find_market(self.token.get(AccessToken.GAMESPACE), market_name)
        except NoMarketError:
            raise HTTPError(404, "Market not found")
        except MarketError as e:
            raise HTTPError(400, e.message)

    def dump_orders(self, orders):
        self.dumps({
            "orders": [
                {
                    "order_id": str(order.order_id),
                    "owner_id": order.owner_id,
                    "give_item": order.give_item,
                    "give_payload": order.give_payload,
                    "give_amount": int(order.give_amount),
                    "take_item": order.take_item,
                    "take_payload": order.take_payload,
                    "take_amount": int(order.take_amount),
                    "time": str(order.time),
                    "available": int(order.available),
                    "payload": order.payload,
                    "deadline": str(order.deadline),
                }
                for order in orders
            ]
        })


class MarketItemsHandler(MarketHandler):
    @scoped(["market", "market_update_item"])
    async def post(self, market_name):
        items = self.application.items
        gamespace = self.token.get(AccessToken.GAMESPACE)

        try:
            items_to_update = validate_value(self.get_argument("items"), "load_json")
        except ValidationError:
            raise HTTPError(400, "Corrupted items")

        market = await self.get_market(market_name)

        try:
            await items.update_items(gamespace, self.token.account, market.market_id, items_to_update)
        except ItemError as e:
            raise HTTPError(e.code, e.message)

    @scoped(["market"])
    async def get(self, market_name):
        items = self.application.items
        gamespace = self.token.get(AccessToken.GAMESPACE)
        market = await self.get_market(market_name)

        try:
            item_entries = await items.list_items(gamespace, self.token.account, market.market_id)
        except ItemError as e:
            raise HTTPError(e.code, e.message)

        self.dumps({
            "items": [
                {
                    "name": entry.name,
                    "payload": entry.payload,
                    "amount": entry.amount
                }
                for entry in item_entries
            ]
        })


class MarketItemHandler(MarketHandler):
    @scoped(["market", "market_update_item"])
    async def post(self, market_name, item_name):
        items = self.application.items
        gamespace = self.token.get(AccessToken.GAMESPACE)

        try:
            payload = validate_value(self.get_argument("payload"), "load_json")
        except ValidationError:
            raise HTTPError(400, "Corrupted payload")

        update_amount = validate_value(self.get_argument("amount"), "int")

        market = await self.get_market(market_name)

        try:
            await items.update_items(gamespace, self.token.account, market.market_id, [{
                "name": item_name,
                "amount": update_amount,
                "payload": payload
            }])
        except ItemError as e:
            raise HTTPError(e.code, e.message)

    @scoped(["market"])
    async def get(self, market_name, item_name):
        items = self.application.items
        gamespace = self.token.get(AccessToken.GAMESPACE)
        market = await self.get_market(market_name)

        try:
            payload = validate_value(self.get_argument("payload"), "load_json")
        except ValidationError:
            raise HTTPError(400, "Corrupted payload")

        try:
            item = await items.find_item(gamespace, self.token.account, market.market_id, item_name, payload)
        except ItemError as e:
            raise HTTPError(e.code, e.message)
        except NoItemError:
            raise HTTPError(404, "No such item")

        self.dumps({
            "amount": int(item.amount)
        })


class UpdateMarketOrdersHandler(MarketHandler):
    @scoped(["market", "market_post_order"])
    async def post(self, market_name):
        give_item = validate_value(self.get_argument("give_item"), "str_name")
        give_amount = validate_value(self.get_argument("give_amount", "1"), "int")
        give_payload = validate_value(self.get_argument("give_payload", "{}"), "load_json_dict_of_primitives")
        take_item = validate_value(self.get_argument("take_item"), "str_name")
        take_amount = validate_value(self.get_argument("take_amount", "1"), "int")
        take_payload = validate_value(self.get_argument("take_payload", "{}"), "load_json_dict_of_primitives")
        orders_amount = validate_value(self.get_argument("orders_amount", "1"), "int")
        payload = validate_value(self.get_argument("payload", "{}"), "load_json_dict")
        deadline = validate_value(self.get_argument("deadline"), "datetime")

        gamespace_id = self.token.get(AccessToken.GAMESPACE)

        try:
            market = await self.application.markets.find_market(gamespace_id, market_name)
        except NoMarketError:
            raise HTTPError(404, "Market not found")
        except MarketError as e:
            raise HTTPError(400, e.message)

        try:
            order_id = await self.application.orders.new_order(
                gamespace_id, self.token.account, market.market_id,
                give_item, give_payload, give_amount,
                take_item, take_payload, take_amount,
                orders_amount, payload, deadline, subtract_items=True)
        except OrderError as e:
            raise HTTPError(e.code, e.message)

        try:
            fulfilled = await self.application.orders.fulfill_order(
                order_id, gamespace_id, self.token.account, market.market_id)
        except OrderError as e:
            logging.exception("Could not fulfill an order after creation")
            fulfilled = False

        self.dumps({
            "order_id": str(order_id),
            "fulfilled_immediately": bool(fulfilled)
        })

    @scoped(["market"])
    async def get(self, market_name):
        gamespace_id = self.token.get(AccessToken.GAMESPACE)
        market = await self.get_market(market_name)
        q = self.application.orders.orders_query(gamespace_id, market.market_id)

        owner_id = self.get_argument("owner_id", None)
        give_item = self.get_argument("give_item", None)
        give_amount = self.get_argument("give_amount", None)
        give_amount_comparison = self.get_argument("give_amount_comparison", None)
        give_payload = self.get_argument("give_payload", None)
        take_item = self.get_argument("take_item", None)
        take_amount = self.get_argument("take_amount", None)
        take_amount_comparison = self.get_argument("take_amount_comparison", None)
        take_payload = self.get_argument("take_payload", None)
        sort_by = self.get_argument("sort_by", "")
        if sort_by:
            sort_by = validate_value(sort_by, "str_name")
        sort_desc = self.get_argument("sort_desc", "true") == "true"

        q.offset = validate_value(self.get_argument("offset", "0"), "int")
        q.limit = min(validate_value(self.get_argument("limit", "1000"), "int"), 1000)

        if owner_id:
            q.owner = validate_value(owner_id, "int")
        if give_item:
            q.give_item = validate_value(give_item, "str_name")
        if give_payload:
            q.give_payload = validate_value(give_payload, "load_json_dict_of_primitives")
        if give_amount and give_amount_comparison:
            q.give_amount = validate_value(give_amount, "str_name")
            q.give_amount_comparison = validate_value(give_amount_comparison, "str")
        if take_item:
            q.take_item = validate_value(take_item, "str_name")
        if take_payload:
            q.take_payload = validate_value(take_payload, "load_json_dict_of_primitives")
        if take_amount and take_amount_comparison:
            q.take_amount = validate_value(take_amount, "str_name")
            q.take_amount_comparison = validate_value(take_amount_comparison, "str")

        q.sort_by = sort_by
        q.sort_desc = sort_desc

        try:
            orders = await q.query()
        except OrderError as e:
            raise HTTPError(e.code, e.message)

        self.dump_orders(orders)


class UpdateMarketMyOrdersHandler(MarketHandler):
    @scoped(["market"])
    async def get(self, market_name):
        gamespace_id = self.token.get(AccessToken.GAMESPACE)
        market = await self.get_market(market_name)
        q = self.application.orders.orders_query(gamespace_id, market.market_id)
        q.owner = self.token.account

        try:
            orders = await q.query()
        except OrderError as e:
            raise HTTPError(e.code, e.message)

        self.dump_orders(orders)


class OrderHandler(MarketHandler):
    @scoped(["market"])
    async def get(self, market_name, order_id):
        gamespace_id = self.token.get(AccessToken.GAMESPACE)
        market = await self.get_market(market_name)

        try:
            order = await self.application.orders.get_order(
                gamespace_id, order_id)
        except OrderError as e:
            raise HTTPError(e.code, e.message)
        except NoOrderError as e:
            raise HTTPError(404, "No such order")

        if str(market.market_id) != str(order.market_id):
            raise HTTPError(409, "This order does not belong to this market")

        self.dumps({
            "order_id": str(order.order_id),
            "owner_id": str(order.owner_id),
            "give_item": order.give_item,
            "give_payload": order.give_payload,
            "give_amount": int(order.give_amount),
            "take_item": order.take_item,
            "take_payload": order.take_payload,
            "take_amount": int(order.take_amount),
            "available": int(order.available),
            "time": str(order.time),
            "deadline": str(order.deadline)
        })


class FulfillOrderHandler(MarketHandler):
    @scoped(["market", "market_post_order"])
    async def post(self, market_name, order_id):
        gamespace_id = self.token.get(AccessToken.GAMESPACE)
        market = await self.get_market(market_name)

        fulfill_amount = validate_value(self.get_argument("amount", "1"), "int")

        try:
            result = await self.application.orders.fulfill_order_with_account(
                order_id, gamespace_id, self.token.account, market.market_id, fulfill_amount)
        except OrderError as e:
            raise HTTPError(e.code, e.message)

        if result is None:
            raise HTTPError(409, "Cannot fulfill the order")

        self.dumps({
            "order_id": order_id,
            "fulfilled_completely": result
        })


class DeleteOrderHandler(MarketHandler):
    @scoped(["market", "market_post_order"])
    async def post(self, market_name, order_id):
        gamespace_id = self.token.get(AccessToken.GAMESPACE)
        market = await self.get_market(market_name)

        try:
            order = await self.application.orders.get_order(
                gamespace_id, order_id)
        except OrderError as e:
            raise HTTPError(e.code, e.message)

        if (str(order.owner_id) != str(self.token.account)) and (not self.token.has_scopes(["market_delete_order"])):
            raise HTTPError(409, "The order has not been created by you.")

        if str(order.market_id) != str(market.market_id):
            raise HTTPError(409, "The order does not belong to the market")

        try:
            await self.application.orders.delete_order(
                gamespace_id, order_id)
        except OrderError as e:
            raise HTTPError(e.code, e.message)


class GetMarketHandler(MarketHandler):
    @scoped(["market"])
    async def get(self, market_name):
        market = await self.get_market(market_name)
        self.dumps({
            "settings": market.settings
        })
