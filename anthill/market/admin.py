from anthill.common.validate import validate

from anthill.common import update as common_update, to_int
from anthill.common.validate import validate_value
import anthill.common.admin as a

from . model.item import ItemError, NoItemError
from . model.market import MarketError, NoMarketError
from . model.order import OrderQuery, OrderQueryError, OrderError, NoOrderError

import math
import ujson


class RootAdminController(a.AdminController):
    def render(self, data):
        return [
            a.links("Market service", [
                a.link("markets", "Markets", icon="area-chart")
            ])
        ]

    def access_scopes(self):
        return ["market_admin"]


class MarketsAdminController(a.AdminController):
    def render(self, data):
        return [
            a.breadcrumbs([], "Markets"),
            a.content(title="Markets", headers=[
                {
                    "id": "market",
                    "title": "Market"
                }
            ], items=[{
                "market": [
                    a.link("market", market_entry.name, icon="bullhorn", market_id=market_entry.market_id)
                ]
            } for market_entry in data["markets"]], style="default"),
            a.links("Actions", [
                a.link("new_market", "New market", icon="plus")
            ])
        ]

    async def get(self):
        try:
            markets = await self.application.markets.list_markets(self.gamespace)
        except MarketError as e:
            raise a.ActionError("Cannot obtain list of markets: {0}".format(e.message))

        return {
            "markets": markets
        }

    def access_scopes(self):
        return ["market_admin"]


class NewMarketAdminController(a.AdminController):
    def render(self, data):
        return [
            a.breadcrumbs([a.link("markets", "Markets")], "New market"),
            a.form("Create New Market", fields={
                "name": a.field("Market Name", "text", "primary", "non-empty", order=1),
                "settings": a.field(
                    "Market Settings", "json", "primary", "non-empty",
                    description="Arbitrary object that could be useful to a game", order=2, height=400),
            }, methods={
                "create": a.method("Create", "primary")
            }, data={"settings": {}}),
        ]

    @validate(name="str_name", settings="load_json_dict")
    async def create(self, name, settings):
        try:
            market_id = await self.application.markets.new_market(
                self.gamespace, name, settings)
        except MarketError as e:
            raise a.ActionError("Cannot create a market: {0}".format(e.message))
        raise a.Redirect("market", market_id=market_id)

    def access_scopes(self):
        return ["market_admin"]


class MarketAdminController(a.AdminController):
    def render(self, data):
        return [
            a.breadcrumbs([
                a.link("markets", "Markets")
            ], data["name"]),
            a.split([
                a.links("Market options", links=[
                    a.link("market_orders", "Orders", icon="bar-chart", market_id=self.get_context("market_id")),
                    a.link("market_settings", "Settings", icon="gears", market_id=self.get_context("market_id")),
                ]),
                a.form("See/Edit User Holdings", fields={
                    "account": a.field("User ID", "text", "primary", "number")
                }, methods={
                    "lookup": a.method("Lookup", "primary")
                }, data={})
            ])
        ]

    @validate(account="int")
    async def lookup(self, account):

        market_id = self.get_context("market_id")

        try:
            await self.application.markets.get_market(self.gamespace, market_id)
        except MarketError as e:
            raise a.ActionError("Cannot get a market: {0}".format(e.message))
        except NoMarketError:
            raise a.ActionError("No such market")

        raise a.Redirect("market_items", account_id=account, market_id=market_id)

    @validate(market_id="int")
    async def get(self, market_id):
        try:
            market_data = await self.application.markets.get_market(self.gamespace, market_id)
        except MarketError as e:
            raise a.ActionError("Cannot get a market: {0}".format(e.message))
        except NoMarketError:
            raise a.ActionError("No such market")

        return {
            "name": market_data.name
        }


class MarketSettingsAdminController(a.AdminController):
    def render(self, data):
        return [
            a.breadcrumbs([
                a.link("markets", "Markets"),
                a.link("market", data["name"], market_id=self.get_context("market_id")),
            ], "Settings"),
            a.form("Edit Market", fields={
                "name": a.field("Market Name", "text", "primary", "non-empty", order=1),
                "settings": a.field(
                    "Market Settings", "json", "primary", "non-empty",
                    description="Arbitrary object that could be useful to a game", order=2, height=400),
            }, methods={
                "update": a.method("Update", "primary"),
                "delete": a.method("Delete", "danger"),
            }, data=data),
        ]

    async def get(self, market_id):
        try:
            market_data = await self.application.markets.get_market(self.gamespace, market_id)
        except MarketError as e:
            raise a.ActionError("Cannot get a market: {0}".format(e.message))

        return {
            "name": market_data.name,
            "settings": market_data.settings,
        }

    @validate(name="str_name", settings="load_json_dict")
    async def update(self, name, settings):
        market_id = self.get_context("market_id")

        try:
            await self.application.markets.update_market(self.gamespace, market_id, name, settings)
        except MarketError as e:
            raise a.ActionError("Cannot update market: {0}".format(e.message))

        raise a.Redirect("market", message="Market settings have been updated", market_id=market_id)

    async def delete(self, **ignored):
        market_id = self.get_context("market_id")

        try:
            await self.application.markets.delete_market(self.gamespace, market_id)
        except MarketError as e:
            raise a.ActionError("Cannot update market: {0}".format(e.message))

        raise a.Redirect("markets")

    def access_scopes(self):
        return ["market_admin"]


class MarketOrdersAdminController(a.AdminController):
    ORDERS_PER_PAGE = 20

    def render(self, data):
        market_id = self.get_context("market_id")

        orders = [
            {
                "owner": [
                    a.link("market_orders", str(order.owner_id), icon="user",
                           market_id=market_id, order_owner=order.owner_id)
                ],
                "give_item": [
                    a.link("market_orders", order.give_item, market_id=market_id, order_give_item=order.give_item)
                ],
                "give_payload": [
                    a.json_view(order.give_payload)
                ],
                "give_amount": str(order.give_amount),
                "take_item": [
                    a.link("market_orders", order.take_item, market_id=market_id, order_take_item=order.take_item)
                ],
                "take_payload": [
                    a.json_view(order.take_payload)
                ],
                "payload": [
                    a.json_view(order.payload)
                ],
                "take_amount": str(order.take_amount),
                "available": str(order.available),
                "time": str(order.time),
                "deadline": str(order.deadline),
                "id": [
                    a.link("order", order.order_id, icon="certificate", order_id=order.order_id)
                ]
            }
            for order in data["orders"]
        ]

        return [
            a.breadcrumbs([
                a.link("markets", "Markets"),
                a.link("market", data["market_name"], market_id=self.context.get("market_id"))
            ], "Orders"),
            a.content("Orders", [
                {
                    "id": "id",
                    "title": "ID"
                }, {
                    "id": "owner",
                    "title": "Seller"
                }, {
                    "id": "give_item",
                    "title": "Item To Sell"
                }, {
                    "id": "give_payload",
                    "title": "Sell Payload"
                }, {
                    "id": "give_amount",
                    "title": "Sell Amount"
                }, {
                    "id": "take_item",
                    "title": "Item To Buy"
                }, {
                    "id": "take_payload",
                    "title": "Buy Payload"
                }, {
                    "id": "take_amount",
                    "title": "Buy Amount"
                }, {
                    "id": "available",
                    "title": "Entries Available"
                }, {
                    "id": "time",
                    "title": "Time"
                }, {
                    "id": "payload",
                    "title": "Order Payload"
                }, {
                    "id": "deadline",
                    "title": "Deadline"
                }], orders, "default", empty="No orders to display."),
            a.pages(data["pages"]),
            a.form("Filters", fields={
                "order_owner":
                    a.field("Seller", "text", "primary", order=1),
                "order_give_item":
                    a.field("Sell Item", "text", "primary", order=2),
                "order_give_payload":
                    a.field("Sell Payload", "json", "primary", order=3, height=100),
                "order_give_amount":
                    a.field("Sell Amount", "text", "number", order=4),
                "order_give_amount_comparison":
                    a.field("Sell Amount Should Be", "select", "primary", order=5, values=data["comparisons"]),
                "order_take_item":
                    a.field("Buy Item", "text", "primary", order=6),
                "order_take_payload":
                    a.field("Buy Payload", "json", "primary", order=7, height=100),
                "order_take_amount":
                    a.field("Buy Amount", "text", "number", order=8),
                "order_take_amount_comparison":
                    a.field("Buy Amount Should Be", "select", "primary", order=9, values=data["comparisons"]),
            }, methods={
                "filter": a.method("Filter", "primary")
            }, data=data, icon="filter"),
            a.links("Actions", [
                a.link("market", "Go back", icon="chevron-left", market_id=self.context.get("market_id")),
                a.link("new_order", "Post an order", icon="plus", market_id=self.context.get("market_id")),
            ])
        ]

    def access_scopes(self):
        return ["market_admin"]

    async def filter(self, **args):

        market_id = self.context.get("market_id")
        page = self.context.get("page", 1)

        filters = {
            "page": page
        }

        filters.update({
            k: v for k, v in args.items() if v not in ["0", "any"]
        })

        raise a.Redirect("market_orders", market_id=market_id, **filters)

    @validate(market_id="int", page="int", order_owner="int",
              order_give_item="str_name", order_give_payload="load_json_dict",
              order_give_amount="int", order_give_amount_comparison="str", order_take_item="str_name",
              order_take_payload="load_json_dict", order_take_amount="int", order_take_amount_comparison="str")
    async def get(self,
                  market_id,
                  page=1,
                  order_owner=None,
                  order_give_item=None,
                  order_give_payload=None,
                  order_give_amount=None,
                  order_give_amount_comparison=None,
                  order_take_item=None,
                  order_take_payload=None,
                  order_take_amount=None,
                  order_take_amount_comparison=None):

        markets = self.application.markets

        try:
            market = await markets.get_market(self.gamespace, market_id)
        except NoMarketError:
            raise a.ActionError("No such market")

        page = to_int(page)

        orders = self.application.orders

        q = orders.orders_query(self.gamespace, market_id)

        q.offset = (page - 1) * MarketOrdersAdminController.ORDERS_PER_PAGE
        q.limit = MarketOrdersAdminController.ORDERS_PER_PAGE

        q.order_owner = order_owner
        q.give_item = order_give_item
        q.give_payload = order_give_payload
        q.take_item = order_take_item
        q.take_payload = order_take_payload

        if order_give_amount_comparison != "any":
            q.give_amount = order_give_amount
            q.give_amount_comparison = order_give_amount_comparison

        if order_take_amount_comparison != "any":
            q.take_amount = order_take_amount
            q.take_amount_comparison = order_take_amount_comparison

        orders, count = await q.query(count=True)
        pages = int(math.ceil(float(count) / float(MarketOrdersAdminController.ORDERS_PER_PAGE)))

        return {
            "orders": orders,
            "pages": pages,
            "order_owner": order_owner or "0",
            "order_give_item": order_give_item or "0",
            "order_give_payload": order_give_payload or {},
            "order_give_amount": order_give_amount or "0",
            "order_give_amount_comparison": order_give_amount_comparison or "any",
            "order_take_item": order_take_item or "0",
            "order_take_payload": order_take_payload or {},
            "order_take_amount": order_take_amount or "0",
            "order_take_amount_comparison": order_take_amount_comparison or "any",
            "market_name": market.name,
            "comparisons": {
                "any": "Any",
                OrderQuery.COMP_LESS: "Less",
                OrderQuery.COMP_MORE: "More",
                OrderQuery.COMP_EQUAL: "Equal",
                OrderQuery.COMP_LESS_OR_EQUAL: "Less Or Equal",
                OrderQuery.COMP_MORE_OR_EQUAL: "More Or Equal"
            }
        }


class OrderAdminController(a.AdminController):
    def render(self, data):
        return [
            a.breadcrumbs([
                a.link("markets", "Markets"),
                a.link("market", data["market_name"], market_id=data["market_id"]),
                a.link("market_orders", "Orders", market_id=data["market_id"]),
            ], str(self.get_context("order_id"))),
            a.form("Settings", fields={
                "order_owner":
                    a.field("Owner", "readonly", "primary", order=1),
                "order_give_item":
                    a.field("Sell Item", "text", "primary", order=2),
                "order_give_payload":
                    a.field("Sell Payload", "json", "primary", order=3, height=200),
                "order_give_amount":
                    a.field("Sell Amount", "text", "number", order=4),
                "order_take_item":
                    a.field("Buy Item", "text", "primary", order=5),
                "order_take_payload":
                    a.field("Buy Payload", "json", "primary", order=6, height=200),
                "order_take_amount":
                    a.field("Buy Amount", "text", "number", order=7),
                "order_available":
                    a.field("Orders Available", "text", "number", order=8),
                "order_deadline":
                    a.field("Deadline", "date", "primary", order=9),
                "order_payload":
                    a.field("Order Payload", "json", "primary", order=10, height=200),
                "order_time":
                    a.field("Time created", "readonly", "primary", order=11)
            }, methods={
                "update": a.method("Update", "primary"),
                "delete": a.method("Delete", "danger"),
            }, data=data),
            a.form("Fulfill an order", fields={
                "fulfill_account":
                    a.field("Fulfill Account With", "text", "number", order=1),
                "orders_amount":
                    a.field("Orders Amount", "text", "number", order=2)
            }, methods={
                "fulfill": a.method("Fulfill", "primary")
            }, data=data),
            a.links("Actions", [
                a.link("market_orders", "Go back", icon="chevron-left", market_id=data["market_id"]),
                a.link("new_order", "Clone order", icon="clone",
                       market_id=data["market_id"], clone_id=self.get_context("order_id")),
            ])
        ]

    @validate(fulfill_account="int", orders_amount="int")
    async def fulfill(self, fulfill_account, orders_amount):
        order_id = self.get_context("order_id")

        try:
            order_data = await self.application.orders.get_order(self.gamespace, order_id)
        except OrderError as e:
            raise a.ActionError("Cannot order: {0}".format(e.message))
        except NoOrderError:
            raise a.ActionError("No such order: {0}".format(order_id))

        try:
            deleted = await self.application.orders.fulfill_order_with_account(
                order_id, self.gamespace, fulfill_account, order_data.market_id, orders_amount)
        except OrderError as e:
            raise a.ActionError("Cannot fulfill order: {0}".format(e.message))
        except NoOrderError:
            raise a.ActionError("No such order: {0}".format(order_id))

        if deleted is None:
            raise a.ActionError("Cannot fulfill the order (not enough items or other reasons)")

        if deleted is True:
            raise a.Redirect("market_orders", message="Order has been completely fulfilled",
                             market_id=order_data.market_id)

        raise a.Redirect("order", message="Order has been updated", order_id=order_id)

    async def delete(self, **ignored):
        order_id = self.get_context("order_id")

        try:
            order_data = await self.application.orders.get_order(self.gamespace, order_id)
        except OrderError as e:
            raise a.ActionError("Cannot order: {0}".format(e.message))
        except NoOrderError as e:
            raise a.ActionError("No such order: {0}".format(order_id))

        try:
            await self.application.orders.delete_order(self.gamespace, order_id)
        except OrderError as e:
            raise a.ActionError("Cannot delete order: {0}".format(e.message))

        raise a.Redirect("market_orders", message="Order has been successfully deleted", market_id=order_data.market_id)

    @validate(order_give_item="str_name", order_give_payload="load_json_dict",
              order_give_amount="int", order_take_item="str_name",
              order_take_payload="load_json_dict", order_take_amount="int", order_available="int",
              order_payload="load_json_dict",
              order_deadline="str_datetime")
    async def update(self, order_give_item, order_give_payload, order_give_amount,
                     order_take_item, order_take_payload, order_take_amount,
                     order_available, order_payload, order_deadline):
        order_id = self.get_context("order_id")

        try:
            order = await self.application.orders.get_order(self.gamespace, order_id)
        except OrderError as e:
            raise a.ActionError("Cannot get order: {0}".format(e.message))
        except NoOrderError as e:
            raise a.ActionError("No such order: {0}".format(order_id))

        try:
            await self.application.orders.update_order(
                self.gamespace, order.owner_id, order.market_id, order_id,
                order_give_item, order_give_payload, order_give_amount,
                order_take_item, order_take_payload, order_take_amount,
                order_available, order_payload, order_deadline)
        except OrderError as e:
            raise a.ActionError("Cannot update order: {0}".format(e.message))

        if await self.application.orders.fulfill_order(order_id, self.gamespace, order.order_id, order.market_id):
            raise a.Redirect("market_orders",
                             message="Order has been updated and then got fulfilled",
                             market_id=order.market_id)
        else:
            raise a.Redirect("order", message="Order has been updated", order_id=order_id)

    @validate(order_id="int")
    async def get(self, order_id):
        try:
            order_data = await self.application.orders.get_order(self.gamespace, order_id)
        except OrderError as e:
            raise a.ActionError("Cannot get order: {0}".format(e.message))
        except NoOrderError as e:
            raise a.ActionError("No such order: {0}".format(order_id))

        try:
            market_data = await self.application.markets.get_market(self.gamespace, order_data.market_id)
        except MarketError as e:
            raise a.ActionError("Cannot get a market: {0}".format(e.message))
        except NoMarketError:
            raise a.ActionError("No such market")

        return {
            "market_name": market_data.name,
            "market_id": order_data.market_id,
            "order_owner": order_data.owner_id,
            "order_give_item": order_data.give_item,
            "order_give_payload": order_data.give_payload or {},
            "order_give_amount": order_data.give_amount,
            "order_take_item": order_data.take_item,
            "order_take_payload": order_data.take_payload or {},
            "order_take_amount": order_data.take_amount,
            "order_available": order_data.available,
            "order_deadline": str(order_data.deadline),
            "order_time": str(order_data.time),
            "fulfill_account": str(self.token.account),
            "order_payload": order_data.payload or {},
            "orders_amount": "1"
        }


class NewOrderAdminController(a.AdminController):
    def render(self, data):
        return [
            a.breadcrumbs([
                a.link("markets", "Markets"),
                a.link("market", data["market_name"], market_id=data["market_id"]),
                a.link("market_orders", "Orders", market_id=data["market_id"]),
            ], "New order"),
            a.form("New order settings", fields={
                "order_owner":
                    a.field("Owner", "text", "primary", order=1),
                "order_give_item":
                    a.field("Sell Item", "text", "primary", order=2),
                "order_give_payload":
                    a.field("Sell Payload", "json", "primary", order=3, height=200),
                "order_give_amount":
                    a.field("Sell Amount", "text", "number", order=4),
                "order_take_item":
                    a.field("Buy Item", "text", "primary", order=5),
                "order_take_payload":
                    a.field("Buy Payload", "json", "primary", order=6, height=200),
                "order_take_amount":
                    a.field("Buy Amount", "text", "number", order=7),
                "order_available":
                    a.field("Orders Available", "text", "number", order=8),
                "order_payload":
                    a.field("Order Payload", "json", "primary", order=10, height=200),
                "order_deadline":
                    a.field("Deadline", "date", "primary", order=9),
            }, methods={
                "create": a.method("Clone" if self.context.get("clone_id", None) else "Create", "primary")
            }, data=data),
            a.links("Actions", [
                a.link("market_orders", "Go back", icon="chevron-left", market_id=data["market_id"])
            ])
        ]

    @validate(order_owner="int", order_give_item="str_name", order_give_payload="load_json_dict",
              order_give_amount="int", order_take_item="str_name",
              order_take_payload="load_json_dict", order_take_amount="int", order_available="int",
              order_payload="load_json_dict",
              order_deadline="str_datetime")
    async def create(self, order_owner, order_give_item, order_give_payload, order_give_amount,
                     order_take_item, order_take_payload, order_take_amount,
                     order_available, order_payload, order_deadline):

        market_id = self.get_context("market_id")

        try:
            await self.application.markets.get_market(self.gamespace, market_id)
        except MarketError as e:
            raise a.ActionError("Cannot get a market: {0}".format(e.message))
        except NoMarketError:
            raise a.ActionError("No such market")

        try:
            order_id = await self.application.orders.new_order(
                self.gamespace, order_owner, market_id,
                order_give_item, order_give_payload, order_give_amount,
                order_take_item, order_take_payload, order_take_amount,
                order_available, order_payload, order_deadline)
        except OrderError as e:
            raise a.ActionError("Cannot create order: {0}".format(e.message))

        if await self.application.orders.fulfill_order(order_id, self.gamespace, order_owner, market_id):
            raise a.Redirect(
                "market_orders",
                message="Order has been created but was immediately fulfilled",
                market_id=market_id)

        raise a.Redirect("order", message="Order has been created", order_id=order_id)

    @validate(market_id="int", clone_id="int")
    async def get(self, market_id, clone_id=None):

        if clone_id:
            try:
                order_data = await self.application.orders.get_order(self.gamespace, clone_id)
            except OrderError as e:
                raise a.ActionError("Cannot get order: {0}".format(e.message))
            except NoOrderError as e:
                raise a.ActionError("No such order: {0}".format(clone_id))
        else:
            order_data = None

        try:
            market_data = await self.application.markets.get_market(self.gamespace, market_id)
        except MarketError as e:
            raise a.ActionError("Cannot get a market: {0}".format(e.message))
        except NoMarketError:
            raise a.ActionError("No such market")

        return {
            "market_name": market_data.name,
            "market_id": market_id,
            "order_owner": str(self.token.account),
            "order_give_item": order_data.give_item if order_data else "",
            "order_give_payload": order_data.give_payload if order_data else {},
            "order_give_amount": order_data.give_amount if order_data else 1,
            "order_take_item": order_data.take_item if order_data else "",
            "order_take_payload": order_data.take_payload if order_data else {},
            "order_take_amount": order_data.take_amount if order_data else 1,
            "order_available": order_data.available if order_data else 1,
            "order_deadline": str(order_data.deadline) if order_data else "",
            "order_time": str(order_data.time) if order_data else "",
            "order_payload": order_data.payload if order_data else {},
        }


class MarketItemsAdminController(a.AdminController):
    def render(self, data):
        return [
            a.breadcrumbs([
                a.link("markets", "Markets"),
                a.link("market", data["market_name"], market_id=self.get_context("market_id")),
            ], "User {0} holdings".format(self.get_context("account_id"))),
            a.content("Holdings", headers=[
                {
                    "id": "name",
                    "title": "Name"
                },
                {
                    "id": "amount",
                    "title": "Amount"
                },
                {
                    "id": "payload",
                    "title": "Payload"
                },
                {
                    "id": "hash",
                    "title": "Hash"
                },
                {
                    "id": "actions",
                    "title": "Actions"
                }
            ], items=[
                {
                    "name": [
                        a.link("user_item", str(item.name), market_id=self.get_context("market_id"),
                               account_id=self.get_context("account_id"), hash=item.hash)
                    ],
                    "amount": str(item.amount),
                    "payload": [
                        a.json_view(contents=item.payload)
                    ],
                    "hash": item.hash,
                    "actions": [
                        a.button("market_items", "Remove", "primary", _method="update_ctx",
                                 account_id=self.get_context("account_id"), market_id=self.get_context("market_id"),
                                 name=item.name, payload=ujson.dumps(item.payload), amount=str(-int(item.amount)))
                    ]
                }
                for item in data["items"]
            ], style="default"),
            a.form("Update holdings", fields={
                "name": a.field("Item Name", "text", "non-empty", order=1),
                "payload": a.field("Payload", "json", "primary",
                                   description="Each holding with unique payload uses it's own count.",
                                   order=2, height=200),
                "amount": a.field("Amount", "text", "number", description="Positive or negative", order=3)
            }, methods={
                "update": a.method("Update", "primary")
            }, data={"payload": {}}),
            a.links("Actions", [
                a.link("market", "Go back", icon="chevron-left", market_id=self.get_context("market_id"))
            ])
        ]

    async def update_ctx(self):
        name = self.get_context("name")
        payload = self.get_context("payload")
        amount = self.get_context("amount")
        await self.update(name, payload, amount)

    @validate(name="str_name", payload="load_json_dict", amount="int")
    async def update(self, name, payload, amount):
        market_id = validate_value(self.get_context("market_id"), "int")
        account_id = validate_value(self.get_context("account_id"), "int")

        try:
            await self.application.markets.get_market(self.gamespace, market_id)
        except MarketError as e:
            raise a.ActionError("Cannot get a market: {0}".format(e.message))
        except NoMarketError:
            raise a.ActionError("No such market")

        try:
            await self.application.items.update_item(
                self.gamespace, account_id, market_id,
                name, amount, payload)
        except ItemError as e:
            raise a.ActionError("Cannot list items: {0}".format(e.message))

        raise a.Redirect("market_items", account_id=account_id, market_id=market_id)

    @validate(market_id="int", account_id="int")
    async def get(self, market_id, account_id):
        try:
            market_data = await self.application.markets.get_market(self.gamespace, market_id)
        except MarketError as e:
            raise a.ActionError("Cannot get a market: {0}".format(e.message))
        except NoMarketError:
            raise a.ActionError("No such market")

        try:
            items = await self.application.items.list_items(self.gamespace, account_id, market_id)
        except ItemError as e:
            raise a.ActionError("Cannot list items: {0}".format(e.message))

        return {
            "market_name": market_data.name,
            "items": items
        }

