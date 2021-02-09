
from anthill.common.options import options

from . import handler as h
from . import options as _opts

from anthill.common import server, database, access, keyvalue

from . import admin
from . model.item import ItemModel
from . model.market import MarketModel
from . model.order import OrderModel
from . model.transaction import TransactionModel


class MarketServer(server.Server):
    # noinspection PyShadowingNames
    def __init__(self):
        super(MarketServer, self).__init__()

        self.db = database.Database(
            host=options.db_host,
            database=options.db_name,
            user=options.db_username,
            password=options.db_password)

        self.cache = keyvalue.KeyValueStorage(
            host=options.cache_host,
            port=options.cache_port,
            db=options.cache_db,
            max_connections=options.cache_max_connections)

        self.transactions = TransactionModel(self, self.db)
        self.orders = OrderModel(self, self.db)
        self.markets = MarketModel(self, self.db)
        self.items = ItemModel(self, self.db)

    def get_models(self):
        return [self.markets, self.transactions, self.items, self.orders]

    def get_admin(self):
        return {
            "index": admin.RootAdminController,
            "markets": admin.MarketsAdminController,
            "new_market": admin.NewMarketAdminController,
            "market": admin.MarketAdminController,
            "market_settings": admin.MarketSettingsAdminController,
            "market_orders": admin.MarketOrdersAdminController,
            "order": admin.OrderAdminController,
            "new_order": admin.NewOrderAdminController,
            "market_items": admin.MarketItemsAdminController,
        }

    def get_metadata(self):
        return {
            "title": "Market",
            "description": "Real time in-game free market",
            "icon": "area-chart"
        }

    def get_handlers(self):
        return [
            (r"/markets/(.*)/items", h.MarketItemsHandler),
            (r"/markets/(.*)/items/(.*)", h.MarketItemHandler),
            (r"/markets/(.*)/orders", h.UpdateMarketOrdersHandler),
            (r"/markets/(.*)/orders/my", h.UpdateMarketMyOrdersHandler),
            (r"/markets/(.*)/orders/(.*)/fulfill", h.FulfillOrderHandler),
            (r"/markets/(.*)/orders/(.*)/delete", h.DeleteOrderHandler),
            (r"/markets/(.*)/orders/(.*)", h.OrderHandler),
            (r"/markets/(.*)", h.GetMarketHandler)
        ]


if __name__ == "__main__":
    stt = server.init()
    access.AccessToken.init([access.public()])
    server.start(MarketServer)
