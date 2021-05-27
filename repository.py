import abc
import datetime

from sqlalchemy.sql import text

import model


class AbstractRepository(abc.ABC):
    @abc.abstractmethod
    def add(self, batch: model.Batch):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, reference) -> model.Batch:
        raise NotImplementedError


class SqlRepository(AbstractRepository):
    def __init__(self, session):
        self.session = session

    def add(self, batch):
        if batch.eta is None:
            eta = 'NULL'

        cursor = self.session.execute(
                f'INSERT INTO batches (reference, sku, _purchased_quantity, eta)'
                f" VALUES ('{batch.reference}', '{batch.sku}', {batch._purchased_quantity}, {eta})"
                f" ON CONFLICT (reference) DO UPDATE SET _purchased_quantity = excluded._purchased_quantity ")

        batch_id = cursor.lastrowid


        for order_line in batch._allocations:
            orderline_id = self.session.execute(
                    f"INSERT INTO order_lines (sku, qty, orderid) VALUES ('{order_line.sku}', {order_line.qty}, '{order_line.orderid}') ON CONFLICT (orderid) DO NOTHING").lastrowid

            self.session.execute(
                    f"INSERT INTO allocations (orderline_id, batch_id) VALUES ({orderline_id}, {batch_id}) ON CONFLICT (orderline_id) DO NOTHING")
                    

            

    def get(self, reference) -> model.Batch:
        rows = list(self.session.execute(
            f"SELECT * FROM batches JOIN allocations ON allocations.batch_id = batches.id JOIN order_lines ON allocations.orderline_id = order_lines.id WHERE reference = '{reference}'"))

        if len(rows) == 0:
            return None
        else:
            first_row = rows[0]
            if first_row.eta is None:
                eta = None
            else:
                eta = datetime.datetime(first_row.eta)

            batch =  model.Batch(first_row.reference, first_row.sku, first_row._purchased_quantity, eta)

            for row in rows:
                batch.allocate(model.OrderLine(row.orderid, row.sku, row.qty))

            return batch




