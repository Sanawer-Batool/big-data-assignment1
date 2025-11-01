from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
 
DELIVERY_COL = 52  # Shipping Mode column
TRANS_ID_COL = 29  # Order Id column
 
class ShippingMethodCounter(MRJob):
    def mapper_init(self):
        self.first_row = False
 
    def mapper(self, _, record):
        parsed = next(csv.reader([record]))
        if not self.first_row:
            self.first_row = True
            return
        try:
            delivery_type = parsed[DELIVERY_COL].strip()
            transaction_id = parsed[TRANS_ID_COL].strip()
            yield (delivery_type, transaction_id), 1
        except Exception:
            pass
 
    def combiner(self, composite_key, _counts):
        yield composite_key, 1
 
    def reducer_unique(self, composite_key, _counts):
        delivery_type, _ = composite_key
        yield delivery_type, 1
 
    def reducer_aggregate(self, delivery_type, counts):
        yield delivery_type, sum(counts)
 
    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer_unique),
            MRStep(reducer=self.reducer_aggregate)
        ]
 
if __name__ == "__main__":
    ShippingMethodCounter.run()