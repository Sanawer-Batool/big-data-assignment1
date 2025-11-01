from mrjob.job import MRJob
import csv
 
PRODUCT_TYPE_COL = 8   # Category Name column
DELIVERY_COL = 52      # Shipping Mode column
REDUCTION_COL = 32     # Order Item Discount Rate column
 
class DiscountRateAnalyzer(MRJob):
    def mapper_init(self):
        self.first_row = False
 
    def mapper(self, _, record):
        parsed = next(csv.reader([record]))
        if not self.first_row:
            self.first_row = True
            return
        try:
            composite = (parsed[PRODUCT_TYPE_COL].strip(), parsed[DELIVERY_COL].strip())
            reduction = float(parsed[REDUCTION_COL])
            yield composite, (reduction, 1)
        except Exception:
            pass
 
    def combiner(self, composite, data_pairs):
        total_reduction = item_count = 0
        for rate, cnt in data_pairs:
            total_reduction += rate
            item_count += cnt
        yield composite, (total_reduction, item_count)
 
    def reducer(self, composite, data_pairs):
        total_reduction = item_count = 0
        for rate, cnt in data_pairs:
            total_reduction += rate
            item_count += cnt
        mean = (total_reduction / float(item_count)) if item_count else 0.0
        yield composite, mean
 
if __name__ == "__main__":
    DiscountRateAnalyzer.run()