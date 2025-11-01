from mrjob.job import MRJob
import csv
 
PRODUCT_TYPE_COL = 8  # Category Name column
ACTUAL_DAYS_COL = 1   # Days for shipping (real) column
PLANNED_DAYS_COL = 2  # Days for shipment (scheduled) column
 
class CategoryDelayCalculator(MRJob):
    def mapper_init(self):
        self.skip_header = False
 
    def mapper(self, _, record):
        parsed = next(csv.reader([record]))
        if not self.skip_header:
            self.skip_header = True
            return
        try:
            product_type = parsed[PRODUCT_TYPE_COL].strip()
            variance = float(parsed[ACTUAL_DAYS_COL]) - float(parsed[PLANNED_DAYS_COL])
            yield product_type, (variance, 1)
        except Exception:
            pass
 
    def combiner(self, product_type, data_pairs):
        total_variance = total_count = 0
        for v, cnt in data_pairs:
            total_variance += v
            total_count += cnt
        yield product_type, (total_variance, total_count)
 
    def reducer(self, product_type, data_pairs):
        total_variance = total_count = 0
        for v, cnt in data_pairs:
            total_variance += v
            total_count += cnt
        avg = (total_variance / float(total_count)) if total_count else 0.0
        yield product_type, avg
 
if __name__ == "__main__":
    CategoryDelayCalculator.run()