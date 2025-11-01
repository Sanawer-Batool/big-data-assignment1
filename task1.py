from mrjob.job import MRJob
import csv
 
# Column positions (0-indexed)
CATEGORY_COL = 16
REVENUE_COL = 37
 
class SegmentRevenueAggregator(MRJob):
    def mapper_init(self):
        self.skip_first = False
 
    def mapper(self, _, record):
        parsed = next(csv.reader([record]))
        if not self.skip_first:
            self.skip_first = True
            return
        try:
            category = parsed[CATEGORY_COL].strip()
            revenue = float(parsed[REVENUE_COL])
            yield category, revenue
        except Exception:
            pass
 
    def combiner(self, category, revenues):
        yield category, sum(revenues)
 
    def reducer(self, category, revenues):
        yield category, sum(revenues)
 
if __name__ == "__main__":
    SegmentRevenueAggregator.run()