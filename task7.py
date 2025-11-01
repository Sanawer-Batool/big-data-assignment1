from mrjob.job import MRJob
import csv
 
CATEGORY_COL = 16      # Customer Segment column
REGION_COL = 24        # Market column
DELAY_FLAG_COL = 6     # Late_delivery_risk column
 
class SegmentMarketRiskPredictor(MRJob):
    def mapper_init(self):
        self.skip_header = False
 
    def mapper(self, _, record):
        parsed = next(csv.reader([record]))
        if not self.skip_header:
            self.skip_header = True
            return
        try:
            group_key = (parsed[CATEGORY_COL].strip(), parsed[REGION_COL].strip())
            is_late = 1 if float(parsed[DELAY_FLAG_COL]) > 0 else 0
            yield group_key, (is_late, 1)
        except Exception:
            pass
 
    def combiner(self, group_key, pairs):
        delayed_sum = order_total = 0
        for delay_indicator, count in pairs:
            delayed_sum += delay_indicator
            order_total += count
        yield group_key, (delayed_sum, order_total)
 
    def reducer(self, group_key, pairs):
        delayed_sum = order_total = 0
        for delay_indicator, count in pairs:
            delayed_sum += delay_indicator
            order_total += count
        risk_ratio = (float(delayed_sum) / order_total) if order_total else 0.0
        yield group_key, risk_ratio
 
if __name__ == "__main__":
    SegmentMarketRiskPredictor.run()