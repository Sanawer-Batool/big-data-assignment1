from mrjob.job import MRJob
import csv
 
REGION_COL = 24       # Market column
DELAY_RISK_COL = 6    # Late_delivery_risk column
 
class MarketDelayRiskAnalyzer(MRJob):
    def mapper_init(self):
        self.first_line = False
 
    def mapper(self, _, record):
        parsed = next(csv.reader([record]))
        if not self.first_line:
            self.first_line = True
            return
        try:
            region = parsed[REGION_COL].strip()
            is_delayed = 1 if float(parsed[DELAY_RISK_COL]) > 0 else 0
            yield region, (is_delayed, 1)
        except Exception:
            pass
 
    def combiner(self, region, pairs):
        delayed_count = order_count = 0
        for delay_flag, count in pairs:
            delayed_count += delay_flag
            order_count += count
        yield region, (delayed_count, order_count)
 
    def reducer(self, region, pairs):
        delayed_count = order_count = 0
        for delay_flag, count in pairs:
            delayed_count += delay_flag
            order_count += count
        ratio = (float(delayed_count) / order_count) if order_count else 0.0
        yield region, ratio
 
if __name__ == "__main__":
    MarketDelayRiskAnalyzer.run()