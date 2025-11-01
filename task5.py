from mrjob.job import MRJob
import csv
 
CLIENT_ID_COL = 27     # Order Customer Id column
EARNINGS_COL = 39      # Order Profit Per Order column
 
class CustomerProfitAggregator(MRJob):
    def mapper_init(self):
        self.skip_first = False
 
    def mapper(self, _, record):
        parsed = next(csv.reader([record]))
        if not self.skip_first:
            self.skip_first = True
            return
        try:
            client_id = parsed[CLIENT_ID_COL].strip()
            earnings = float(parsed[EARNINGS_COL])
            yield client_id, earnings
        except Exception:
            pass
 
    def combiner(self, client_id, amounts):
        yield client_id, sum(amounts)
 
    def reducer(self, client_id, amounts):
        yield client_id, sum(amounts)
 
if __name__ == "__main__":
    CustomerProfitAggregator.run()