# big-data-assignment1
MapReduce tasks on the e-commerce dataset using MRJob.

# Supply Chain Analytics with MapReduce

Analyzing DataCo Global's supply chain data using Hadoop MapReduce to find insights about sales, deliveries, and customer behavior.

## Dataset

Using the DataCo Smart Supply Chain dataset from [Kaggle](https://www.kaggle.com/datasets/shashwatwork/dataco-smart-supply-chain-for-big-data-analysis). It has about 180K records of transactions across different products and markets.

Download `DataCoSupplyChainDataset.csv` and you're good to go.

## Getting Started

### Setup
```bash
# Install Python and mrjob
pip install mrjob

# Upload data to HDFS
hadoop fs -put DataCoSupplyChainDataset.csv /user/root/
```

### Running Tasks
```bash
# Test locally first (always a good idea)
head -100 DataCoSupplyChainDataset.csv | python task1.py

# Run on Hadoop
python task1.py -r hadoop \
  --python-bin python \
  --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
  hdfs:///user/root/DataCoSupplyChainDataset.csv
```

Replace `task1.py` with whichever task you want to run.

## The Tasks

**Task 1 - Segment Sales**: Total revenue by customer segment (Consumer, Corporate, Home Office)

**Task 2 - Shipping Methods**: Count orders by shipping mode

**Task 3 - Category Delays**: Average delivery delays for each product category

**Task 4 - Market Risks**: Late delivery percentage by market region

**Task 5 - Customer Profits**: Total profit per customer

**Task 6 - Discount Patterns**: Average discount rates by category and shipping mode

**Task 7 - Risk Prediction**: Late delivery risk by segment and market

