import csv
from datetime import datetime
from heapq import nlargest
from typing import List

from dagster import (
    DynamicOut,
    DynamicOutput,
    In,
    Nothing,
    Out,
    job,
    op,
    usable_as_dagster_type,
)
from pydantic import BaseModel


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: datetime
    close: float
    volume: int
    open: float
    high: float
    low: float

    @classmethod
    def from_list(cls, input_list: list):
        """Do not worry about this class method for now"""
        return cls(
            date=datetime.strptime(input_list[0], "%Y/%m/%d"),
            close=float(input_list[1]),
            volume=int(float(input_list[2])),
            open=float(input_list[3]),
            high=float(input_list[4]),
            low=float(input_list[5]),
        )


@usable_as_dagster_type(description="Aggregation of stock data")
class Aggregation(BaseModel):
    date: datetime
    high: float


@op(
    config_schema={"s3_key": str},
    out={"stocks": Out(dagster_type=List[Stock])},
    tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context):
    output = list()
    with open(context.op_config["s3_key"]) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            stock = Stock.from_list(row)
            output.append(stock)
    return output


@op(
    description="Given a list of stocks return the top x Aggregation with the greatest `high` value",
    ins={"stocks": In(dagster_type=List[Stock])},
    out=DynamicOut(Aggregation),
    config_schema={"nlargest": int},
)
def process_data(context, stocks):
    nlargest = context.op_config["nlargest"]
    top_stocks = sorted(stocks, key=lambda s: s.high, reverse=True)[:nlargest]
    for idx, stock in enumerate(top_stocks):
        context.log.info(stock)
        yield DynamicOutput(Aggregation(date=stock.date, high=stock.high), mapping_key=str(idx))


@op(
    description="Upload an Aggregation to Redis",
    ins={"aggregation": In(dagster_type=Aggregation)},
    out=Out(Nothing),
    tags={"kind": "redis"},
)
def put_redis_data(aggregation):
    pass


@job(
    config={
        "ops": {
            "process_data": {"config": {"nlargest": 2}},
            "get_s3_data": {"config": {"s3_key": "week_1/data/stock.csv"}},
        }
    }
)
def week_1_pipeline():
    stocks = get_s3_data()
    aggregation = process_data(stocks)
    aggregation.map(put_redis_data)
