import datetime
import logging
import random
import secrets
import statistics
import timeit

import resource

from transactional_clock.core.storage import Transaction, TransactionType, ResultingTransaction

log_filename = f"{timeit.default_timer()}_cpu-measurement.csv"
logging.basicConfig(level=logging.INFO,
                    format="%(message)s",
                    handlers=[
                        logging.FileHandler(
                            log_filename),
                        logging.StreamHandler()
                    ]
                    )

iters = 100

transaction_count_min = 5000
transaction_count_max = 50000
transaction_count_step = 500

database = 'test'
collection = 'users'
priority = 50

_id = secrets.token_hex(24)
transaction = Transaction(
    _id,
    {
        'firstname': secrets.token_hex(10),
        'lastname': secrets.token_hex(10),
        'age': random.randint(0, 80)
    },
    datetime.datetime.utcnow(),
    TransactionType.UPDATE,
        database,
        collection,
        priority
)


def process(_transactions: list):
    res = dict()
    for t in _transactions:
        from transactional_clock.core.util import dict_merge
        dict_merge(res, t.data)

    res = ResultingTransaction(_id, res, TransactionType.UPDATE, database, collection)

transactions = list()
for transaction_count in range(transaction_count_min, transaction_count_max, transaction_count_step):

    while len(transactions) < transaction_count:
        transactions.append(transaction)

    diffs = list()
    for _ in range(iters):
        # Start monitoring CPU usage
        cpu_start = resource.getrusage(resource.RUSAGE_SELF)[0]

        process(transactions)

        # Stop monitoring CPU usage
        cpu_end = resource.getrusage(resource.RUSAGE_SELF)[0]

        # Calculate the difference between the start and end points
        cpu_diff = cpu_end - cpu_start

        diffs.append(cpu_diff)

    _min = min(diffs)
    _max = max(diffs)
    _median = statistics.median(diffs)
    _mean = statistics.mean(diffs)
    logging.info(f"{transaction_count},{iters},{_median},{_mean},{_min},{_max}")

