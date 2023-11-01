import os
from subgrounds import Subgrounds

from dotenv import load_dotenv

load_dotenv()


sg = Subgrounds.from_pg_key(os.getenv("PG_API_KEY"))
deployment_id = "QmSGDNPW2iAwwgh4He5eeBTaSLeTJpWKzuNfP2McAdBVq1"

subgraph = sg.load_subgraph(
    # univ3 substreams subgraph
    # https://thegraph.com/explorer/subgraphs/HUZDsRpEVP2AvzDCyzDHtdc64dyDxx8FQjzsmqSg4H3B?view=Overview&chain=arbitrum-one
    f"https://api.playgrounds.network/v1/proxy/deployments/id/{deployment_id}"
)

END_BLOCK = 18473464
START_BLOCK = END_BLOCK - 7200  # 7200 blocks in a day (12blocks/second)

swaps_query = subgraph.Query.swaps(
    first=5000,
    block={"number": END_BLOCK},
    where={"_change_block": {"number_gte": START_BLOCK}},
)

df = sg.query_df(
    # swaps_query # use this or the list below
    [
        # - swaps_ hash, timestamp, to, from, blockNumber, tokenIn_id, amountIn, tokenOut_id, amountOut, amountInUSD, amountOutUSD, pool_id
        swaps_query.hash,
        swaps_query.timestamp,
        swaps_query.logIndex,
        swaps_query.to,
        swaps_query._select("from"),
        swaps_query.blockNumber,
        swaps_query.tokenIn._select("id"),
        swaps_query.tokenIn._select("symbol"),
        swaps_query.amountIn,
        swaps_query.tokenOut._select("id"),
        swaps_query.tokenOut._select("symbol"),
        swaps_query.amountOut,
        swaps_query.amountInUSD,
        swaps_query.amountOutUSD,
        swaps_query.pool._select("id"),
    ]
)

print(df.columns, df.shape)

print(
    df[
        [
            "swaps_blockNumber",
            "swaps_logIndex",
            "swaps_tokenIn_symbol",
            "swaps_tokenOut_symbol",
        ]
    ]
)
