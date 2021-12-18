import requests
import os
import pandas as pd
from datetime import datetime
import csv

def norm(qty, asset):
    dps = 6 if (asset == "USDC" or asset == "USDT") else 18
    tmp = qty.rjust(dps + 1, "0")
    return (tmp[0:-dps] + "." + tmp[-dps:]).rstrip("0").rstrip(".")

def main(in_class="mined", out_class="remove funds"):

    wallet = os.environ["ETH_WALLET"]
    csv_path = "transactions.csv"

    inc = 100
    n = 0
    new_c = 0

    while True:
        # Try to read existing csv if available
        try:
            df = pd.read_csv(csv_path)
        except (FileNotFoundError, pd.errors.EmptyDataError, csv.Error):
            df = None

        # get the next batch of transactions
        try:
            resp = requests.get(
                f"https://api.zksync.io/api/v0.1/account/{wallet}/history/{n}/{inc}"
            )
            resp.raise_for_status()
        except requests.exceptions.HTTPError as err:
            raise SystemExit(err)

        # get json response
        j = resp.json()

        # Repeat until response empty
        if not j:
            break

        all_trx = []

        # For every transaction in the batch
        for t in j:

            # clean transaction string
            tx_hash = t["hash"].replace("sync-tx:", "")

            # skip existing transactions to avoid duplicates
            try:
                if df.isin([tx_hash]).any().any():
                    # just for debugging
                    # print(f"Found duplicate: {tx_hash}, skipping!")
                    continue
            except AttributeError:
                pass

            # transaction detail shortcut
            td = t["tx"]

            # init structure for csv
            my_trx = {
                "Type": None,
                "Buy Quantity": None,
                "Buy Asset": None,
                "Buy Value": None,
                "Sell Quantity": None,
                "Sell Asset": None,
                "Sell Value": None,
                "Fee Quantity": None,
                "Fee Asset": None,
                "Fee Value": None,
                "Wallet": "ZKSync",
                "Timestamp": None,
            }

            my_date = t["created_at"]

            # convert to datetime object and then to format needed
            dto = datetime.strptime(my_date, "%Y-%m-%dT%H:%M:%S.%f%z")
            my_trx["Timestamp"] = dto.strftime("%d/%m/%Y %H:%M:%S")

            # trx coming in
            try:

                # Withdrawal to mainnet
                if (td["type"] == "Withdraw"):
                    my_trx["Type"] = "Withdrawal"
                    my_trx["Sell Quantity"] = norm(td["amount"], td["token"])
                    my_trx["Sell Asset"] = td["token"]
                    my_trx["Fee Quantity"] = norm(td["fee"], td["token"])
                    my_trx["Fee Asset"] = td["token"]

                elif (td["type"] == "Transfer"):
                    if td["to"].lower() == wallet.lower():
                        # Some fee payments look like zero value transfers to oneself with fees
                        if (td["from"].lower() == wallet.lower() and td["amount"] == "0" and td["fee"] != "0"):
                            my_trx["Type"] = "Spend"
                            my_trx["Sell Quantity"] = 0
                            my_trx["Sell Asset"] = td["token"]
                            my_trx["Fee Quantity"] = norm(td["fee"], td["token"])
                            my_trx["Fee Asset"] = td["token"]
                        # Incoming tx
                        else:
                            my_trx["Type"] = "Income"
                            my_trx["Buy Quantity"] = norm(td["amount"], td["token"])
                            my_trx["Buy Asset"] = td["token"]
                    # Outgoing tx
                    else:
                        my_trx["Type"] = "Spend"
                        my_trx["Sell Quantity"] = norm(td["amount"], td["token"])
                        my_trx["Sell Asset"] = td["token"]
                        my_trx["Fee Quantity"] = norm(td["fee"], td["token"])
                        my_trx["Fee Asset"] = td["token"]

                # Swaps are too hard to populate: need to do it manually (e.g. token types are indexed)
                elif (td["type"] == "Swap"):
                    my_trx["Type"] = "Trade"

                # Ignore others like ChangePubKey
                # Also, I don't know how deposits from L1 are categorised - I've never done one.
                else:
                    print(f"Warning: ignoring transaction type {td['type']}")
                    continue

            except KeyError:
                continue

            my_trx["operationId"] = tx_hash

            all_trx.append(my_trx)

            # count new transactions
            new_c += 1

        # Append to existing df or create new
        if df is None or df.empty:
            df = pd.DataFrame(all_trx)
        else:
            df = df.append(pd.DataFrame(all_trx))

        # Safe df to csv
        df.to_csv(csv_path, index=False)

        # Increment count of processed transactions
        n += inc

        print(f"Processed: {n:9d} Transactions. In csv: {len(df.index):9d}")

    print(f"Done! Added {new_c} new transactions to csv!")


if __name__ == "__main__":
    main()
