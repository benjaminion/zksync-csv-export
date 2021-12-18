# zksync-csv-export
This is a tool to extract zksync transactions from an Ethereum wallet into a CSV file.

### Modifications

I've modified the original as follows:

  - output is now in [BittyTax](https://github.com/BittyTax/BittyTax) format
  - change the nasty US date format
  - correct the number of decimal places used for USDT and USDC
  - handle more transaction types correctly
  - handle fee-only transactions

This is not intended to be comprehensive, but it works well enough for me.

Sorry for making it uglier: I don't Python.
