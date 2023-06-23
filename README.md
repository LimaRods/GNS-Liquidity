# GNS-Liquidity
This repo is dedicated to store the code used in the GNS Liquidity Project
---
Files:

- **onchain_data.py:** It contains the Flipside queries used to analyze the data utilizing Python, and the function to retrieve current crypto prices from Coingecko
- **query_data.py:** It Contains functions to retrieve Flipside data and the main function of the slippage simulator
- **data-analysis.ipynb:** notebook to run some analyses
- **app.py** Slippage simulator app built in Dash Plolty
- **analytics**: folder of some dash plolty features that I help to develop in previous jobs

If you want to run the app locally, run the following command:
`pip install -r requirements.txt`
`python3 app.py`

You can find the dashboards used in the project:
- Dune Dashboard: https://dune.com/dolfo_lima/gns-liquidity
- Flipside Dashboard: https://flipsidecrypto.xyz/Rodolfo-Lima/slippage-in-gns-pools-DGxFD4
