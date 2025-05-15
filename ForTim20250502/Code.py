import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# File paths
filePathASXCompanies = r'C:\Users\HarryBox\Documents\SK_Investair\ForTim20250502\ASX_combined_final.csv'
filePathSInvestors = r'C:\Users\HarryBox\Documents\SK_Investair\ForTim20250502\all_substantial_shareholders.csv'
filePathSave = r'C:\Users\HarryBox\Documents\SK_Investair\ForTim20250502\Output.csv'


def getCompanies():
    df = pd.read_csv(filePathASXCompanies)

    # Filter for companies with a market cap under $1 billion
    filtered_df = df[df['Market Cap'] <= 1_000_000_000]

    # Find the index of the 'Open' column and drop columns after 'Open'
    open_index = df.columns.get_loc('Open')
    filtered_df = filtered_df.iloc[:, :open_index + 1]

    return filtered_df


def InvestorOutput(Companies):
    df = pd.read_csv(filePathSInvestors)

    # Prepare a list to store the results
    investor_info = []

    # Iterate through each company in CompaniesUnder1bn
    for _, company in Companies.iterrows():
        ticker = company['Ticker']

        # Find all investors associated with the current company (based on 'Ticker' matching)
        investors = df[df['Ticker'] == ticker]

        # If investors are found for this company, add them to the result
        if not investors.empty:
            for _, investor in investors.iterrows():
                # Ensure 'Shares Held (%)' is numeric
                investor['Shares Held (%)'] = pd.to_numeric(investor['Shares Held (%)'], errors='coerce')

                # Calculate 'Value of Holding' if the data is valid
                if pd.notna(company['Open']) and pd.notna(company['Shares Outstanding']) and pd.notna(investor['Shares Held (%)']):
                    value_of_holding = company['Open'] * company['Shares Outstanding'] * (investor['Shares Held (%)']) / 100
                else:
                    value_of_holding = None  # Handle invalid cases

                investor_info.append({
                    'Ticker': ticker,
                    'Company': company['Company'],
                    'Sector': company['Sector'],
                    'Investor': investor['Name'],
                    'Shares Held (%)': investor['Shares Held (%)'],
                    'Total Shares': company['Shares Outstanding'],
                    'Last Share Price': company['Open'],
                    'Value of Holding': value_of_holding
                })

    # Convert the list of results to a DataFrame
    investors_df = pd.DataFrame(investor_info)

    return investors_df


def findSInvestors(SInvestors):
    # Filter investors who have holdings in more than 5 companies
    investor_counts = SInvestors['Investor'].value_counts()
    investors_with_multiple_holdings = investor_counts[investor_counts >= 2].index

    # Filter the main dataframe to include only these investors
    filtered_investors = SInvestors[SInvestors['Investor'].isin(investors_with_multiple_holdings)]

    # Replace non-numeric values (e.g., '--') in the 'Shares' column with NaN
    filtered_investors.loc[:, 'Shares Held (%)'] = pd.to_numeric(filtered_investors['Shares Held (%)'], errors='coerce')

    # Prepare data for the heatmap (pivoting the data)
    heatmap_data = filtered_investors.pivot_table(index='Investor', columns='Company', values='Shares Held (%)', aggfunc='sum', fill_value=0)

    # Save filtered investors to CSV
    filtered_investors.to_csv(filePathSave, index=False)

    return heatmap_data


def generate_heatmap(heatmap_data):
    # Generate the heatmap
    plt.figure(figsize=(12, 8))
    sns.heatmap(heatmap_data, annot=True, cmap='YlGnBu', fmt='.2f', cbar_kws={'label': 'Shares Held'})
    plt.title("Investors Holdings in More Than 5 Companies")
    plt.xlabel("Company")
    plt.ylabel("Investor")

    # Show the plot
    plt.show()


if __name__ == "__main__":
    companies = getCompanies()
    SInvestor = InvestorOutput(companies)

    # Find the substantial investors and create the heatmap data
    heatmap_data = findSInvestors(SInvestor)

    # Generate the heatmap
    # generate_heatmap(heatmap_data)
