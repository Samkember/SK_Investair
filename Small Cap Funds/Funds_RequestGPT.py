import pandas as pd 



fundFilePath = r'C:\Users\Sam\Documents\SK_Investair\Small Cap Funds\funds_list.csv'

def ReadFundInformation():
    df = pd.read_csv(fundFilePath, delimiter=',')

    fundList = df.iloc[:,0].tolist()

    return fundList

def RequestGPT(fund):



def GetFundInformation(fundList):
    



if __name__ == "__main__":
    fundList = ReadFundInformation()





