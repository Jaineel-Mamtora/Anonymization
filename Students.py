import sys, csv, collections, hashlib, argparse
import pandas as pd

def hashedTable(df1, column, index, df_new, filename):
    r, c = df1.shape
    l1 = []
    l2 = []
    hashed = column + " Hashed"
    for i in range(index-1,index):
        for j in range(r):
            x = df1.iloc[j][i]
            x1 = str(x)
            l1.append(x1)
            sh = hashlib.sha256(x1.encode()).hexdigest()
            l2.append(sh)
    df2 = pd.DataFrame({ column: l1 })
    df3 = pd.DataFrame({ hashed : l2 })
    df_new1 = df2.join(df3)
    if(df_new.empty):
        df_new1.to_csv("hashkeys_for_" + (str(filename)), index=None)
        return df_new1
    else:
        df_new = df_new.join(df_new1)
        df_new.to_csv("hashkeys_for_" + (str(filename)), index=None)
        return df_new


def Anonymize(df1, column, df_new):
    r, c = df1.shape
    for i in range(len(column)):
        df1 = df1.drop(str(column[i]), 1)
        df_new = df_new.drop(str(column[i]), 1)
    return df1, df_new

def backOriginal(d):
    file1 = input("Enter the name of hashed file: ")
    df2 = pd.read_csv(file1)
    d1 = dict(zip(range(1, len(df2.columns) + 1), df2.columns))
    file2 = input("Enter the name of the anonymized file: ")
    df3 = pd.read_csv(file2)
    d2 = dict(zip(range(1, len(df3.columns) + 1), df3.columns))
    v1 = list(d1.values())
    v2 = list(d2.values())


def main():
    filename = None
    indices = []
    column = []
    no = int(sys.argv[2])
    values = str(sys.argv[3]).split(",")
    if len(sys.argv) > 1:
        filename = sys.argv[1]
    else:
        filename = input("Enter the name of the csv file: ")
    print("Filename :", filename)
    df1 = pd.read_csv(filename)
    li = list(df1)
    length = len(li)
    print("All the columns are: ")
    d = dict(zip(range(1, len(df1.columns) + 1), df1.columns))
    print(d)
    if(no == len(values)):
        print("No of columns :", no)
        print("Columns read :", values)
    elif(no > len(values)):
        print("Number of columns greater than number of names of columns specified.")
        sys.exit
    else:
        print("Number of columns less than number of names of columns specified.")
        sys.exit
    print("Accessing", filename, "file....")
    df_new = pd.DataFrame()
    for i in range(no):
        indices.append(list(d.keys())[list(d.values()).index(values[i])])
    for i in range(len(indices)):
        column.append(d.get(indices[i]))
    print("Creating hashkeys_for_" + (str(filename)) ,"file....")
    for i in range(len(indices)):
        df_new = hashedTable(df1, column[i], indices[i], df_new, filename)
    print("hashkeys_for_" + (str(filename)) ,"created successfully!")
    print("Creating hashed_" + (str(filename)) ,"file....")
    for i in range(len(li)):
        df4, hashed = Anonymize(df1, column, df_new)
    hashed = hashed.join(df4)
    hashed.to_csv("hashed_" + (str(filename)), index=None)
    print("hashed_" + (str(filename)) ,"created successfully!")

if __name__ == '__main__':
    main()