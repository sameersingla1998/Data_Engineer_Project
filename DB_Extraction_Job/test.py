import pandas as pd # type: ignore
data = [
                    (1, 'Alice', 'alice@example.com', 30, '2022-06-15', 100000.00, 5000.00),
                    (2, 'Bob', 'bob@example.com', 25, '2021-09-20', 150000.00, 10000.00),
                    (3, 'Charlie', 'charlie@example.com', 40, '2020-12-05', 250000.00, 20000.00),
                    (4, 'David', 'david@example.com', 35, '2019-04-22', 50000.00, 0.00),
                    (5, 'Eve', 'eve@example.com', 28, '2023-03-10', 300000.00, 15000.00)
                ]

df = pd.DataFrame(data, columns=['user_id', 'name', 'email', 'age', 'signup_date', 'balance', 'debt'])

age_bins = [0, 20, 30, 40, 50, 100]  # Age ranges
age_labels = ['0-20', '21-30', '31-40', '41-50', '50+']  # Age group labels
df['age_group'] = pd.cut(df['age'], bins=age_bins, labels=age_labels)

# Define balance groups in a simpler way
balance_bins = [0, 50000, 100000, 200000, 300000, 500000]  # Balance ranges
balance_labels = ['0-50K', '50K-100K', '100K-200K', '200K-300K', '300K+']  # Balance group labels
df['balance_group'] = pd.cut(df['balance'], bins=balance_bins, labels=balance_labels)

df = df[df['age'].between(0, 80)]
body=df.to_json(orient='records')
print(body)