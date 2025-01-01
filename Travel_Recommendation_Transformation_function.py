import json
import boto3
import pandas as pd

def lambda_handler(event, context):
    # Initialize the S3 client
    s3 = boto3.client("s3")

    try:
        # Fetch the raw data from S3
        obj = s3.get_object(Bucket="weather-reports-data", Key="raw/weather_data.json")
        raw_data = json.loads(obj['Body'].read())

        # Ensure the raw data is non-empty
        if not raw_data:
            raise ValueError("Raw data from S3 is empty or invalid.")

        # Create a Pandas DataFrame from the raw data
        df_new = pd.DataFrame(raw_data)

        # Check if DataFrame is empty
        if df_new.empty:
            raise ValueError("The DataFrame created from raw data is empty.")

        # Function to apply alerts based on Threshold-Conditions
        def get_alert(row):
            alerts = []
            if row.get('wind_speed', 0) > 20:
                alerts.append("High Wind Warning")
            if row.get('humidity', 0) > 90:
                alerts.append("Humidity Warning")
            if row.get('temperature', 0) > 40:
                alerts.append("Heat Advisory")
            if row.get('temperature', 0) < -10:
                alerts.append("Cold Advisory")
            if row.get('sea_level') and row['sea_level'] > 1020:
                alerts.append("Tsunami Warning")
            return ', '.join(alerts) if alerts else None

        
        df_new['Alert'] = df_new.apply(get_alert, axis=1)

        #Filter the rows having Non-Null values
        df_alerts = df_new[df_new['Alert'].notnull()]
   
        # Normalize columns 
        columns_to_normalize = ['temperature', 'humidity', 'wind_speed']
        normalized_columns = []

        # Loop through each column to normalize and store normalized column names
        for column in columns_to_normalize:
            min_val = df_new[column].min()
            max_val = df_new[column].max()
            normalized_col_name = f"{column}_normalized"
            df_new[normalized_col_name] = (df_new[column] - min_val) / (max_val - min_val)
            normalized_columns.append(normalized_col_name)

        # Calculate Comfort Index 
        comfort_index = []
        for _, row in df_new.iterrows():
            temperature_score = 0.4 * (1 - abs(row['temperature_normalized'] - 0.5))
            humidity_score = 0.3 * (1 - abs(row['humidity_normalized'] - 0.5))
            wind_speed_score = 0.2 * (1 - row['wind_speed_normalized'])
            weather_condition_score = 0.1 if "clear" in row['weather'].lower() else 0.05
            comfort_index.append(
                temperature_score + humidity_score + wind_speed_score + weather_condition_score
            )

        # Assign the Comfort Index to the DataFrame
        df_new['Comfort_Index'] = comfort_index

        #Sort columns based on Comfort Index in Descending order
        df_new_final = df_new[['city','Comfort_Index']].sort_values(by='Comfort_Index',ascending=False)

        #Assign Ranking to cities based on Comfort-Index as ComfortIndex inversely proportional to Ranking 
        df_new_final['Ranking'] = df_new['Comfort_Index'].rank(ascending=False, method='dense').astype(int)

        # Save only the necessary columns for the final output
        df_Comfort_Ranking = df_new_final[['Ranking', 'city', 'Comfort_Index']]

        s3.put_object(
            Bucket="weather-reports-data",
            Key="processed/Travel_comfort_cities/comfort_cities.csv",
            Body=df_Comfort_Ranking.to_csv(index=False, header=True)
        )


        s3.put_object(
            Bucket="weather-reports-data",
            Key="processed/Weather-Alert-cities/weather_alerts.csv",
            Body=df_alerts.to_csv(header=True)
        )

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Weather data processed and saved successfully."})
        }

    except Exception as e:
        # Log the error for debugging
        print(f"Error: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
