This assesment represent how to utilize GCP resource for making binace data pipline both in batch and real-time data for the following symbols : BTCUSDT, AAVEUSDT, STXUSDT and ARBUSDT

# Batch data pipeline:

use **cloud composer** to utilize airflow, here is the DAG from figure 1 shown below
![image](https://github.com/Tinasavasapphavat/bittaza-test/assets/115886539/533f7819-7229-4706-85f8-e5e46fa4a9a6)
since we have symbols containing BTCUSDT, AAVEUSDT, STXUSDT and ARBUSDT. So, we store data's to different tables first and start orchresting the pipeline. Here is the step of the pipeline
1. append the data to scd dataset(figure 2), which is made to keep historical data in case that the user might need to visualize them.
2. extract data using api request to store data's to data warehouse in figuere 2.
3. check that our data is uploaded successfully

noted: all data schema's are not different from the figure below 

![image](https://github.com/Tinasavasapphavat/bittaza-test/assets/115886539/56365f4d-e3ef-402c-98fe-5dcaa7dfe4e5) 



figure 2

![image](https://github.com/Tinasavasapphavat/bittaza-test/assets/115886539/65099817-e812-472f-a941-ff2bd928400a)




figure 2.1 represents how the data looks like
![image](https://github.com/Tinasavasapphavat/bittaza-test/assets/115886539/68fffe1f-df92-4c81-abdb-31decf0e49b0)


figure 3

![image](https://github.com/Tinasavasapphavat/bittaza-test/assets/115886539/1abbecd9-c992-4bf5-957d-922793f2c3b1)
## noted: since there is a location restricted issue(we must be US resident), so the pipeline can orchrestrate only couple times


# Real-time pipeline
connect Binance websocket(trade stream)
according to the limitation of gcp service on my account and ingesting time is limited for 10 minutes only, Using **cloud compoeser** will be triggered every 10 minutes or using VM will be used to execute from the code you can see the code  in **/func/stream_binance** 

![image](https://github.com/Tinasavasapphavat/bittaza-test/assets/115886539/35a93804-5522-46fc-a837-137bfd32817c)

the result will be shown below

![image](https://github.com/Tinasavasapphavat/bittaza-test/assets/115886539/d02ed578-cb3e-43e6-847e-a004871b1777)

here is the result of the table
![image](https://github.com/Tinasavasapphavat/bittaza-test/assets/115886539/ae686afa-84b3-463a-8695-6066f7178269)

here is a dashboard to display OHLC, since the api has been changed to kline chart for one minute interval
https://lookerstudio.google.com/reporting/ed489ffa-eccc-4a80-9376-5b6bf1290a62

# noted: For OHLC dashboard pipeline will be not different but api request from @trade to @kline









