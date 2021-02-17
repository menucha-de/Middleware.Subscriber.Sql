# Middleware.Subscriber.Sql

Choose custom subscriber and enter this URL and activate the subscriber 

    sql:///?connection=jdbc%3Ah2%3Amem%3Atest_db&table=test_table&date=column_date&epc=column_epc&init=true&drop=true&storage=test_storage

You should now be able to fetch the CSV from this URL

[test_storage](http://raspberrypi/rest/middleware/subscriber/sql/test_storage)
