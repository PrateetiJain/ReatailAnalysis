from lib.DataReader import read_customers,read_orders 


def test_read_customers_df(spark):
    customer_count = read_customers(spark,"LOCAL")
    assert customer_count.count() == 12435
    

def test_read_orders_df(spark):
    orders_count = read_orders(spark,"LOCAL")
    assert orders_count.count() == 68884