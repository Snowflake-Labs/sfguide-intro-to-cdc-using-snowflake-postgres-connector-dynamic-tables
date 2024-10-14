import streamlit as st
import altair as alt
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, sum as snowflake_sum, when, lit, to_date, current_date, \
    min as snowflake_min

st.set_page_config(layout="wide", initial_sidebar_state="expanded")

session = get_active_session()

def load_data():
    query = """
    SELECT
        customer_id,
        customer_age,
        product_id,
        product_name,
        product_category,
        merchant_id,
        merchant_name,
        merchant_category,
        transaction_date,
        transaction_time,
        quantity,
        total_price,
        transaction_card,
        transaction_category,
        transaction_id
    FROM
        CONNECTORS_DEST_DB."raw_cdc"."customer_purchase_summary"
    """
    return session.sql(query)


# add empty space in the streamlit app
def empty_space():
    st.markdown("<br><br>", unsafe_allow_html=True)


# calculate customer spending and categorize spenders
def calculate_customer_spending(data):
    # filter only purchase transactions
    purchase_data = data.filter(col("TRANSACTION_CATEGORY") == "Purchase")

    # aggregate total price per customer
    total_purchase = purchase_data.group_by("CUSTOMER_ID").agg(snowflake_sum("TOTAL_PRICE").alias("TOTAL_PRICE"))

    # categorize customers based on their total spending
    total_purchase = total_purchase.with_column(
        "SPEND_STATUS",
        when(col("TOTAL_PRICE") < 5000, lit("Low Spenders"))
        .when((col("TOTAL_PRICE") >= 5000) & (col("TOTAL_PRICE") < 7000), lit("Medium Spenders"))
        .otherwise(lit("High Spenders"))
    )

    return total_purchase


# display spend status counts in the streamlit app
def display_spend_status_counts(customer_spending):
    # filter out customers with zero spending
    positive_spending = customer_spending.filter(col("TOTAL_PRICE") > 0)

    # count the number of customers in each spend status category
    spend_status_counts = positive_spending.group_by("SPEND_STATUS").count().to_pandas()

    # ensure all spend status categories are represented, even if count is zero
    spend_status_counts = spend_status_counts.set_index("SPEND_STATUS").reindex(
        ["High Spenders", "Medium Spenders", "Low Spenders"], fill_value=0)

    # display the counts in three columns
    col1, col2, col3 = st.columns(3)
    col1.metric("High Spenders", spend_status_counts.loc["High Spenders", "COUNT"])
    col2.metric("Medium Spenders", spend_status_counts.loc["Medium Spenders", "COUNT"])
    col3.metric("Low Spenders", spend_status_counts.loc["Low Spenders", "COUNT"])


# apply filters to the data based on user input from the sidebar
def apply_filters(data, customer_spending):
    # get user-selected spend status from sidebar
    spend_status = st.sidebar.selectbox("Select Customer Spend Status",
                                        options=["All", "Low Spenders", "Medium Spenders", "High Spenders"])

    # get the earliest transaction date from the data
    earliest_date = data.select(snowflake_min(col("TRANSACTION_DATE"))).collect()[0][0]

    # get user-selected date range from sidebar
    start_date = st.sidebar.date_input("Start Date", value=earliest_date)
    end_date = st.sidebar.date_input("End Date", value=session.sql("SELECT CURRENT_DATE()").collect()[0][0])

    # ensure end date is not in the future
    if end_date > session.sql("SELECT CURRENT_DATE()").collect()[0][0]:
        st.sidebar.warning("End Date cannot be in the future. Setting End Date to today's date.")
        end_date = session.sql("SELECT CURRENT_DATE()").collect()[0][0]

    # ensure start date is not earlier than the earliest available date
    if start_date < earliest_date:
        st.sidebar.warning(
            f"Start Date cannot be earlier than {earliest_date}. Setting Start Date to the earliest available date.")
        start_date = earliest_date

    # get user-selected customer ID and transaction category from sidebar
    customer_id = st.sidebar.selectbox("Select Customer ID", options=["All"] + list(
        data.select("CUSTOMER_ID").distinct().to_pandas()["CUSTOMER_ID"]))
    transaction_category = st.sidebar.selectbox("Select Transaction Category", options=["All", "Purchase", "Refund"])

    # convert dates to Snowflake date format
    start_date = to_date(lit(start_date))
    end_date = to_date(lit(end_date))

    # apply date range filter
    data = data.filter((col("TRANSACTION_DATE") >= start_date) & (col("TRANSACTION_DATE") <= end_date))

    # apply customer ID filter if not "All"
    if customer_id != "All":
        data = data.filter(col("CUSTOMER_ID") == customer_id)

    # apply transaction category filter if not "All"
    if transaction_category != "All":
        data = data.filter(col("TRANSACTION_CATEGORY") == transaction_category)

    # apply spend status filter if not "All"
    if spend_status != "All":
        customer_spending = customer_spending.filter(col("SPEND_STATUS") == spend_status)
        data = data.filter(col("CUSTOMER_ID").isin(customer_spending.select("CUSTOMER_ID")))

    # warn if selected customer ID has no purchases
    if customer_id != "All" and data.filter(col("TRANSACTION_CATEGORY") == "Purchase").count() == 0:
        st.warning(f"Customer ID {customer_id} does not have any purchases.")

    return data, spend_status


# display metrics in the streamlit app
def display_metrics(data):
    # calculate total amount spent on purchases
    total_spent = data.filter(col("TRANSACTION_CATEGORY") == "Purchase").agg(snowflake_sum("TOTAL_PRICE")).collect()[0][
        0]
    if total_spent is None:
        total_spent = 0.0
    col4, col5 = st.columns(2)
    col4.metric("Total Spent", f"${total_spent:,.2f}")


# display various charts in the streamlit app
def display_charts(data):
    data_pd = data.to_pandas()

    col1, col2 = st.columns(2)

    # prepare data for daily transactions chart
    total_items_data = data_pd.groupby(["TRANSACTION_DATE", "TRANSACTION_CATEGORY"]).agg({
        "QUANTITY": "sum"
    }).reset_index().rename(columns={"QUANTITY": "TOTAL_ITEMS"})

    with col1:
        histogram = alt.Chart(total_items_data).mark_bar().encode(
            x=alt.X("TRANSACTION_DATE:T", axis=alt.Axis(title="Transaction Date", labelAngle=-45)),
            y=alt.Y("TOTAL_ITEMS:Q", axis=alt.Axis(title="Total Items")),
            color=alt.Color("TRANSACTION_CATEGORY:N", scale=alt.Scale(domain=["Purchase", "Refund"]),
                            legend=alt.Legend(title="Transaction Category")),
            tooltip=["TRANSACTION_DATE", "TOTAL_ITEMS", "TRANSACTION_CATEGORY"]
        ).properties(
            title="Daily Transactions"
        ).interactive()
        st.altair_chart(histogram, use_container_width=True)

    # prepare data for transactions by card type chart
    card_data = data_pd.groupby("TRANSACTION_CARD").agg({
        "TRANSACTION_ID": "count"
    }).reset_index().rename(columns={"TRANSACTION_ID": "TRANSACTION_COUNT"})

    with col2:
        card_chart = alt.Chart(card_data).mark_bar().encode(
            x=alt.X("TRANSACTION_CARD:N", axis=alt.Axis(title="Transaction Card", labelAngle=-45)),
            y=alt.Y("TRANSACTION_COUNT:Q", axis=alt.Axis(title="Transaction Count")),
            color="TRANSACTION_CARD:N",
            tooltip=["TRANSACTION_CARD", "TRANSACTION_COUNT"]
        ).properties(
            title="Transactions by Card Type"
        ).interactive()
        st.altair_chart(card_chart, use_container_width=True)

    empty_space()

    col3, col4 = st.columns(2)

    # prepare data for transactions by product category chart
    purchases_by_category = data_pd.groupby("PRODUCT_CATEGORY").agg({
        "TRANSACTION_ID": "count"
    }).reset_index().rename(columns={"TRANSACTION_ID": "PURCHASE_COUNT"})

    with col3:
        category_chart = alt.Chart(purchases_by_category).mark_bar().encode(
            x=alt.X("PRODUCT_CATEGORY:N", axis=alt.Axis(title="Product Category", labelAngle=-45)),
            y=alt.Y("PURCHASE_COUNT:Q", axis=alt.Axis(title="Purchase Count")),
            color="PRODUCT_CATEGORY:N",
            tooltip=["PRODUCT_CATEGORY", "PURCHASE_COUNT"]
        ).properties(
            title="Transactions by Category"
        ).interactive()
        st.altair_chart(category_chart, use_container_width=True)

    # prepare data for transactions by merchant chart
    merchant_data = data_pd.groupby("MERCHANT_NAME").agg({
        "TRANSACTION_ID": "count",
        "TOTAL_PRICE": "sum"
    }).reset_index().rename(columns={"TRANSACTION_ID": "TRANSACTION_COUNT"})

    with col4:
        bubble_chart = alt.Chart(merchant_data).mark_circle().encode(
            x=alt.X("MERCHANT_NAME:N", axis=alt.Axis(title="Merchant Name", labelAngle=-45)),
            y=alt.Y("TRANSACTION_COUNT:Q", axis=alt.Axis(title="Transaction Count")),
            size=alt.Size("TOTAL_PRICE:Q", scale=alt.Scale(range=[100, 1000])),
            color=alt.Color("MERCHANT_NAME:N", scale=alt.Scale(scheme="category20b")),
            opacity=alt.value(0.7),
            tooltip=["MERCHANT_NAME", "TRANSACTION_COUNT", "TOTAL_PRICE"]
        ).properties(
            title="Transactions by Merchant",
            padding={"left": 10, "right": 10, "top": 10, "bottom": 10}
        ).configure_axis(
            labelFontSize=12,
            titleFontSize=14
        ).interactive()
        st.altair_chart(bubble_chart, use_container_width=True)


# display promotions for low spenders in the streamlit app
def display_promotions(data, customer_spending, spend_status):
    if spend_status != "Low Spenders":
        return

    # get customer IDs of low spenders
    low_spenders = customer_spending.filter(col("SPEND_STATUS") == "Low Spenders").select("CUSTOMER_ID")

    # filter data for low spenders
    low_spender_data = data.filter(col("CUSTOMER_ID").isin([row["CUSTOMER_ID"] for row in low_spenders.collect()]))

    if low_spender_data.count() == 0:
        st.sidebar.subheader("Promotions")
        st.sidebar.write("No low spenders found.")
        return

    # aggregate transaction count by merchant for low spenders
    merchant_data = low_spender_data.group_by("MERCHANT_NAME").agg(
        snowflake_sum("QUANTITY").alias("TRANSACTION_COUNT")
    )

    # find the merchant with the highest transaction count
    highest_transaction_merchant = merchant_data.sort(
        col("TRANSACTION_COUNT").desc()
    ).select("MERCHANT_NAME").first()["MERCHANT_NAME"]

    st.sidebar.subheader("Promotions")
    st.sidebar.info(
        f"**Exclusive promotion for our low spenders:** \$500 credit for customers who spend \$1500 at {highest_transaction_merchant} over a period of 6 months.")


# main function to run the streamlit app
def main():
    data = load_data()
    customer_spending = calculate_customer_spending(data)

    # join customer spending data with the main data
    data = data.join(customer_spending.select("CUSTOMER_ID", "SPEND_STATUS"), on="CUSTOMER_ID", how="left")

    st.title("Customer Purchase Summary")
    data, spend_status = apply_filters(data, customer_spending)

    filtered_customer_spending = calculate_customer_spending(data)

    display_spend_status_counts(filtered_customer_spending)

    display_metrics(data)

    empty_space()

    if data.count() == 0:
        st.write("No data available for the selected filters.")
    else:
        st.subheader("Purchases")
        st.dataframe(data.to_pandas())
        empty_space()
        display_charts(data)

    display_promotions(data, customer_spending, spend_status)

    st.sidebar.empty()
    if st.sidebar.button("Refresh"):
        st.experimental_rerun()


if __name__ == "__main__":
    main()