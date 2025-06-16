import streamlit as st
import altair as alt
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, sum as snowflake_sum, when, lit, to_date, current_date, min as snowflake_min

st.set_page_config(layout="wide", initial_sidebar_state="expanded")

session = get_active_session()

def load_data():
    return session.sql("select * from cdc_prod.analytics.customer_purchase_summary")

def empty_space():
    st.markdown("<br><br>", unsafe_allow_html=True)

def calculate_customer_spending(data):
    purchase_data = data.filter(col("TRANSACTION_CATEGORY") == "Purchase")
    total_purchase = purchase_data.group_by("CUSTOMER_ID").agg(snowflake_sum("TOTAL_PRICE").alias("TOTAL_PRICE"))
    total_purchase = total_purchase.with_column(
        "SPEND_STATUS",
        when(col("TOTAL_PRICE") < 4000, lit("Low Spenders"))
        .when((col("TOTAL_PRICE") >= 4000) & (col("TOTAL_PRICE") < 6000), lit("Medium Spenders"))
        .otherwise(lit("High Spenders"))
    )
    return total_purchase

def categorize_by_age(data):
    return data.with_column(
        "AGE_GROUP",
        when((col("CUSTOMER_AGE") >= 18) & (col("CUSTOMER_AGE") <= 25), lit("Gen Z"))
        .when((col("CUSTOMER_AGE") >= 26) & (col("CUSTOMER_AGE") <= 41), lit("Millennials"))
        .when((col("CUSTOMER_AGE") >= 42) & (col("CUSTOMER_AGE") <= 57), lit("Gen X"))
        .when((col("CUSTOMER_AGE") >= 58) & (col("CUSTOMER_AGE") <= 76), lit("Boomers"))
        .when((col("CUSTOMER_AGE") >= 77) & (col("CUSTOMER_AGE") <= 90), lit("Silent Generation"))
    )

def display_spend_status_counts(customer_spending):
    positive_spending = customer_spending.filter(col("TOTAL_PRICE") > 0)
    spend_status_counts = positive_spending.group_by("SPEND_STATUS").count().to_pandas()
    spend_status_counts = spend_status_counts.set_index("SPEND_STATUS").reindex(
        ["High Spenders", "Medium Spenders", "Low Spenders"], fill_value=0)

    col1, col2, col3 = st.columns(3)
    col1.metric("High Spenders", spend_status_counts.loc["High Spenders", "COUNT"])
    col2.metric("Medium Spenders", spend_status_counts.loc["Medium Spenders", "COUNT"])
    col3.metric("Low Spenders", spend_status_counts.loc["Low Spenders", "COUNT"])

def apply_filters(data, customer_spending):
    spend_status = st.sidebar.selectbox("Select Customer Spend Status", options=["All", "Low Spenders", "Medium Spenders", "High Spenders"])
    age_group = st.sidebar.selectbox("Select Age Group", options=["All", "Gen Z", "Millennials", "Gen X", "Boomers", "Silent Generation"])

    earliest_date = data.select(snowflake_min(col("TRANSACTION_DATE"))).collect()[0][0]
    start_date = st.sidebar.date_input("Start Date", value=earliest_date)
    end_date = st.sidebar.date_input("End Date", value=session.sql("SELECT CURRENT_DATE()").collect()[0][0])

    if end_date > session.sql("SELECT CURRENT_DATE()").collect()[0][0]:
        st.sidebar.warning("End Date cannot be in the future. Setting End Date to today's date.")
        end_date = session.sql("SELECT CURRENT_DATE()").collect()[0][0]

    if start_date < earliest_date:
        st.sidebar.warning(f"Start Date cannot be earlier than {earliest_date}. Setting Start Date to the earliest available date.")
        start_date = earliest_date

    customer_id = st.sidebar.selectbox("Select Customer ID", options=["All"] + list(data.select("CUSTOMER_ID").distinct().to_pandas()["CUSTOMER_ID"]))
    transaction_category = st.sidebar.selectbox("Select Transaction Category", options=["All", "Purchase", "Refund"])

    start_date = to_date(lit(start_date))
    end_date = to_date(lit(end_date))

    data = data.filter((col("TRANSACTION_DATE") >= start_date) & (col("TRANSACTION_DATE") <= end_date))

    if customer_id != "All":
        data = data.filter(col("CUSTOMER_ID") == customer_id)

    if transaction_category != "All":
        data = data.filter(col("TRANSACTION_CATEGORY") == transaction_category)

    if spend_status != "All":
        customer_spending = customer_spending.filter(col("SPEND_STATUS") == spend_status)
        data = data.filter(col("CUSTOMER_ID").isin(customer_spending.select("CUSTOMER_ID")))

    if age_group != "All":
        data = data.filter(col("AGE_GROUP") == age_group)

    if customer_id != "All" and data.filter(col("TRANSACTION_CATEGORY") == "Purchase").count() == 0:
        st.warning(f"Customer ID {customer_id} does not have any purchases.")

    return data, spend_status

def display_metrics(data):
    total_spent = data.filter(col("TRANSACTION_CATEGORY") == "Purchase").agg(snowflake_sum("TOTAL_PRICE")).collect()[0][0]
    if total_spent is None:
        total_spent = 0.0

    col4 = st.columns(1)[0]
    col4.metric("Total Spent", f"${total_spent:,.2f}")

def display_charts(data):
    data_pd = data.to_pandas()

    col1, col2 = st.columns(2)
    total_items_data = data_pd.groupby(["TRANSACTION_DATE", "TRANSACTION_CATEGORY"]).agg({"QUANTITY": "sum"}).reset_index().rename(columns={"QUANTITY": "TOTAL_ITEMS"})

    with col1:
        histogram = alt.Chart(total_items_data).mark_bar().encode(
            x=alt.X("TRANSACTION_DATE:T", axis=alt.Axis(title="Transaction Date")),
            y=alt.Y("TOTAL_ITEMS:Q", axis=alt.Axis(title="Total Items")),
            color=alt.Color("TRANSACTION_CATEGORY:N", scale=alt.Scale(domain=["Purchase", "Refund"])),
            tooltip=["TRANSACTION_DATE", "TOTAL_ITEMS", "TRANSACTION_CATEGORY"]
        ).properties(title="Daily Transactions").interactive()
        st.altair_chart(histogram, use_container_width=True)

    card_data = data_pd.groupby("TRANSACTION_CARD").agg({"TRANSACTION_ID": "count"}).reset_index().rename(columns={"TRANSACTION_ID": "TRANSACTION_COUNT"})

    with col2:
        card_chart = alt.Chart(card_data).mark_bar().encode(
            x=alt.X("TRANSACTION_CARD:N", axis=alt.Axis(title="Transaction Card", labelAngle=-45)),
            y=alt.Y("TRANSACTION_COUNT:Q", axis=alt.Axis(title="Transaction Count")),
            color="TRANSACTION_CARD:N",
            tooltip=["TRANSACTION_CARD", "TRANSACTION_COUNT"]
        ).properties(title="Transactions by Card Type").interactive()
        st.altair_chart(card_chart, use_container_width=True)

    empty_space()

    col3, col4 = st.columns(2)
    purchases_by_category = data_pd.groupby("PRODUCT_CATEGORY").agg({"TRANSACTION_ID": "count"}).reset_index().rename(columns={"TRANSACTION_ID": "PURCHASE_COUNT"})
    with col3:
        category_chart = alt.Chart(purchases_by_category).mark_bar().encode(
            x=alt.X("PRODUCT_CATEGORY:N", axis=alt.Axis(title="Product Category", labelAngle=-45)),
            y=alt.Y("PURCHASE_COUNT:Q", axis=alt.Axis(title="Purchase Count")),
            color="PRODUCT_CATEGORY:N",
            tooltip=["PRODUCT_CATEGORY", "PURCHASE_COUNT"]
        ).properties(title="Transactions by Category").interactive()
        st.altair_chart(category_chart, use_container_width=True)

    merchant_data = data_pd.groupby("MERCHANT_NAME").agg({"TRANSACTION_ID": "count", "TOTAL_PRICE": "sum"}).reset_index().rename(columns={"TRANSACTION_ID": "TRANSACTION_COUNT"})

    with col4:
        bubble_chart = alt.Chart(merchant_data).mark_circle().encode(
            x=alt.X("MERCHANT_NAME:N", axis=alt.Axis(title="Merchant Name", labelAngle=-45)),
            y=alt.Y("TRANSACTION_COUNT:Q", axis=alt.Axis(title="Transaction Count")),
            size=alt.Size("TOTAL_PRICE:Q", legend=alt.Legend(title="Total Price")),
            color="MERCHANT_NAME:N",
            tooltip=["MERCHANT_NAME", "TRANSACTION_COUNT", "TOTAL_PRICE"]
        ).properties(title="Transactions by Merchant").interactive()
        st.altair_chart(bubble_chart, use_container_width=True)

def display_promotions(data, customer_spending, spend_status):
    if spend_status not in ["Low Spenders", "Medium Spenders", "High Spenders"]:
        return

    spenders = customer_spending.filter(col("SPEND_STATUS") == spend_status).select("CUSTOMER_ID")
    spender_data = data.filter(col("CUSTOMER_ID").isin([row["CUSTOMER_ID"] for row in spenders.collect()]))

    if spender_data.count() == 0:
        st.sidebar.subheader("Promotions")
        st.sidebar.write(f"No {spend_status.lower()} found.")
        return

    merchant_data = spender_data.group_by("MERCHANT_NAME").agg(snowflake_sum("TOTAL_PRICE").alias("TOTAL_PRICE"))
    sorted_merchant_data = merchant_data.sort(col("TOTAL_PRICE").desc())
    highest_transaction_merchant_row = sorted_merchant_data.first()

    if highest_transaction_merchant_row is None:
        st.sidebar.subheader("Promotions")
        st.sidebar.write(f"No {spend_status.lower()} found.")
        return

    highest_transaction_merchant = highest_transaction_merchant_row["MERCHANT_NAME"]

    if spend_status == "Low Spenders":
        credit = 800
        required_spend = 1500
    elif spend_status == "Medium Spenders":
        credit = 600
        required_spend = 2000
    else:
        credit = 400
        required_spend = 2500

    st.sidebar.subheader("Promotions")
    st.sidebar.info(f"**Exclusive promotion for our {spend_status.lower()}:** **\${credit}** credit for customers who spend **\${required_spend}** at **{highest_transaction_merchant}** over a period of 6 months.")

def main():
    data = load_data()
    data = categorize_by_age(data)
    customer_spending = calculate_customer_spending(data)
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