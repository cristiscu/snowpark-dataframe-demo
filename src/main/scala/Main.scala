import com.snowflake.snowpark._
import com.snowflake.snowpark.functions._

object Main {
  def main(args: Array[String]): Unit = {
    var session = Session.builder.configFile("profile_db.conf").create

    val v1 = makeV1(session)
    val v2 = makeV2(session, v1)
    val v = makeFinal(session, v2)
    v.show()
    //v.explain()
  }

  def makeV1(session: Session): DataFrame = {

    // the tables
    val item = session.table("item")
      .select(col("i_category"), col("i_brand"), col("i_item_sk"))
    val catalog_sales = session.table("catalog_sales")
      .select(col("cs_item_sk"), col("cs_call_center_sk"), col("cs_sold_date_sk"), col("cs_sales_price"))
    val date_dim = session.table("date_dim")
      .select(col("d_year"), col("d_moy"), col("d_date_sk"))
    val call_center = session.table("call_center")
      .select(col("cc_name"), col("cc_call_center_sk"))

    // the FROM clause, with all JOINs
    val from1 = catalog_sales
      .join(item, catalog_sales("cs_item_sk") === item("i_item_sk"))
      .join(date_dim, catalog_sales("cs_sold_date_sk") === date_dim("d_date_sk"))
      .join(call_center, catalog_sales("cs_call_center_sk") === call_center("cc_call_center_sk"))

    // the WHERE clause
    val filter1 = from1.filter(col("d_year") === 1999
        || (col("d_year") === (1999 - 1) && col("d_moy") === 12)
        || (col("d_year") === (1999 + 1) && col("d_moy") === 1))

    // all OVER clauses
    val avg_monthly_sales_window = Window.partitionBy(
      col("i_category"), col("i_brand"), col("cc_name"), col("d_year"))
    val rank_window = Window.partitionBy(
      col("i_category"), col("i_brand"), col("cc_name"))
      .orderBy(col("d_year"), col("d_moy"))

    // the GROUP BY clause
    val group1 = filter1.groupBy(
      col("i_category"), col("i_brand"), col("cc_name") , col("d_year"), col("d_moy"))

    // selection of aggregate functions
    val agg1 = group1.agg(
        sum(col("cs_sales_price")).as("sum_sales"),
        avg(sum(col("cs_sales_price"))).over(avg_monthly_sales_window).as("avg_monthly_sales"),
        rank().over(rank_window).as("rn"))

    return agg1
  }

  def makeV2(session: Session, v1: DataFrame): DataFrame = {

    // reuse the same CTE
    val v1_lag = v1.clone
      .select(col("i_category").as("lag_category"), col("i_brand").as("lag_brand"),
        col("cc_name").as("lag_name"), col("rn").as("lag_rn"), col("sum_sales").as("psum"))
    val v1_lead = v1.clone
      .select(col("i_category").as("lead_category"), col("i_brand").as("lead_brand"),
        col("cc_name").as("lead_name"), col("rn").as("lead_rn"), col("sum_sales").as("nsum"))

    // the FROM clause with all the JOINs
    val from2 = v1
      .join(v1_lag, v1("i_category") === v1_lag("lag_category")
        && v1("i_brand") === v1_lag("lag_brand")
        && v1("cc_name") === v1_lag("lag_name")
        && v1("rn") === v1_lag("lag_rn") + 1)
      .join(v1_lead, v1("i_category") === v1_lead("lead_category")
        && v1("i_brand") === v1_lead("lead_brand")
        && v1("cc_name") === v1_lead("lead_name")
        && v1("rn") === v1_lead("lead_rn") - 1)

    // the SELECT clause
    val select2 = from2.select(
      v1.col("i_category"), v1.col("d_year"), v1.col("d_moy"),
      v1.col("avg_monthly_sales"), v1.col("sum_sales"),
      v1_lag.col("psum"), v1_lead.col("nsum"))

    return select2
  }

  def makeFinal(session: Session, v2: DataFrame): DataFrame = {

    // the WHERE clause
    val filter3 = v2.filter(
      col("d_year") === 1999
      && col("avg_monthly_sales") > 0
      && abs(col("sum_sales") - col("avg_monthly_sales")) / col("avg_monthly_sales") > 0.1)

    // the ORDER BY clause
    val order3 = filter3.sort(col("sum_sales") - col("avg_monthly_sales"), col("d_moy"))

    // the LIMIT clause
    val limit3 = order3.limit(10)

    return limit3
  }
}