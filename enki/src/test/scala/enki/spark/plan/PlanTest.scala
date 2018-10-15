package enki.spark.plan

import enki.EnkiTestSuite
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf

class PlanTest extends EnkiTestSuite {
  "test" in {
    val parser = new SparkSqlParser(new SQLConf())
    val plan = parser.parsePlan(
      """
        |create table t_team_mlc.pim_crm_id stored as PARQUET as
        |    select
        |        c.row_id,
        |        c.pr_indust_id
        |    from
        |        (
        |            select
        |                a.*,
        |                row_number() over (partition by row_id order by modification_num desc, last_upd desc) as rn
        |            from core_internal_crm_kb.s_org_ext a
        |            where
        |                ou_type_cd in ('ИП', 'Юр. лицо')
        |
        |        ) c
        |        left join
        |        (
        |            select
        |                a.row_id,
        |                a.bu_id,
        |                row_number() over (partition by row_id order by modification_num desc, last_upd desc) as rn
        |            from core_internal_crm_kb.s_postn a
        |        ) tb on c.pr_postn_id = tb.row_id
        |    where
        |        --tb.bu_id = "1-6L21" -- POV
        |        tb.bu_id = "${var:bu_id}"
        |        and c.rn = 1 and tb.rn=1
        |        and c.cust_stat_cd in ('Активна', 'Закреплена')
        |
      """.stripMargin)

    plan.collect { case (a : UnresolvedRelation) => a.tableIdentifier }.foreach(println(_))

  //  println(plan)
  }
}
