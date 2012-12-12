package co.torri.dod.analysis

import spark._
import com.thoughtworks.dod._
import scala.annotation.switch


class CommittersPerWeekAnalyzer extends Analyzer {
    def apply(data: RepoData, sc: SparkContext) = {

        val d = data.commits
            .flatMap { case c => c.files.flatMap(fc => fc.workspace).map(ws => (c.yearWeek, c.author)) }
            .groupBy { case (date, author) => (date) }
            .map { case (date, i) => (date, i.distinct.size) }
            .toArray
            .sortBy { case (date, commiters) => (date) }
            .map { case ((year, week), commiters) => List(year, week, commiters) }


        Result(Seq("year", "week", "commiters"), d)
    }
}
